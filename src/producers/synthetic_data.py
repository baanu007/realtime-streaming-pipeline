"""
Synthetic event generator.

Uses Faker to produce realistic clickstream / order / signup events for the
streaming pipeline. Kept dependency-free of AWS so it can be unit-tested
in isolation and reused from the event_simulator or from tests.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from faker import Faker

EVENT_TYPES: List[str] = ["page_view", "order", "signup"]

# Bounded catalog so partition keys and dimensions stay realistic.
_PRODUCTS: List[Dict[str, Any]] = [
    {"product_id": "P-1001", "name": "Laptop", "price": 1299.00, "category": "electronics"},
    {"product_id": "P-1002", "name": "Headphones", "price": 199.00, "category": "electronics"},
    {"product_id": "P-1003", "name": "Coffee Maker", "price": 89.50, "category": "home"},
    {"product_id": "P-1004", "name": "Running Shoes", "price": 129.99, "category": "apparel"},
    {"product_id": "P-1005", "name": "Yoga Mat", "price": 39.99, "category": "fitness"},
    {"product_id": "P-1006", "name": "Smartwatch", "price": 349.00, "category": "electronics"},
]

_PAGES: List[str] = [
    "/",
    "/search",
    "/product",
    "/cart",
    "/checkout",
    "/account",
    "/help",
]


@dataclass
class GeneratorConfig:
    """Tuning knobs for synthetic data generation."""

    # Distribution of event types (must sum to ~1.0).
    page_view_weight: float = 0.75
    order_weight: float = 0.20
    signup_weight: float = 0.05
    # Pool of user_ids - bounded so the same user shows up multiple times,
    # which is what you want for partition-key based ordering in Kinesis.
    user_pool_size: int = 500
    # Probability of marking an order high-value (drives alert routing).
    high_value_threshold: float = 1000.0
    # Seed for deterministic tests.
    seed: Optional[int] = None


class SyntheticEventGenerator:
    """Generates clickstream / order / signup events as dicts."""

    def __init__(self, config: Optional[GeneratorConfig] = None):
        self.config = config or GeneratorConfig()
        self.faker = Faker()
        if self.config.seed is not None:
            Faker.seed(self.config.seed)
            random.seed(self.config.seed)
        self._user_pool = [f"U-{self.faker.uuid4()[:8]}" for _ in range(self.config.user_pool_size)]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def generate(self) -> Dict[str, Any]:
        """Generate a single random event."""
        event_type = self._pick_event_type()
        if event_type == "page_view":
            return self._page_view()
        if event_type == "order":
            return self._order()
        return self._signup()

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        """Generate ``n`` events."""
        if n < 0:
            raise ValueError("n must be >= 0")
        return [self.generate() for _ in range(n)]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _pick_event_type(self) -> str:
        weights = [
            self.config.page_view_weight,
            self.config.order_weight,
            self.config.signup_weight,
        ]
        return random.choices(EVENT_TYPES, weights=weights, k=1)[0]

    def _base(self, event_type: str) -> Dict[str, Any]:
        user_id = random.choice(self._user_pool)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "session_id": f"S-{uuid.uuid4().hex[:12]}",
            "source": "web",
        }

    def _page_view(self) -> Dict[str, Any]:
        event = self._base("page_view")
        event.update(
            {
                "page": random.choice(_PAGES),
                "referrer": self.faker.uri(),
                "user_agent": self.faker.user_agent(),
                "ip": self.faker.ipv4_public(),
            }
        )
        return event

    def _order(self) -> Dict[str, Any]:
        event = self._base("order")
        product = random.choice(_PRODUCTS)
        quantity = random.randint(1, 4)
        amount = round(product["price"] * quantity, 2)
        event.update(
            {
                "order_id": f"O-{uuid.uuid4().hex[:10].upper()}",
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["price"],
                "amount": amount,
                "currency": "USD",
                "is_high_value": amount >= self.config.high_value_threshold,
                "payment_method": random.choice(
                    ["credit_card", "debit_card", "paypal", "apple_pay"]
                ),
            }
        )
        return event

    def _signup(self) -> Dict[str, Any]:
        event = self._base("signup")
        event.update(
            {
                "email": self.faker.email(),
                "country": self.faker.country_code(),
                "referral_source": random.choice(
                    ["organic", "paid_search", "social", "email", "direct"]
                ),
            }
        )
        return event
