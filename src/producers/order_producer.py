"""
Order Event Producer
Generates and publishes order events to Kafka
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, Any

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer


class OrderProducer:
    """Produces order events to Kafka"""
    
    PRODUCTS = [
        {"id": "PROD001", "name": "Laptop", "price": 999.99},
        {"id": "PROD002", "name": "Smartphone", "price": 699.99},
        {"id": "PROD003", "name": "Headphones", "price": 149.99},
        {"id": "PROD004", "name": "Tablet", "price": 449.99},
        {"id": "PROD005", "name": "Smartwatch", "price": 299.99},
        {"id": "PROD006", "name": "Camera", "price": 799.99},
        {"id": "PROD007", "name": "Speaker", "price": 199.99},
        {"id": "PROD008", "name": "Monitor", "price": 349.99},
    ]
    
    ORDER_STATUSES = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"]
    PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "APPLE_PAY", "CRYPTO"]
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.producer = Producer({
            'bootstrap.servers': config.get('bootstrap_servers', 'localhost:9092'),
            'client.id': 'order-producer',
            'acks': 'all',
            'retries': 3,
            'linger.ms': 5,
            'batch.size': 16384
        })
        self.topic = config.get('topic', 'orders')
        self.string_serializer = StringSerializer('utf-8')
        
    def generate_order(self) -> Dict[str, Any]:
        """Generate a random order event"""
        product = random.choice(self.PRODUCTS)
        quantity = random.randint(1, 5)
        discount = random.choice([0, 0, 0, 0.1, 0.15, 0.2])  # 60% no discount
        
        return {
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product_id": product["id"],
            "product_name": product["name"],
            "quantity": quantity,
            "unit_price": product["price"],
            "discount": discount,
            "gross_amount": round(quantity * product["price"], 2),
            "net_amount": round(quantity * product["price"] * (1 - discount), 2),
            "order_status": random.choice(self.ORDER_STATUSES),
            "payment_method": random.choice(self.PAYMENT_METHODS),
            "shipping_address": f"{random.randint(100, 9999)} Main St, City {random.randint(1, 50)}, State",
            "event_time": datetime.utcnow().isoformat() + "Z",
            "event_type": "ORDER_CREATED"
        }
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            print(f"❌ Delivery failed: {err}")
        else:
            print(f"✅ Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")
    
    def produce_order(self, order: Dict[str, Any]):
        """Send order to Kafka"""
        key = order["order_id"]
        value = json.dumps(order)
        
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=value,
            callback=self.delivery_callback
        )
        self.producer.poll(0)
    
    def run(self, events_per_second: int = 10, duration_seconds: int = None):
        """
        Run the producer
        
        Args:
            events_per_second: Target throughput
            duration_seconds: How long to run (None = forever)
        """
        print(f"🚀 Starting Order Producer")
        print(f"   Topic: {self.topic}")
        print(f"   Rate: {events_per_second} events/sec")
        
        interval = 1.0 / events_per_second
        start_time = time.time()
        event_count = 0
        
        try:
            while True:
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                    
                order = self.generate_order()
                self.produce_order(order)
                event_count += 1
                
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_rate = event_count / elapsed
                    print(f"📊 Produced {event_count} events ({actual_rate:.1f}/sec)")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n⏹️ Shutting down...")
        finally:
            # Flush remaining messages
            remaining = self.producer.flush(timeout=10)
            print(f"✅ Producer stopped. {event_count} events sent, {remaining} pending.")


def main():
    import os
    
    config = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic": os.getenv("KAFKA_ORDERS_TOPIC", "orders")
    }
    
    producer = OrderProducer(config)
    producer.run(events_per_second=10, duration_seconds=None)


if __name__ == "__main__":
    main()
