import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='7TUSIFBKFOX4UR3P',
    sasl_plain_password='Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate sample data
def generate_order(order_id):
    return {
        "order_id": order_id,
        "order_date": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat(),
        "created_at": datetime.now().isoformat(),
        "customer_id": f"customer_{random.randint(1, 100)}",
        "amount": random.randint(100, 1000)
    }

def generate_payment(order_id, payment_id):
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_date": (datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat(),
        "created_at": datetime.now().isoformat(),
        "amount": random.randint(50, 500)
    }

# Send sample data to Kafka
order_ids = [f"order_{i}" for i in range(1, 11)]
payment_ids = [f"payment_{i}" for i in range(1, 21)]

for order_id in order_ids:
    order = generate_order(order_id)
    producer.send('orders_topic_data', value=order)
    print(f"Sent order: {order}")

    # Send one or more payments for each order
    for payment_id in payment_ids:
        if random.choice([True, False]):  # Randomly decide if we send a payment for this order
            payment = generate_payment(order_id, payment_id)
            producer.send('payments_topic_data', value=payment)
            print(f"Sent payment: {payment}")
    
    time.sleep(1)  # Simulate delay between orders

producer.flush()
producer.close()