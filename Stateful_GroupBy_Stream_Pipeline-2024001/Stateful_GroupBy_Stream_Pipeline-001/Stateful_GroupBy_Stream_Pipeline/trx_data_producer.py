# from confluent_kafka import Producer
# import json
# import time

# def delivery_report(err, msg):
#     if err is not None:
#         print(f"Message delivery failed: {err}")
#     else:
#         print(f"Message delivered to {msg.topic()}")

# with open('user_transactions.json') as f:
#     data = json.load(f)

# p = Producer({'bootstrap.servers': 'localhost:9092'})

# for record in data:
#     p.poll(0)
#     record_str = json.dumps(record)
#     p.produce('trx_data', record_str, callback=delivery_report)
#     print("Message Published -> ",record_str)
#     time.sleep(2) # wait for 20 seconds before sending the next record

# p.flush()

# from confluent_kafka import Producer
# import time
# import json

# kafka_config = {
#     'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
#     'sasl.mechanisms': 'PLAIN',
#     'security.protocol': 'SASL_SSL',
#     'sasl.username': '7TUSIFBKFOX4UR3P',
#     'sasl.password': 'Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/'
# }

# p = Producer(**kafka_config)

# def delivery_report(err, msg):
#     """ Called once for each message produced to indicate delivery result. """
#     if err is not None:
#         print(f'Message delivery failed: {err}')
#     else:
#         print(f'Message delivered to {msg.topic()}')

# with open('user_transactions.json') as f:
#     data = json.load(f)

# # with open('user_transactions.json', 'r') as f:
# for line in data:
#     record = json.loads(line)
#     p.produce('trx_topic_data', key=str(record['user_id']), value=json.dumps(record), callback=delivery_report)
#     print("Message Published -> ",record)
#     time.sleep(3)  # Send one message per 20 seconds
#     p.flush()

from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")

kafka_config = {
    'bootstrap.servers': 'pkc-4j8dq.southeastasia.azure.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '7TUSIFBKFOX4UR3P',
    'sasl.password': 'Ks4s1dZAgPEtSwRhTwxv5MprrvTd5oaLaZ6en79iYAgBTeoeZ8tJw+KEekj5+H7/'
}

with open('user_transactions.json') as f:
    data = json.load(f)

p = Producer(**kafka_config)

for record in data:
    p.poll(0)
    record_str = json.dumps(record)
    p.produce('trx_topic_data', record_str, callback=delivery_report)
    print("Message Published -> ",record_str)
    time.sleep(3) # wait for 20 seconds before sending the next record

p.flush()