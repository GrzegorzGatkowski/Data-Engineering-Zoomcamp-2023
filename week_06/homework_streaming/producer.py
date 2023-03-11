from kafka import KafkaProducer
import csv

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def read_csv_file_and_publish_to_kafka(file_path, topic_name):
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        header = next(reader)
        for row in reader:
            ride_data = ','.join(row)
            publish_message(producer, topic_name, 'ride', ride_data)

read_csv_file_and_publish_to_kafka('fhv_tripdata_2019-01.csv.gz', 'rides_fhv')
read_csv_file_and_publish_to_kafka('green_tripdata_2019-01.csv.gz', 'rides_green')
