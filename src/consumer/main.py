# consumer.py (폴더 구조화 최종 버전)

import os
import json
import logging
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv

import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
from confluent_kafka import Consumer, KafkaException, KafkaError

load_dotenv()

# (로깅 및 설정 부분은 이전과 동일)
# ...
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID')

MINIO_ENDPOINT_URL = os.environ.get('MINIO_ENDPOINT_URL')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = os.environ.get('MINIO_BUCKET_NAME')


def get_kafka_consumer():
    # ... (이전과 동일)
    try:
        conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest',
        }
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
        logging.info(f"Successfully subscribed to topic '{KAFKA_TOPIC}'")
        return consumer
    except Exception as e:
        logging.error(f"Failed to create Kafka consumer: {e}")
        return None

def get_s3_client():
    # ... (이전과 동일)
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        logging.info(f"S3 client created for endpoint {MINIO_ENDPOINT_URL}")
        return s3_client
    except Exception as e:
        logging.error(f"Failed to create S3 client: {e}")
        return None

def consume_and_save(consumer, s3_client):
    """ Kafka 메시지를 소비하고 구조화된 폴더에 Parquet 형식으로 S3에 저장합니다. """
    logging.info("Starting to consume messages...")
    msg_count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                continue
            else:
                logging.error(f"Kafka error: {msg.error()}")
                break
        
        msg_count += 1
        logging.info(f"Message #{msg_count} received, processing...")

        try:
            trade_data = json.loads(msg.value().decode('utf-8'))
            
            # --- 경로 생성을 위한 정보 추출 ---
            symbol = trade_data.get('s')
            event_time_ms = trade_data.get('E')
            event_dt = datetime.fromtimestamp(event_time_ms / 1000.0)
            
            # 1. 기본 경로 설정 (코인 심볼 / 데이터 소스 / 데이터 종류)
            base_path = f"symbol={symbol}/source=websocket/type=trade"
            
            # 2. 시간 기반 파티션 경로 추가
            partition_path = event_dt.strftime("year=%Y/month=%m/day=%d")
            
            # 3. 고유한 파일 이름 생성
            trade_id = trade_data.get('t')
            file_name = f"{event_time_ms}_{trade_id}.parquet"
            
            # 4. 전체 S3 객체 경로 조합
            s3_object_key = f"{base_path}/{partition_path}/{file_name}"
            
            # --- Parquet 변환 및 업로드 (이전과 동일) ---
            df = pd.DataFrame([trade_data])
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            
            parquet_buffer.seek(0)
            s3_client.put_object(
                Bucket=MINIO_BUCKET_NAME,
                Key=s3_object_key,
                Body=parquet_buffer.read(),
                ContentType='application/octet-stream'
            )

            logging.info(f"Successfully saved message #{msg_count} to S3: s3://{MINIO_BUCKET_NAME}/{s3_object_key}")

        except Exception as e:
            logging.error(f"An error occurred while processing message #{msg_count}: {e}")

if __name__ == "__main__":
    kafka_consumer = get_kafka_consumer()
    s3_client = get_s3_client()

    if kafka_consumer and s3_client:
        try:
            consume_and_save(kafka_consumer, s3_client)
        except KeyboardInterrupt:
            logging.info("Consumer is shutting down.")
        finally:
            kafka_consumer.close()
            logging.info("Kafka consumer closed.")