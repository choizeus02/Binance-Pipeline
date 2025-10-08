# producer.py (수정 완료)

import asyncio
import json
import os
import logging
import socket
import websockets # <-- 이 줄이 누락되었습니다

from confluent_kafka import Producer

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 환경 변수 또는 기본값을 사용하여 설정 로드
KAFKA_BROKER = os.environ.get('KAFKA_BROKER')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
COIN_SYMBOL = os.environ['COIN_SYMBOL'].lower()
BINANCE_WEBSOCKET_URI = f"wss://stream.binance.com:9443/ws/{COIN_SYMBOL}@trade"


def get_kafka_producer():
    """ Confluent Kafka Producer 인스턴스를 생성하고 반환합니다. """
    try:
        # Producer 설정이 딕셔너리 형태로 변경됨
        conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'client.id': socket.gethostname(),
            'linger.ms': 100,
            'batch.size': 16384 * 4,
        }
        producer = Producer(conf)
        logging.info(f"Successfully connected to Kafka Broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        return None

# 메시지 전송 결과를 처리하는 콜백 함수 (비동기 방식)
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    # else:
    #     logging.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')

async def binance_ws_client(producer):
    """ Binance WebSocket에 연결하고 데이터를 Kafka로 전송합니다. """
    while True:
        try:
            async with websockets.connect(BINANCE_WEBSOCKET_URI) as websocket: # 정상 동작
                logging.info(f"Connected to Binance WebSocket at {BINANCE_WEBSOCKET_URI}")
                async for message in websocket:
                    producer.poll(0)
                    
                    trade_data = json.loads(message)
                    message_key = trade_data.get('s')
                    
                    producer.produce(
                        KAFKA_TOPIC,
                        key=str(message_key).encode('utf-8'),
                        value=json.dumps(trade_data).encode('utf-8'),
                        callback=delivery_report
                    )

        except websockets.exceptions.ConnectionClosed as e: # 정상 동작
            logging.warning(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            producer.flush()
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            producer.flush()
            await asyncio.sleep(5)


if __name__ == "__main__":
    kafka_producer = get_kafka_producer()
    if kafka_producer:
        try:
            asyncio.run(binance_ws_client(kafka_producer))
        except KeyboardInterrupt:
            logging.info("Producer is shutting down.")
        finally:
            kafka_producer.flush()
            logging.info("Kafka producer flushed and closed.")