import time
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_repository import StgRepository
import json


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        self._logger = logger   
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        # Имитация работы. Здесь будет реализована обработка сообщений.
        # time.sleep(2)
        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                break
            # upsert in PG table
            order = msg['payload']
            self._stg_repository.order_events_insert(
                object_id=msg['object_id'],
                object_type=msg['object_type'],
                sent_dttm=datetime.strptime(msg['sent_dttm'], "%Y-%m-%d %H:%M:%S"),
                payload=json.dumps(order)
            )

            # user info
            user_id = order['user']['id']
            user = self._redis.get(user_id)
            user_name = user['name']
            user_login = user['login']

            # resaurant info
            restaurant_id = order['restaurant']['id']
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant['name']

            # result message
            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": order["date"],
                    "cost": order["cost"],
                    "payment": order["payment"],
                    "status": order["final_status"],
                    "restaurant": self._format_restaurant(restaurant_id, restaurant_name),
                    "user": self._format_user(user_id, user_name, user_login),
                    "products": self._format_items(order["order_items"], restaurant)
                }
            }

            self._producer.produce(dst_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _format_restaurant(self, id, name):
        return {
            "id": id,
            "name": name
        }
    
    def _format_user(self, id, name, login):
        return {
            "id": id,
            "name": name,
            "login": login
        }
    
    def _format_items(self, order_items, restaurant):
        items = []

        menu = restaurant["menu"]
        for it in order_items:
            menu_item = next(x for x in menu if x["_id"] == it["id"])
            dst_it = {
                "id": it["id"],
                "price": it["price"],
                "quantity": it["quantity"],
                "name": menu_item["name"],
                "category": menu_item["category"]
            }
            items.append(dst_it)

        return items



