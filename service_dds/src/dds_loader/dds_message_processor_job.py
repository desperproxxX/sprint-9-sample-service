import time
from datetime import datetime
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from dds_loader.repository.dds_repository import DdsRepository
import json
import hashlib
import uuid

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        self._logger = logger   
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        self._logger.info(f"DDS Processor started at {datetime.utcnow()}")

        while True:
            try:
                batch = []
                for _ in range(self._batch_size):
                    msg = self._consumer.consume(timeout=0.1)
                    if msg:
                        batch.append(msg)
                    else:
                        break

                if not batch:
                    time.sleep(1)
                    continue

                self._logger.info(f"DDS: Processing batch of {len(batch)} messages")

                # Обрабатываем собранную пачку
                for msg in batch:
                    payload = msg.get('payload', {})
                    if not payload:
                        self._logger.error(f"Empty payload: {msg}")
                        continue

                    products = payload.get('products', [])    
                    categories = {p.get('category') for p in products if p.get('category')}
                    # Загрузка категорий
                    for category_name in categories:
                        self._dds_repository.h_category_insert(
                            h_category_pk=self._generate_uuid_from_key(category_name),
                            category_name=category_name,
                            load_dt=datetime.utcnow(),
                            load_src='stg-service'
                        )
                        
                    # Загрузка заказов
                    order_id = payload.get('id')

                    if order_id is None:
                        self._logger.error(f"Order without id: {payload}")
                        continue

                    self._dds_repository.h_orders_insert(
                        h_order_pk=self._generate_uuid_from_key(str(order_id)),
                        order_id=order_id,
                        order_dt=payload.get('date'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service'
                    )

                    self._dds_repository.s_order_cost_insert(
                        h_order_pk=self._generate_uuid_from_key(str(order_id)),
                        cost=payload.get('cost'),
                        payment=payload.get('payment'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service',
                        hk_order_cost_hashdiff=self._generate_uuid_from_key(f"{order_id}_{payload.get('cost')}_{payload.get('payment')}")
                    )

                    self._dds_repository.s_order_status_insert(
                        h_order_pk=self._generate_uuid_from_key(str(order_id)),
                        status=payload.get('status'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service',
                        hk_order_status_hashdiff=self._generate_uuid_from_key(f"{order_id}_{payload.get('status')}")
                    )

                    # Загрузка продуктов
                    for p in products:
                        if not p.get('category'):
                            self._logger.warning(f"Product with id {p.get('id')} has no category. Skipping.")
                            continue

                        self._dds_repository.h_product_insert(
                            h_product_pk=self._generate_uuid_from_key(str(p.get('id'))),
                            product_id=p.get('id'),
                            load_dt=datetime.utcnow(),
                            load_src='stg-service'
                        )

                        self._dds_repository.s_product_names_insert(
                            h_product_pk=self._generate_uuid_from_key(str(p.get('id'))),
                            name=p.get('name'),
                            load_dt=datetime.utcnow(),
                            load_src='stg-service',
                            hk_product_names_hashdiff=self._generate_uuid_from_key(f"{p.get('id')}_{p.get('name')}")
                        )

                    # Загрузка ресторанов
                    self._dds_repository.h_restaurant_insert(
                        h_restaurant_pk=self._generate_uuid_from_key(str(payload.get('restaurant').get('id'))),
                        restaurant_id=payload.get('restaurant').get('id'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service'
                    )

                    self._dds_repository.s_restaurant_names_insert(
                        h_restaurant_pk=self._generate_uuid_from_key(str(payload.get('restaurant').get('id'))),
                        name=payload.get('restaurant').get('name'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service',
                        hk_restaurant_names_hashdiff=self._generate_uuid_from_key(f"{payload.get('restaurant').get('id')}_{payload.get('restaurant').get('name')}")
                    )

                    # Загрузка пользователей
                    self._dds_repository.h_user_insert(
                        h_user_pk=self._generate_uuid_from_key(str(payload.get('user').get('id'))),
                        user_id=payload.get('user').get('id'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service'
                    )

                    self._dds_repository.s_user_names_insert(
                        h_user_pk=self._generate_uuid_from_key(str(payload.get('user').get('id'))),
                        username=payload.get('user').get('name'),
                        userlogin=payload.get('user').get('login'),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service',
                        hk_user_names_hashdiff=self._generate_uuid_from_key(f"{payload.get('user').get('id')}_{payload.get('user').get('name')}_{payload.get('user').get('login')}")
                    )

                    # Загрузка link-таблиц
                    for p in products:
                        self._dds_repository.l_order_product_insert(
                            hk_order_product_pk=self._generate_uuid_from_key(f"{order_id}_{str(p.get('id'))}"),
                            h_order_pk=self._generate_uuid_from_key(str(order_id)),
                            h_product_pk=self._generate_uuid_from_key(str(p.get('id'))),
                            load_dt=datetime.utcnow(),
                            load_src='stg-service'
                        )

                        self._dds_repository.l_product_category_insert(
                            hk_product_category_pk=self._generate_uuid_from_key(f"{p.get('id')}_{p.get('category')}"),
                            h_product_pk=self._generate_uuid_from_key(str(p.get('id'))),
                            h_category_pk=self._generate_uuid_from_key(p.get('category')),
                            load_dt=datetime.utcnow(),
                            load_src='stg-service'
                        )

                        self._dds_repository.l_product_restaurant_insert(
                            hk_product_restaurant_pk=self._generate_uuid_from_key(f"{payload.get('restaurant').get('id')}_{p.get('id')}"),
                            h_restaurant_pk=self._generate_uuid_from_key(str(payload.get('restaurant').get('id'))),
                            h_product_pk=self._generate_uuid_from_key(str(p.get('id'))),
                            load_dt=datetime.utcnow(),
                            load_src='stg-service'
                        )
                    
                    self._dds_repository.l_order_user_insert(
                        hk_order_user_pk=self._generate_uuid_from_key(f"{order_id}_{str(payload.get('user').get('id'))}"),
                        h_order_pk=self._generate_uuid_from_key(str(order_id)),
                        h_user_pk=self._generate_uuid_from_key(str(payload.get('user').get('id'))),
                        load_dt=datetime.utcnow(),
                        load_src='stg-service'
                    )

                self._logger.info(f"DDS: Batch of {len(batch)} successfully processed")
                
                # Если в библиотеке есть commit(), вызывай его здесь, 
                # чтобы подтвердить прочтение всей пачки сразу

            except Exception as e:
                self._logger.error(f"Error in DDS processing loop: {e}", exc_info=True)
                time.sleep(5)

     

    
    def _generate_uuid_from_key(self, business_key: str) -> str:
        hash_object = hashlib.sha256(business_key.encode('utf-8'))
        return str(uuid.UUID(hash_object.hexdigest()[:32]))



