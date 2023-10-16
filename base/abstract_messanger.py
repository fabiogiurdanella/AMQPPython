from abc import ABC
import sys
import time
import pika
import os

NUM_RETRIES = 5

class AbstractMessanger(ABC):
    def __init__(self, queue: str, routing_key: str, callback: callable = None):
        self.connection = None
        self.channel = None
        
        self.username   = os.environ.get('RABBITMQ_USER')
        self.password   = os.environ.get('RABBITMQ_PASS')
        self.exchange   = os.environ.get('RABBITMQ_EXCHANGE')
        self.hostname   = os.environ.get('RABBITMQ_HOSTNAME')
        self.port       = os.environ.get('RABBITMQ_PORT')
        
        self.queue = queue
        self.routing_key = routing_key
        self.callback = callback
        
        try:
            self.__validate_env_variables()
        except Exception as e:
            raise e
        
    def __validate_env_variables(self):
        required_variables = [
            'RABBITMQ_USER',
            'RABBITMQ_PASS',
            'RABBITMQ_HOSTNAME',
            'RABBITMQ_PORT',
            'RABBITMQ_EXCHANGE'
        ]
        
        for var in required_variables:
            if os.environ.get(var) is None:
                raise Exception(f'Environment variable {var} is not defined')
            
    def __create_connection(self):
        param = pika.ConnectionParameters(
            host=self.hostname,
            port=self.port,
            credentials=pika.PlainCredentials(self.username, self.password),
            heartbeat=600,
            blocked_connection_timeout=300
        )
        self.connection = pika.BlockingConnection(param)
        
    def __create_channel(self):
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

    # Setup queue
    def __create_queue(self):
        self.channel.queue_declare(queue=self.queue, durable=False)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)
        if self.callback is not None:
            self.channel.basic_consume(self.queue, self.callback, auto_ack=True)
            
    def __start_connetion(self):
        try:
            self.__create_connection()
            self.__create_channel()
            self.__create_queue()
            print('Connection and channel are open for queue: ', self.queue, file=sys.stderr)
        except Exception as e:
            print(e, file=sys.stderr)
            raise e
        
    def start_messanger(self):
        retries = 0
        try:
            self.__start_connetion()
        except Exception as e:
            retries += 1
            if retries > NUM_RETRIES:
                raise Exception('Failed to connect to RabbitMQ, max retries reached')
            
            print('Retrying to connect to', self.hostname)
            time.sleep(5)
            return self.start_messanger()
    
    def close_connection(self):
        try:
            if self.channel is not None:
                self.channel.close()
            
            if self.connection is not None:
                self.connection.close()
                
            self.channel = None
            self.connection = None
        except Exception as e:
            raise e