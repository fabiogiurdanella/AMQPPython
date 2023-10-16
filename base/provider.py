from ast import Dict
from enum import Enum
import os
import sys
from time import sleep
import uuid
import collections
import pika

from core.amqp.base.consumer import AMQPConsumer
from core.amqp.base.producer import AMQPProducer

collections.Callable = collections.abc.Callable

# Questo file gestisce la trasmissione dei dati e la gestione degli eventi di ritorno, derivanti dal corretto salvataggio degli stessi
# - Trasmette la lista dei customers leggi dal CSV al consumer del microservizio customers
# - Riceve la risposta dal microservizio customers
# - Se non ci sta nessun errore -- Ok
# - Altrimenti, rispedisce la lista degli ID da eliminare al microservizio customers

class AMQPProviderType(Enum):
    BBSENDER = "BBSENDER"
    BBPAYMENTS = "BBPAYMENTS"
    


class AMQPProvider():
    
    instances = {}
    
    def __init__(self, consumer_queue: str, producer_queue: str, has_producer: bool = True):
        # Questa coda descrive i dati richiesti al microservizio
        self.consumer_queue = consumer_queue
        self.consumer_queue_rk = consumer_queue + '_rk'
        
        # Questa cosa descrive i dati restituiti dal microservizio
        self.producer_queue = producer_queue
        self.producer_queue_rk = producer_queue + '_rk'

        self.response_dict = {}    
        
        self.consumer = AMQPConsumer(
            queue=self.consumer_queue,
            routing_key=self.consumer_queue_rk,
            callback=self.data_received_response
        )
        
        self.has_producer = has_producer
        if has_producer:
            self.producer = AMQPProducer(
                queue=self.producer_queue,
                routing_key=self.producer_queue_rk
            )
        
        
    @staticmethod
    def get_instance(amqp_provider_type: AMQPProviderType):
        # return cls.instances[amqp_provider_type]
        return AMQPProvider.instances.get(amqp_provider_type)

    @staticmethod
    def create_istance(consumer_queue: str, producer_queue: str, amqp_provider_type: AMQPProviderType):
        if AMQPProvider.get_instance(amqp_provider_type) is None:
            # AMQPProvider.instances[amqp_provider_type] = AMQPProvider(consumer_queue, producer_queue)
            istance = AMQPProvider(consumer_queue, producer_queue)
            AMQPProvider.instances[amqp_provider_type] = istance

        return AMQPProvider.get_instance(amqp_provider_type)
  
    def data_received_response(self, ch, method, props, body):
        self.response_dict[props.correlation_id] = body.decode("utf-8")
        
            
    def provide_listening(self):
        try:
            self.consumer.start_messanger()
            self.consumer.listen()
        except Exception as e:
            print(e, file=sys.stderr)
            self.consumer.close_connection()
    
    def provide_publishing(self):
        try:
            self.producer.start_messanger()
        except Exception as e:
            print(e, file=sys.stderr)
            self.producer.close_connection()
    
    def publish(self, origin: str, method: str, body: str, corr_id: str):
        if self.has_producer:
            self.producer.publish(origin, method, corr_id, body)
        else:
            producer = AMQPProducer(
                queue=self.producer_queue + "_" + origin,
                routing_key=self.producer_queue_rk
            )
            producer.start_messanger()
            producer.publish(origin, method, corr_id, body)
            producer.close_connection()
