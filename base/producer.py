# # Questo file è necessario per l'invio del messaggio ai servizi correlati
# # Pika - Python Message Queue Asynchronous Library AMQP

import os
import sys
import time
import pika
import collections

from core.amqp.base.abstract_messanger import AbstractMessanger

collections.Callable = collections.abc.Callable

class AMQPProducer(AbstractMessanger):

    def __init__(self, queue: str, routing_key: str):
        super().__init__(queue, routing_key)
        

    def publish(self, method, corr_id, body):
        try:
            if not self.connection:
                raise Exception('Connection not open')
            
            if self.connection and self.channel:
                origin = os.environ["BBSENDER_ORIGIN"]
                if origin is None:
                    raise Exception("BBSENDER_ORIGIN environment variable not set")
                
            content_type = origin + "|" + method
            
            props = pika.BasicProperties(
                content_type=content_type,
                correlation_id=corr_id,
            )
            
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=body,
                properties=props
            )
            
            print("Message sent", file=sys.stderr)
            
            time.sleep(1)
        except Exception as e:
            raise e
    










