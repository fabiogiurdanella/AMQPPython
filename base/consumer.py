# # Questo file è necessario per l'invio del messaggio ai servizi correlati
# # Pika - Python Message Queue Asynchronous Library AMQP

import os
from pika.exceptions import AMQPConnectionError
import pika
import time
import collections

from core.amqp.base.abstract_messanger import AbstractMessanger

collections.Callable = collections.abc.Callable

class AMQPConsumer(AbstractMessanger):
    
    def __init__(self, queue: str, routing_key: str, callback: callable):
        super().__init__(queue, routing_key, callback)
        
    def listen(self):
        while True:
            self.connection.process_data_events()
            time.sleep(0.1)