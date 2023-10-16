import os
import sys
from threading import Thread

from core.amqp.base.provider import AMQPProvider, AMQPProviderType


class AMQPServer:
    def __init__(self):
        self.__amqp_provider = self.__create_amqp_provider()
    
    def __create_amqp_provider(self):
        
        if "BBSENDER_ORIGIN" not in os.environ:
            raise Exception("BBSENDER_ORIGIN non impostato")
        
        origin = os.environ.get("BBSENDER_ORIGIN")
        
        # provider = AMQPProvider(
        #     response_dict={},
        #     corr_id=None,
        #     consumer_queue="bbsender_response" + "_" + origin,
        #     producer_queue="bbsender_request",
        #     is_consumer_responding=False,
        # )
        provider = AMQPProvider.create_istance(
            consumer_queue="bbsender_response" + "_" + origin,
            producer_queue="bbsender_request",
            amqp_provider_type=AMQPProviderType.BBSENDER
        )
        print("AMQPProvider created", file=sys.stderr)
        return provider

    def get_amqp_provider(self):
        return self.__amqp_provider

    def start_amqp_server(self):
        thread_listener = Thread(target=self.__amqp_provider.provide_listening)
        thread_publisher = Thread(target=self.__amqp_provider.provide_publishing)
        
        thread_listener.start()
        thread_publisher.start()
        
        # thread.start()