import sys
import json
import os
import time
from uuid import uuid4
from core.amqp.base.provider import AMQPProvider
from core.amqp.model.payload import AMQPMethod, AMQPPayload, AMQPBody, AMQPStatus, AMQPResponse
from services.fernet_service import FernetService


class AMQPService:
    def __init__(self, amqp_provider: AMQPProvider):
        self.amqp_provider = amqp_provider

    def connect_and_get_data(self, amqp_method: AMQPMethod, **kwargs):
        """
        Connette e ottiene i dati da un microservizio tramite RabbitMQ.

        :param amqp_method: Metodo AMQP da invocare.
        :param kwargs: Parametri da passare al metodo AMQP.
        :return: Risposta ottenuta dal microservizio.
        """
        try:
            payload = AMQPPayload(amqp_method, AMQPBody(**kwargs))
            response = self.__send_data(payload, self.amqp_provider)

            if response.status == AMQPStatus.ERROR:
                raise Exception(response.to_json())

            return response

        except Exception as e:
            return AMQPResponse(amqp_method, AMQPStatus.ERROR, str(e))

    def __send_data(self, payload: AMQPPayload, amqp: AMQPProvider):
        """
        Invia dati crittografati a un microservizio tramite RabbitMQ e attende la risposta.

        :param payload: Payload AMQP da inviare al microservizio.
        :param amqp: Provider AMQP utilizzato per comunicare con il microservizio.
        :return: Risposta ottenuta dal microservizio.
        """
        method = payload.method.value
        data = payload.body.to_json() if payload.body else None

        encrypt_key = os.environ.get("BBSENDER_ENCRYPT_KEY")
                
        encrypted_data = FernetService.encrypt_data(data, encrypt_key)
        uuid = str(uuid4())

        print(f"Sending data to {method} with uuid {uuid}", file=sys.stderr)
        amqp.publish(method, encrypted_data, corr_id=uuid)

        response = self.__wait_for_response(amqp, uuid, method)
        
        response_dict = json.loads(response)
        
        print(f"Response for {method} with uuid {uuid} is {response_dict}", file=sys.stderr)

        return AMQPResponse(AMQPMethod(response_dict["method"]), AMQPStatus(response_dict["status"]), response_dict["data"])

    def __wait_for_response(self, amqp, uuid, method):
        """
        Attende la risposta da un microservizio tramite RabbitMQ.

        :param amqp: Provider AMQP utilizzato per comunicare con il microservizio.
        :param uuid: UUID utilizzato per identificare univocamente la richiesta.
        :param method: Metodo AMQP invocato.
        :return: Risposta ottenuta dal microservizio.
        """
        print(f"Waiting for response for {method} with uuid {uuid}", file=sys.stderr)
        start_time = time.time()
        
        timeout_time = start_time + 60
        
        while True:
            if uuid in amqp.response_dict:
                response = amqp.response_dict.pop(uuid)
                print(f"Response found for {method} with uuid {uuid}", file=sys.stderr)
                break
            elif time.time() > timeout_time:
                print(f"Timeout for {method} with uuid {uuid}", file=sys.stderr)
                raise Exception("Timeout")
            time.sleep(0.1)

        if isinstance(response, Exception):
            raise response

        return response

