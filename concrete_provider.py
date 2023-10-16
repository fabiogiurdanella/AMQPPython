
import sys
import collections
from threading import Thread
from core.abstract.setting import Setting

from core.amqp.base.provider import AMQPProvider
from core.amqp.model.payload import AMQPBody, AMQPMethod, AMQPResponse, AMQPStatus

import json
from core.exception.external_exception import ExternalException
from utility.fernet_utility import FernetUtility
from workers.delete_file_worker import DeleteFileWorker
from workers.read_file_worker import ReadFileWorker
from workers.save_file_worker import SaveFileWorker
from workers.send_mail_worker import SendMailWorker
from workers.send_notification_worker import SendNotificationWorker

collections.Callable = collections.abc.Callable

class ConcreteAMQPProvider(AMQPProvider):
    def __init__(self, consumer_queue: str, producer_queue: str):
        super().__init__(consumer_queue, producer_queue, has_producer=False)
    
    def data_received_response(self, ch, method, props, body):
        thread = Thread(target=self._manage_data_response, args=(ch, method, props, body))
        thread.start()
    
    def _manage_data_response(self, ch, method, props, body):
        try:
            print("CORR_ID della richiesta: ", props.correlation_id, file=sys.stderr)
            print("Risposta ricevuta", file=sys.stderr)
            
            content_type = props.content_type
            print("Content type: ", content_type, file=sys.stderr)
            body_str = body.decode('utf-8')
            
            try:
                decrypted_body = FernetUtility.decrypt_data(body)
            except Exception as e:
                raise ExternalException("Errore: impossibile decifrare il body")
            
            body_json = json.loads(decrypted_body)
            
            amqp_body = AMQPBody(**body_json)
            # print("BODY: ", amqp_body.__dict__, file=sys.stderr)
            
            try:
                origin, method = self._get_data_from_content_type(content_type)
            except ExternalException as e:
                raise e
            
            amqp_method = AMQPMethod(method)
            
            self.manage_data(origin, amqp_method, amqp_body, props.correlation_id)
            
        except ExternalException as e:
            data = AMQPResponse(AMQPMethod.EXCEPTION, AMQPStatus.ERROR, e.message)
            self.publish(origin, AMQPMethod.EXCEPTION.value, str(data), props.correlation_id)
        except Exception as e:
            data = AMQPResponse(AMQPMethod.EXCEPTION, AMQPStatus.ERROR, "Errore: " + str(e))
            self.publish(origin, AMQPMethod.EXCEPTION.value, str(data), props.correlation_id)
    
    def manage_data(self, origin: str, amqp_method: AMQPMethod, amqp_body: AMQPBody, corr_id: str):
        """
        La funzione gestisce i dati in base al metodo e al provider
        In base al methodo e al provider, la funzione richiama la funzione get_data nel modo corretto
        Il risultato è trasmesso al richiedente
        """
        data = self.get_data(amqp_method, amqp_body)
        
        # print("Risposta: ", data, file=sys.stderr)
        self.publish(origin, amqp_method.value, str(data), corr_id)
    
    
    def get_data(self, method: AMQPMethod, amqp_body: AMQPBody):
        """
        La funzione ritorna i dati in base al metodo e al provider
        """
        try:
            """
                Il body contiene al suo interno un json con le impostazioni
            """
            
            settings = self.__get_settings(amqp_body)
            response = None
            
            if method == AMQPMethod.SEND_EMAIL:
                send_mail_worker = SendMailWorker(settings=settings)
                send_mail_worker.handle_work()
                response = AMQPResponse(method, AMQPStatus.OK, "OK")
            
            if method == AMQPMethod.SEND_NOTIFICATION:
                send_notification_worker = SendNotificationWorker(settings=settings)
                response = send_notification_worker.handle_work()
                
            if method == AMQPMethod.SAVE_FILE:
                save_file_worker = SaveFileWorker(settings=settings)
                response = save_file_worker.handle_work()
            if method == AMQPMethod.READ_FILE:
                read_file_worker = ReadFileWorker(settings=settings)
                response = read_file_worker.handle_work()
            if method == AMQPMethod.DELETE_FILE:
                delete_file_worker = DeleteFileWorker(settings=settings)
                response = delete_file_worker.handle_work()
            
            return response
            
        except Exception as e:
            body = "Errore: " + str(e)
            response = AMQPResponse(method, AMQPStatus.ERROR, body)
            return response
        except ExternalException as e:
            body = "Errore: " + str(e)
            response = AMQPResponse(method, AMQPStatus.ERROR, body)
            return response
        
    def _get_data_from_content_type(self, content_type: str):
        try:
            
            try:
                origin = content_type.split("|")[0]
                content_type_method = content_type.split("|")[1]
            except Exception as e:
                raise ExternalException("Errore: content type non valido")
            
            methods = [key.lower() for key, value in AMQPMethod.__members__.items()]
            
            method_found = None
            for method in methods:
                try:
                    if content_type_method == method:
                        method_found = method
                        break
                except ValueError:
                    continue
            
            if method_found is None:
                raise ExternalException("Errore: metodo non valido")
            
            return origin, method_found
            

            
        except Exception as e:
            raise ExternalException("Errore: impossibile ottenere il metodo dalla content type")
    
    def __get_settings(self, body: AMQPBody) -> dict[Setting]:
        """
            Il body della richiesta contiene al suo interno un dizionario strutturato nel seguente modo:
            
            Chiave: Nome della classe in formato lower case ed underscore
            Valore: Dizionario contenente i parametri della classe
            
            Scorro il dizionario che ricevo in input e restituisco un dizionario con i dati della classe mappati
            
            Può esistere un solo oggetto per ogni classe
            Se ne esistono più di uno, viene restituito un errore
        """
        try:
            from core.models.smtp_settings import SMTPSettings
            from core.models.mail_settings import MailSettings
            from core.models.file_settings import FileSettings
            from core.models.notification_settings import NotificationSettings
            
            return_dict = {}
            
            allowed_settings = {
                "smtp_settings": "SMTPSettings",
                "mail_settings": "MailSettings",
                "file_settings": "FileSettings",
                "notification_settings": "NotificationSettings"
            }
            
            for key, value in body.__dict__.items():
                if key in allowed_settings.keys():
                    class_name = allowed_settings[key]
                    try:
                        setting_class_name = globals()[class_name]
                    except KeyError as e:
                        print(e, file=sys.stderr)
                        setting_class_name = locals()[class_name]
                    
                    if setting_class_name is None:
                        raise ExternalException(f"La classe {class_name} non esiste")
                    
                    if setting_class_name is not None:
                        try:
                            setting_object = setting_class_name(value)
                        except TypeError as e:
                            raise ExternalException(f"Errore: {e}")
                        
                        if class_name in return_dict.keys():
                            raise ExternalException(f"Esiste già un oggetto {class_name}")        
                        return_dict[key] = setting_object
                else:
                    raise ExternalException(f"{key} non è un setting valido")
            
            return return_dict
        except ExternalException as e:
            print(e, file=sys.stderr)
            raise e
