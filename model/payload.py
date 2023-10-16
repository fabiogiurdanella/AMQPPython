from enum import Enum
import json

from core.abstract.json_serializable import JsonSerializable

class AMQPMethod(Enum):
    EXCEPTION                   = "exception"
    
    SEND_EMAIL                  = "send_email"
    
    SAVE_FILE                   = "save_file"
    READ_FILE                   = "read_file"
    DELETE_FILE                 = "delete_file"
    
    SEND_NOTIFICATION           = "send_notification"
    
class AMQPBody():
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if value is not None:
                setattr(self, key, value)
        
    def to_json(self):
        return json.dumps(self.__dict__)
    
class AMQPStatus(Enum):
    OK = "ok"
    ERROR = "error"
    
class AMQPPayload:
    def __init__(self, method: AMQPMethod, body: AMQPBody):
        self.method = method
        self.body = body
        
class AMQPResponse:
    def __init__(self, method: AMQPMethod, status: AMQPStatus, data):
        self.method = method
        self.status = status
        self.data = data
        
    def to_json(self):
        # return {
        #     "method": self.method.value,
        #     "status": self.status.value,
        #     "data": self.data
        # }
        return_dict = {
            "method": self.method.value,
            "status": self.status.value
        }
        
        if isinstance(self.data, JsonSerializable):
            return_dict["data"] = self.data.to_json()
        else:
            return_dict["data"] = self.data
            
        return return_dict
        
        
    
    def __str__(self):
        return json.dumps(self.to_json())
    