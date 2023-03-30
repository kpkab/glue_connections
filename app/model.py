from pydantic import BaseModel
from typing import Union, Optional


class Glue_connection(BaseModel):
    Name : str
    ConnectionType : str
    JDBC_CONNECTION_URL : str
    PASSWORD : str
    USERNAME: str
class SuccessResponse(BaseModel):
    success: bool = True
    status:int = 200
    data: Union[None, dict, list] = None

class ErrorResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Something wrong"}

class ExceptionResponse(BaseModel):
    success: bool = False
    status:int = 404
    message: Union[None, dict, list] = {"message":"Unhandled Exception"}