import uuid
from uuid import uuid4
from datetime import datetime
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import json

class SingletonMeta(type):
    """ Metaclase para implementar Singleton. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]

class InterfazAWS:
    def __init__(self, session_id, cpu_id):
        self.session_id = session_id
        self.cpu_id = cpu_id
        self.log_instance = CorporateLog.getInstance()
        self.data_instance = CorporateData.getInstance()

    def registrar_log(self):
        result = self.log_instance.post(self.session_id)
        return json.dumps({"resultado_registro": result})

    def consultar_datos_sede(self, sede_id):
        data = self.data_instance.getData(self.session_id, sede_id)
        return json.dumps({"datos_sede": data})

    def consultar_cuit(self, sede_id):
        cuit = self.data_instance.getCUIT(self.session_id, sede_id)
        return json.dumps({"cuit": cuit})

    def generar_id_secuencia(self, sede_id):
        new_seq_id = self.data_instance.getSeqID(self.session_id, sede_id)
        return json.dumps({"nuevo_id_secuencia": int(new_seq_id["idSeq"])})
    
    def listar_logs(self, filtro="cpu"):
            """
            Lista los registros en CorporateLog según el filtro.

            Parámetros:
            - filtro (str): "cpu" para filtrar por CPU ID o "session" para filtrar por Session ID.

            Retorna:
            - JSON con la lista de registros detallada.
            """
            logs = self.log_instance.list()  # Obtener todos los registros

            if filtro == "cpu":
                # Retornar todos los registros para el CPU actual
                return json.dumps({"logs_por_cpu": logs}, indent=4)
            elif filtro == "session":
                # Filtrar los registros solo para la sesión actual
                logs_filtrados = [log for log in logs if log["sessionid"] == self.session_id]
                return json.dumps({"logs_por_sesion": logs_filtrados}, indent=4)
            else:
                return json.dumps({"error": "Filtro no válido. Use 'cpu' o 'session'."})


class CorporateData(metaclass=SingletonMeta):
    """Clase que maneja los datos corporativos con implementación Singleton."""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('CorporateData')
    
    @staticmethod
    def getInstance():
        """Método estático para obtener la instancia única de CorporateData."""
        return CorporateData()

    def getData(self, uuid, id):
        """
        Retorna los datos corporativos en formato JSON para un ID de sede específico.
        
        Parámetros:
        - uuid: Identificador único de sesión.
        - id: Identificador de sede.

        Retorna:
        - JSON con los datos de sede o un mensaje de error si no se encuentra.
        """
        try:
            response = self.table.get_item(Key={'id': id})  # Cambiado a 'id' en minúsculas
            if 'Item' in response:
                data = response['Item']
                return {
                    "sede": data.get("sede"),
                    "domicilio": data.get("domicilio"),
                    "localidad": data.get("localidad"),
                    "provincia": data.get("provincia")
                }
            else:
                return {"error": "Registro no encontrado"}
        except (BotoCoreError, ClientError) as error:
            return {"error": f"Error al acceder a la base de datos: {error}"}

    def getCUIT(self, uuid, id):
        """
        Retorna el CUIT en formato JSON para un ID de sede específico.

        Parámetros:
        - uuid: Identificador único de sesión.
        - id: Identificador de sede.

        Retorna:
        - JSON con el CUIT o un mensaje de error si no se encuentra.
        """
        try:
            response = self.table.get_item(Key={'id': id})  # Cambiado a 'id' en minúsculas
            if 'Item' in response:
                return {"CUIT": response['Item'].get("CUIT")}
            else:
                return {"error": "Registro no encontrado"}
        except (BotoCoreError, ClientError) as error:
            return {"error": f"Error al acceder a la base de datos: {error}"}

    def getSeqID(self, uuid, id):
        """
        Retorna un identificador único de secuencia y lo incrementa en la base de datos.

        Parámetros:
        - uuid: Identificador único de sesión.
        - id: Identificador de sede.

        Retorna:
        - JSON con el idreq incrementado o un mensaje de error si no se encuentra.
        """
        try:
            response = self.table.get_item(Key={'id': id})  # Cambiado a 'id' en minúsculas
            if 'Item' in response:
                idSeq = response['Item'].get("idreq", 0) + 1
                # Actualizar el valor en la base de datos
                self.table.update_item(
                    Key={'id': id},
                    UpdateExpression="set idreq = :val",
                    ExpressionAttributeValues={':val': idSeq}
                )
                return {"idSeq": idSeq}
            else:
                return {"error": "Registro no encontrado"}
        except (BotoCoreError, ClientError) as error:
            return {"error": f"Error al acceder a la base de datos: {error}"}


class CorporateLog(metaclass=SingletonMeta):
    """Clase que maneja los registros (logs) de acciones con implementación Singleton."""
    
    def __init__(self):
        self.CPUid = str(uuid.getnode())  # Obtener el ID de la CPU
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('CorporateLog')

    @staticmethod
    def getInstance():
        """Método estático para obtener la instancia única de CorporateLog."""
        return CorporateLog()
    
    def post(self, sessionid):
        """
        Registra un log en la tabla CorporateLog.

        Parámetros:
        - sessionid: Identificador único de sesión.

        Retorna:
        - Mensaje de éxito o error.
        """
        try:
            uniqueID = str(uuid.uuid4())
            ts = datetime.now().isoformat()
            # Inserta en la tabla CorporateLog con los campos requeridos
            self.table.put_item(
                Item={
                    'id': uniqueID,
                    'CPUid': self.CPUid,
                    'sessionid': sessionid,
                    'timestamp': ts
                }
            )
            return "Registro guardado correctamente en DynamoDB."
        except (BotoCoreError, ClientError) as error:
            return f"Error al guardar el registro en DynamoDB: {error}"

    def list(self):
        """
        Lista todos los logs en la tabla CorporateLog donde CPUid coincide con el del sistema.

        Retorna:
        - Lista de logs o un mensaje de error si no se encuentran registros.
        """
        try:
            response = self.table.scan(
                FilterExpression="CPUid = :CPUid",
                ExpressionAttributeValues={":CPUid": self.CPUid}
            )
            logs = response.get('Items', [])
            return logs if logs else "No se encontraron registros para la CPU especificada."
        
        except (BotoCoreError, ClientError) as error:
            return f"Error al listar los registros en DynamoDB: {error}"