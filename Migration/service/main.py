from Migration.logs.logs import logger
from Migration.engine.connect import Engine
from Migration.tasks.task_db import SaveDb
from Migration.engine.init_task import ch
from Migration.schema.model import Formate

#Faz toda migração do banco de dados
class Migration:
    def __init__(self, data:dict, new_data:dict)->None:
        
        self.__url = Formate(
            host=data["host"],
            port=data["port"], 
            dbname=data["dbname"],
            password=data["password"],
            user=data["user"]
        )
        
        self.__newurl = Formate(
            host=new_data["host"],
            port=new_data["port"], 
            dbname=new_data["dbname"],
            password=new_data["password"],
            user=new_data["user"]
        )

        self.__source_user = data["user"]
        self.__source_password = data["password"]
        self.__destination_user = new_data["user"]
        self.__destination_password = new_data["password"]
        
        self.tables = Engine(url=self.__url.url_alchemy(), name=data["dbname"]).tables()
        
        self.channel = ch
      
    #Incia uma task para cada tabela do banco de dados  
    def init_task(self)->None:
        for table in self.tables:
            SaveDb(
                table_name=table,
                url=self.__url.url_spark(),
                new_url=self.__newurl.url_spark(),
                user=self.__source_user,
                password=self.__source_password,
                new_user=self.__destination_user,
                new_password=self.__destination_password,
            )
          
    #Chama o metodo init_task para iniciar as tasks, e depois entra num loop e espera todas as tasks finalizarem  
    def run(self) -> None:
        logger.info("Starting database migration...")
        
        self.init_task()
        
        tables_finish = []
        
        while len(tables_finish) != len(self.tables):
            
            data = self.channel.get()

            if isinstance(data, BaseException):
                raise RuntimeError(f"Falha durante a migracao: {data}") from data

            if data is not None:
                logger.info(f"Table {data} migrated successfully.")
                tables_finish.append(data)
                
        
        logger.info("Database migration completed successfully.")
                
            
                
        
        
            
        
    
        
        
        
        
