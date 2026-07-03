from sqlalchemy import  create_engine, inspect, Engine
from Migration.logs.logs import logger

#Cria Conexão do banco de dados e retorna o nome de todas as tabelas

class Engine:
    def __init__(self, url:str, name:str)-> None:
        
        #Url que conecta com o banco de dados
        self.__url = url
        
        self.name = name
        
        self.eng = self.con()
    
    #Cria a engine do banco de dados
    def con(self) -> Engine:
        
        try:
            logger.info(f"Connecting to database {self.name}...")
            
            eng = create_engine(
                url=self.__url
            )
            
            logger.info(f"Connected to database {self.name}.")
            
            return eng
        
        except Exception as error:
            
            raise Exception(error)
     
    #Pega todas as tabelas do banco e retorna em uma lista  
    def tables(self) -> list:
        
        try:
            logger.info(f"Discovering tables in database {self.name}...")
            
            inspector = inspect(self.eng)
            
            tables = inspector.get_table_names()
            
            logger.info(
                f"Discovered {len(tables)} table(s): "
                f"{', '.join(tables) if tables else '[none]'}"
            )
            
            return tables
        
        except Exception as error:
            
            raise Exception(error)
        
    
        
        
    
        
        
        
            
        
        
        
    
        
        
        
        
        

