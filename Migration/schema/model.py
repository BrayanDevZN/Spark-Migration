from urllib.parse import quote_plus
#serve pra formatar od dados
class Formate:
    
    def __init__(self, host:str, port:int, user:str, password:str, dbname:str)->None:
        self.host = host
        self.port = port
        self.user = user
        self.Pass = password
        self.db = dbname
       
    #formata os dados de conexão pra url do spark   
    def url_spark(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"
    
    #forma para url do sqlalchemy
    def url_alchemy(self) ->str:
         password = quote_plus(self.Pass)

         return f"postgresql://{self.user}:{password}@{self.host}:{self.port}/{self.db}"
            
        