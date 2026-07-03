from Migration.logs.logs import logger
from Migration.engine.init_spark import app_spark
from pyspark.sql import DataFrame
import time

#Essa classe serve para ler e transferir a tabela
class SparkDb:
    def __init__(
        self,
        url: str,
        table_name: str,
        new_url: str,
        user: str,
        password: str,
        new_user: str,
        new_password: str,
    ) -> None:
        
        self.url = url
        self.table = table_name
        self.spark = app_spark
        self.new_url = new_url
        self.user = user
        self.password = password
        self.new_user = new_user
        self.new_password = new_password
        
    #Le a tabela  
    def read(self, max_attempts: int = 3)-> DataFrame:
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(
                    f"Reading table {self.table} "
                    f"(attempt {attempt}/{max_attempts})"
                )

                df = (self.spark.
                      read.format("jdbc").
                      option("url", self.url).
                      option("dbtable", self.table).
                      option("driver", "org.postgresql.Driver").
                      option("user", self.user).
                      option("password", self.password).
                      option("connectTimeout", "30").
                      option("socketTimeout", "300").
                      option("tcpKeepAlive", "true").
                      load())

                logger.info(f"Table {self.table} loaded.")
                return df

            except Exception as error:
                if attempt == max_attempts:
                    raise RuntimeError(
                        f"Nao foi possivel ler a tabela {self.table} "
                        f"apos {max_attempts} tentativas: {error}"
                    ) from error

                logger.warning(
                    f"Temporary failure while reading {self.table}: {error}. "
                    "Retrying..."
                )
                time.sleep(3 * attempt)
        
    #Salva no novo banco de dados
    def save(self) -> None:
        try:
            logger.info(f"Writing table {self.table}...")
            
            (self.df.write.
             format("jdbc").
             option("url", self.new_url).
             option("dbtable", self.table).
             option("driver", "org.postgresql.Driver").
             option("user", self.new_user).
             option("password", self.new_password).
             mode("append").save()
             )
            
            logger.info(f"Table {self.table} written successfully.")
            
        except Exception as error:
            raise Exception(error)
        
    def run(self) -> str:
        self.df = self.read()
        self.save()
        return self.table
    

            
        
        
    
                
            
        
        
    
        
    
