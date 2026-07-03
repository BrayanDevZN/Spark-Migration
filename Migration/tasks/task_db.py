
from Migration.engine.init_task import task
from Migration.schema.control import SparkDb

#Transforma todo sparkdb em uma task
@task.run()
def SaveDb(
    url: str,
    new_url: str,
    table_name: str,
    user: str,
    password: str,
    new_user: str,
    new_password: str,
):
    tb = SparkDb(
        url=url,
        new_url=new_url,
        table_name=table_name,
        user=user,
        password=password,
        new_user=new_user,
        new_password=new_password,
    ).run()
    return tb
    


