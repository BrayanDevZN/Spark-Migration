from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from queue import Queue

from Migration.logs.logs import logger

class Channel:
    def __init__(self, name: str) -> None:
        self.name = name
        self.queue = Queue()
        logger.info(f"Channel {self.name} created.")

    def send(self, result) -> None:
        self.queue.put(result)

    def get(self):
        return self.queue.get()


class Task:
    """Executa tarefas em threads para compartilhar uma unica sessao Spark."""

    def __init__(self, max_workers: int, name: str, channel: Channel) -> None:
        self.name = name
        self.channel = channel
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=name,
        )

    def run(self):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                future = self.executor.submit(func, *args, **kwargs)

                def finished(completed):
                    try:
                        self.channel.send(completed.result())
                    except BaseException as error:
                        self.channel.send(error)

                future.add_done_callback(finished)
                return future

            return wrapper

        return decorator


# Canal onde vai receber o resultado das tasks.
ch = Channel(name="Migrations")

# Threads evitam criar uma JVM/SparkSession por tabela e excecoes nao precisam
# ser serializadas entre processos.
task = Task(
    max_workers=4,
    name="Migration_task",
    channel=ch
)

