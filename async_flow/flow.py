from asyncio import Queue, ensure_future, gather
from dataclasses import dataclass
from time import time
from typing import Callable, Iterable, Any, AsyncGenerator


@dataclass
class Job:
    inset_time: float
    data: Any


class Worker:
    def __init__(self, func: Callable, uid: int, args: tuple, kwargs: dict):
        self.func = func
        self.uid = uid
        self.args = args
        self.kwargs = kwargs


class Producer(Worker):
    func: Callable[..., AsyncGenerator]

    async def start(self, jobs_queue: Queue, shards_queue: Queue):
        shard = await shards_queue.get()
        while shard is not None:
            async for data in self.func(*self.args, shard=shard, **self.kwargs):
                await jobs_queue.put(Job(data=data, inset_time=time()))
            shards_queue.task_done()
            shard = await shards_queue.get()
        # handle None value
        shards_queue.task_done()


class Consumer(Worker):
    async def start(self, queue: Queue):
        job: Job = await queue.get()
        while job is not None:
            await self.func(*self.args, job=job, **self.kwargs)
            queue.task_done()
            job = await queue.get()
        # handle None value
        queue.task_done()


class AsyncFlow:
    def __init__(self, queue_size=100_000):
        self.queue_size = queue_size

        self.num_of_producers = None
        self.producer_func = None
        self.producer_args = None
        self.producer_kwargs = None
        self.shards = None

        self.num_of_consumers = None
        self.consumer_func = None
        self.consumer_args = None
        self.consumer_kwargs = None

    def set_consumers(self, num_of_consumers: int, func: Callable, args: tuple = None, kwargs: dict = None):
        self.num_of_consumers = num_of_consumers
        self.consumer_func = func
        self.consumer_args = args or tuple()
        self.consumer_kwargs = kwargs or {}

    def set_producers(self, func: Callable, shards: iter, args: tuple = None, kwargs: dict = None,
                      num_of_producers: int = None):
        if num_of_producers is None or num_of_producers > len(shards):
            num_of_producers = len(shards)

        self.producer_func = func
        self.shards = shards
        self.producer_args = args or tuple()
        self.producer_kwargs = kwargs or {}
        self.num_of_producers = num_of_producers

    def init_producers(self):
        return [Producer(func=self.producer_func, args=self.producer_args, kwargs=self.producer_kwargs, uid=i)
                for i in range(self.num_of_producers)]

    def init_consumers(self):
        return [Consumer(func=self.consumer_func, args=self.consumer_args, kwargs=self.consumer_kwargs, uid=i)
                for i in range(self.num_of_consumers)]

    @staticmethod
    async def start_producers(producers: Iterable[Producer], jobs_queue: Queue, shards_queue: Queue):
        await gather(*[p.start(jobs_queue=jobs_queue, shards_queue=shards_queue) for p in producers])

    @staticmethod
    async def start_consumers(costumers: Iterable[Consumer], jobs_queue: Queue):
        await gather(*[c.start(jobs_queue) for c in costumers])

    async def put_shards_in_queue(self, shards_queue: Queue):
        for shard in self.shards:
            await shards_queue.put(shard)

        # Stop producers
        await self.send_stop_signal(shards_queue, self.num_of_producers)

    @staticmethod
    async def send_stop_signal(queue: Queue, num_of_signals: int):
        for _ in range(num_of_signals):
            await queue.put(None)

    async def start(self):
        jobs_queue = Queue(maxsize=self.queue_size)

        shards_queue = Queue(maxsize=len(self.shards) * 2)  # for None items
        ensure_future(self.put_shards_in_queue(shards_queue))

        consumers = self.init_consumers()
        ensure_future(self.start_consumers(consumers, jobs_queue))
        producers = self.init_producers()
        await self.start_producers(producers, jobs_queue, shards_queue)

        # Stop consumers
        await self.send_stop_signal(jobs_queue, self.num_of_consumers)

        await jobs_queue.join()
