import os
import rx
import asyncio
import ray
from typing import Optional, TypeVar
from functools import partial
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from pyarrow import RecordBatch
T = TypeVar('T')
LOOP = asyncio.get_event_loop()


def task(parallel: Optional[bool] = False):
    """Decorator for turning a function into an operator

    Parameters
    ----------
    parallel : Optional[bool], default is False
        Execute task in parallel, by default False
        Parallel execution is managed by Ray
    """
    def decorate(func):
        def call(func, value):
            if parallel:
                func = ray.remote(func)
                future = func.remote(value)
                result = ray.get(future)
            else:
                result = func(value)
            return result
        def decorated(source):
            def subscribe(observer, scheduler=None):
                def on_next(value):
                    try:
                        result = call(func, value)
                        observer.on_next(result)
                    except Exception as error:
                        observer.on_error(error)
                return source.subscribe_(
                    on_next,
                    observer.on_error,
                    observer.on_completed,
                    scheduler,
                )
            return rx.create(subscribe)
        return decorated
    return decorate


def read_parquet(filepath, batch_size, select=None) -> pa.Table:
    """Generator for reading and yielding batches from parquet files"""
    parquet_file = pq.ParquetFile(filepath, memory_map=True)
    for batch in parquet_file.iter_batches(
        batch_size=batch_size,
        columns=select,
        use_threads=True,
    ):
        yield batch


def read(filepath, batch_size, select=None, filter=None):
    """Returns an observable iterator for reading data"""
    extension = os.path.splitext(filepath)[-1]
    if extension == '.parquet':
        return rx.from_iterable(
            read_parquet(
                filepath=filepath,
                batch_size=batch_size,
                select=select
            )
        )
    else:
        raise TypeError(
            f'{extension} extension not supported'
        )


def combine(*observable):
    """Returns an observable combining other observables"""
    return rx.combine_latest()


def join(*sources):
    """Returns an observable iterator yielding batches of join results
    There are three ways of doing joins in a streaming context
    1: stream-stream join (infinite)
    2: stream-dataset join (infinite)
    3: dataset-dataset join (finite)
    """ 
    pass


def write(source, treekey, primarykey=None,
          name='serengeti', storage_dir='storage'):
    """Writes data to dataset"""
    if type(source, pd.DataFrame):
        table = pa.Table.from_pandas(source)
    if type(source, RecordBatch):
        table = pa.Table.from_batches(source, source.schema)
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    where = os.path.join(storage_dir, name)
    row_group_size = len(source)
    pq.write_table(table, where=where, row_group_size=)
