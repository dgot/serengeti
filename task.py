import ray
import inspect
import functools
from typing import Any, Callable, Generator, Iterable, Union
from multiprocessing import cpu_count
from concurrent.futures import as_completed, Future
from concurrent.futures import ThreadPoolExecutor


THREADS = ThreadPoolExecutor(cpu_count())


def ray_executor(value, func, *args, **kwargs) -> Future:
    """Execute function on value using Ray"""
    _func = ray.remote(func)
    object_ref = _func.remote(value, *args, **kwargs)
    return object_ref.future()


def thread_executor(value, func, **kwargs) -> Future:
    """Execute function on value in a thread pool"""
    return THREADS.submit(func, value, **kwargs)


class LazyMonad:
    def __init__(self, value: object):
        self.compute = value if isinstance(value, Callable) else lambda: value

    def bind(self, function: Callable, *args, **kwargs):
        return LazyMonad(lambda: function(self.compute(), *args, **kwargs))


class StreamMonad:
    """Monadic Async Generator Composition"""

    def __init__(self, source: Union[Iterable, Generator], executor=thread_executor):
        self.executor = executor
        if isinstance(source, (Iterable, Generator)):
            self.source = source
        else:
            raise TypeError("Source must be a Generator or Iterable")

    def __iter__(self):
        return self.source

    def bind(self, function: Callable, ordered=False, **kwargs):
        function = functools.partial(function, **kwargs)
        source = self._stream(function, ordered=ordered)
        return StreamMonad(source)

    def compose(self, *functions):
        return compose(self.source, *functions)

    def _stream(self, function, executor=ray_executor, ordered=False) -> Generator:
        """Map function over source async or ordered"""
        executor = functools.partial(self.executor, func=function)
        futures = (executor(value) for value in self.source)
        if not ordered:
            futures = as_completed(futures)
        results = (self._get_result(future) for future in futures)
        return results

    def _get_result(self, result: Union[Future, Any]):
        if isinstance(result, Future):
            return result.result()
        return result


def compose(source: Union[Iterable, Generator], *functions: Callable) -> StreamMonad:
    """Compose N functions as StreamMonads on an iterable source"""
    bind = lambda monad, function: monad.bind(function)  # noqa
    initial = StreamMonad(source)
    composition = functools.reduce(bind, functions, initial)
    return composition
