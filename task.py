import ray
import inspect
import functools
from typing import Any, Callable, Generator, Iterable, Union
from multiprocessing import cpu_count
from concurrent.futures import as_completed, Future
from concurrent.futures import ThreadPoolExecutor


THREADS = ThreadPoolExecutor(cpu_count())


def ray_executor(value, func, **kwargs) -> Future:
    """Execute function on value using Ray"""
    _func = ray.remote(func)
    object_ref = _func.remote(value)
    return object_ref.future()


def thread_executor(value, func, **kwargs) -> Future:
    """Execute function on value in a thread pool"""
    return THREADS.submit(func, value)


def wartial(func, **kwargs) -> callable:
    """Partial wrapper since ray does not recognize partials as true functions"""
    return lambda *a, **b: functools.partial(func, **kwargs)(*a, **b)


def stream(function, source, executor=ray_executor, ordered=False) -> Generator:
    """Map function over an iterable source async or syncronized"""
    partial = functools.partial(executor, func=function)
    futures = lambda: (partial(value) for value in source)  # noqa
    stream = as_completed(futures()) if not ordered else futures()
    return (future.result() for future in stream)


def task(func=None, executor=ray_executor, ordered=False):
    """Decorator for turning a function into a generator"""

    def decorate(func):
        def _lazy(source=None, **kwargs):
            partial = wartial(func, **kwargs)
            if source:  # in case the task is called with no kwargs
                return stream(
                    function=partial,
                    source=source,
                    executor=executor,
                    ordered=ordered,
                    **kwargs,
                )
            return stream

        return _lazy

    if callable(func):  # if decorated with parameters
        return decorate(func)
    return decorate


def pipe(*functions, executor=ray_executor, ordered=False):
    """compose N functions together as a generator"""

    def _pipe(source, target):
        if inspect.isgeneratorfunction(target):
            return (value for value in target(source))
        if not isinstance(source, Iterable):
            source = [source]
        return stream(target, source, executor, ordered)

    return lambda source: functools.reduce(
        _pipe, functions[1:], _pipe(source, functions[0])
    )


class LazyMonad:
    def __init__(self, value: object):
        self.compute = value if isinstance(value, Callable) else lambda: value

    def bind(self, function: Callable, *args, **kwargs):
        return LazyMonad(lambda: function(self.compute(), *args, **kwargs))


class StreamMonad:
    def __init__(self, source: Union[Iterable, Generator]):
        if isinstance(source, (Iterable, Generator)):
            self.source = source
        else:
            raise TypeError('Source must be a Generator or Iterable')

    def bind(self, function: Callable, *args, **kwargs):
        executor = functools.partial(ray_executor, func=function)
        futures = lambda: (executor(value, *args, **kwargs) for value in self.source)  # noqa
        results = (self._get_result(future) for future in as_completed(futures()))
        return StreamMonad(results)

    def _get_result(self, result: Union[Future, Any]):
        if isinstance(result, Future):
            return result.result()
        return result