from multiprocessing import cpu_count
from typing import Any, Callable, Generator, Iterable, Union
from concurrent.futures import as_completed, Future, ThreadPoolExecutor

import ray
import functools


THREADS = ThreadPoolExecutor(cpu_count())


def ray_executor(value, func, *args, **kwargs) -> Future:
    """Execute function on value using Ray"""
    _func = ray.remote(func)
    object_ref = _func.remote(value, *args, **kwargs)
    return object_ref.future()


def thread_executor(value, func, **kwargs) -> Future:
    """Execute function on value in a thread pool"""
    return THREADS.submit(func, value, **kwargs)


class StreamMonad:
    """An iterable StreamMonad for Monadic Generator Composition

    A StreamMonad represents an iterable data source and an execution context
    for how a function is mapped over the iterable data source.

    StreamMonad::bind allows for mapping a function over the StreamMonads source
    using the configured executor. Bind does not apply the function directly,
    but creates a generator of results and wraps it in a StreamMonad. As such
    this new StreamMonad represents the iterable downstream of results, as the
    function is applied on the current iterable StreamMonad.

    """

    def __init__(self, source: Union[Iterable, Generator], executor=thread_executor):
        self.executor = executor
        if isinstance(source, (Iterable, Generator)):
            self.source = source
        else:
            raise TypeError("Source must be a Generator or Iterable")

    def __iter__(self):
        return self.source

    def bind(self, function: Callable, ordered=False, **kwargs) -> "StreamMonad":
        """Bind function to the current StreamMonad's iterable source

        Parameters
        ----------
        function: Callable
            Function to map over the StreamMonad's source

        ordered: bool, default is False
            When False, function will be applied async and results will be yielded
            as they are available. Settings this to True will ensure that
            results of applying function on the stream, are yielded in the same
            order as the values are received.

        Returns
        --------
        StreamMonad
            An iterable StreamMonad representing the results of the function
            mapped over the current StreamMonad.

        """
        function = functools.partial(function, **kwargs)
        source = self._stream(function, ordered=ordered)
        return StreamMonad(source)

    def pipe(self, *functions) -> "StreamMonad":
        """Pipe the functions on this stream"""
        return pipe(self.source, *functions)

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


def pipe(source: Union[Iterable, Generator], *functions: Callable) -> StreamMonad:
    """pipe N functions as StreamMonads on an iterable source"""
    bind = lambda monad, function: monad.bind(function)  # noqa
    initial = StreamMonad(source)
    composition = functools.reduce(bind, functions, initial)
    return composition


# TODO: Support railway programming:
# Consider allowing providing multiple bind arguments: on_error, on_next, on_completed
# These arguments however makes the StreamMonad look like an observable dataclass.
# This is perhaps not so wrong, since composition of generators is essentially
# downstream generators observing / waiting for a value to be yielded from an
# upstream generator. Using generators is just a very neat way of implementing
# the observer pattern.
#
# Consider using a Result monad like Maybe, Table etc.
