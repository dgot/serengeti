import functools
import inspect

from concurrent.futures import Executor, as_completed, Future
from typing import Callable, Generator, Iterable, Optional, Union, Any

from serengeti import executor
from serengeti import operator
from serengeti.types import Streamable


DEFAULT_EXECUTOR = executor.threads


def map_async(
    function: Callable,
    *iterables: Union[Iterable, Generator],
    executor: Optional[Executor] = None,
    ordered: bool = True,
    **kwargs,
) -> Iterable[Any]:
    """Returns a generator equivalent to map(fn, *iterables)
    but maps the function over the iterables async and non_blocking using
    provided executor. Nothing is executed before you iterate over the
    returned generator.

    Works similar to concurrent.futures.Executor.map_async, in that functions
    are not necessarily executed and yielded in order.

    If <ordered> is True, results will be yielded in the same order as the
    values are ordered in the *iterables.

    Any executor adhering to the `concurrent.futures.Executor` interface can
    be provided and the function will be executed using provided executor.

    Parameters
    ----------
    function : Callable
        A callable that will take as many arguments as there are
        passed iterables.
    *iterables : Union[Iterable, Generator]
        iterables to stream function over
    executor : Executor
        The executor instance to submit function calls to, will default to
        a ThreadPoolExecutor if nothing is provided.
    ordered : bool, optional
        Yield results in same order as they are passed, by default False.

    Returns
    -------
    Generator
        An iterator equivalent to: map(func, *iterables) but the calls may
        be evaluated and yielded out-of-order (async).

    """
    executor = executor or DEFAULT_EXECUTOR
    futures = (
        executor.submit(function, *args, **kwargs)
        for args in zip(*iterables)
    )
    if not ordered:
        futures = as_completed(futures)
    results: Generator = (  # safe unpack of future results
        future.result() if isinstance(future, Future)
        else future for future in futures
    )
    return results


class Stream:
    def __init__(self, *iterables: Streamable):
        self.iterables = iterables

    def __iter__(self):
        iterables = [
            iterable() if callable(iterable)
            else iterable for iterable in self.iterables
        ]  # call to the iterable, if it is a callable returning an iterable
        for iterable in iterables:  # check if unpacked sources are Iterable
            if not isinstance(iterable, (Iterable, Generator)):
                raise TypeError(
                    "Iterables must be of type Generator or Iterable"
                )
        unpacker = (  # noqa
            # unpack if iterables only contain 1 value
            values[0] if len(values) == 1 else values
            for values in zip(*iterables)
        )
        return unpacker


class StreamMonad:
    """An Iterable StreamMonad for Monadic Async Generator Composition

    A StreamMonad represents an iterable data source and an execution context
    for how a function is mapped over the iterable data source.

    StreamMonad::bind allows for mapping a function over the StreamMonads
    source using the configured executor. Bind does not apply the function
    directly, but creates a generator of results and wraps it in a StreamMonad.
    As such this new StreamMonad represents the iterable downstream of results,
    as the function is applied on the current iterable source.

    """

    def __init__(
        self,
        *iterables: Streamable Union[Iterable, Generator, Callable[..., Union[Iterable, Generator]],
        executor: Optional[Executor] = None,
    ):
        self.iterables = iterables
        self.executor = executor or DEFAULT_EXECUTOR

    def __iter__(self):
        iterables = [
            iterable() if callable(iterable)
            else iterable for iterable in self.iterables
        ]  # call to the iterable, if it is a callable returning an iterable
        for iterable in iterables:  # check if unpacked sources are Iterable
            if not isinstance(iterable, (Iterable, Generator)):
                raise TypeError(
                    "Iterables must be of type Generator or Iterable"
                )
        unpacker = (  # noqa
            # unpack if iterables only contain 1 value
            values[0] if len(values) == 1 else values
            for values in zip(*iterables)
        )
        return unpacker

    def bind(
        self,
        function: Callable,
        ordered=True,
        **kwargs
    ) -> "StreamMonad":
        """Bind function to the current StreamMonad's iterable source

        Parameters
        ----------
        function: Callable
            Function to map over the StreamMonad's source
        **kwargs: Mapping
            Keyword arguments to pass

        Returns
        --------
        StreamMonad
            An iterable StreamMonad representing the results of the function
            mapped over the current StreamMonad.

        """
        if not isinstance(ordered, bool):
            raise TypeError(
                "Parameter 'ordered' must be of type <bool> but got "
                f"'{type(ordered)}'"
            )
        # If the function itself is a generator, its results will be the
        # downstream and we can thus wrap it in a StreamMonad and simply
        # return it.
        if inspect.isgeneratorfunction(function):
            iterable = lambda: function(self, **kwargs)  # noqa
        # We wrap the map_async call signature in a lambda, so as to makes
        # the generator re-iterable, by forcing its creation everytime you
        # iterate over the generator.
        else:
            iterable = lambda: map_async(  # noqa
                function,
                self,
                executor=self.executor,
                ordered=ordered,
                **kwargs,
            )
        downstream = StreamMonad(
            iterable, executor=self.executor
        )
        return downstream

    def pipe(self, *functions) -> "StreamMonad":
        """Pipe functions on this stream"""
        return pipe(self, *functions)

    def on_next(self, function: Callable, **kwargs) -> "StreamMonad":
        """Bind function to the stream of all elements yielded upstream"""
        return self.bind(function=function, **kwargs)

    def on_completed(self, function: Callable, **kwargs) -> "StreamMonad":
        """Only bind function to the last element yielded upstream"""
        last_element = lambda: operator.element_at(self, index=-1)  # noqa
        return StreamMonad(last_element, executor=self.executor).bind(function, **kwargs)

    def on_error(self, function: Callable, **kwargs) -> "StreamMonad":
        """Only bind function to the stream of failures yielded upstream"""
        raise NotImplementedError


# =========================================================================== #
# CONVENIENCE METHODS
# =========================================================================== #


def pipe(
    iterable: Union[Iterable, Generator],
    *functions: Callable,
    executor: Executor = None,
) -> StreamMonad:
    """pipe N functions as StreamMonads on an iterable source"""
    executor = executor or DEFAULT_EXECUTOR
    bind = lambda monad, function: monad.bind(function)  # noqa
    initial = StreamMonad(iterable, executor=executor)
    composition = functools.reduce(bind, functions, initial)
    return composition


# TODO: Support railway oriented programming with result monads and allow
# result side effects such as storage and cache.

# TODO: Consider allowing providing multiple bind arguments: on_error, on_next, on_completed
# like rxpy does with its observer pattern. Composition of generators lend itself
# as a very neat way of implementing the observer pattern, since generators
# await and "observe" for results upstream, aand process values as they are
# received.
