import functools
from concurrent.futures import Executor, as_completed, Future
from typing import Callable, Generator, Iterable, Union, Any
from serengeti.executor import distributed


DEFAULT_EXECUTOR = distributed


def stream(
    function: Callable,
    *iterables: Union[Iterable, Generator],
    executor: Executor = None,
    ordered: bool = False,
    **kwargs,
) -> Generator:
    """Returns a generator equivalent to map(fn, *values)
    but yielding function call results on iterables as they are completed (async).

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
    executor = (
        executor
        or DEFAULT_EXECUTOR
    )
    futures = (
        executor.submit(function, *args, **kwargs)
        for args in zip(*iterables)
    )
    if not ordered:
        futures = as_completed(futures)
    results = (  # safe unpack of future results
        future.result() if isinstance(future, Future)
        else future for future in futures
    )
    return results


class StreamMonad:
    """An iterable StreamMonad for Monadic Generator Composition

    A StreamMonad represents an iterable data source and an execution context
    for how a function is mapped over the iterable data source.

    StreamMonad::bind allows for mapping a function over the StreamMonads source
    using the configured executor. Bind does not apply the function directly,
    but creates a generator of results and wraps it in a StreamMonad. As such
    this new StreamMonad represents the iterable downstream of results, as the
    function is applied on the current iterable source.

    """

    def __init__(self, *iterables: Union[Iterable, Generator], executor: Executor = None):
        for iterable in iterables:
            if not isinstance(iterable, (Iterable, Generator)):
                raise TypeError("Iterables must be of type Generator or Iterable")
        self.iterables = iterables
        self.executor = executor or DEFAULT_EXECUTOR

    def __iter__(self):
        return (  # unpack if iterables only contain 1 value
            values[0] if len(values) == 1 else values for values in zip(*self.iterables)
        )

    def bind(self, function: Callable, ordered=False, **kwargs) -> "StreamMonad":
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
                f"Parameter 'ordered' must be of type <bool> but got '{type(ordered)}'"
            )
        iterable = stream(
            function,
            self,
            executor=self.executor,
            ordered=ordered,
            **kwargs,
        )
        downstream = StreamMonad(iterable, executor=self.executor)
        return downstream

    def pipe(self, *functions) -> "StreamMonad":
        """Pipe functions on this stream"""
        return pipe(self, *functions)

    def on_next(self, function: Callable, **kwargs) -> "StreamMonad":
        """Bind function to the stream of all elements yielded upstream"""
        return self.bind(function=function, **kwargs)

    def on_completed(self, function: Callable, **kwargs) -> "StreamMonad":
        """Only bind function to the last element yielded upstream"""
        last_element = lambda: element_at(self, index=-1)  # noqa
        return StreamMonad(last_element, executor=self.executor).bind(function, **kwargs)

    def on_error(self, function: Callable, **kwargs) -> "StreamMonad":
        """Only bind function to the stream of failures yielded upstream"""
        raise NotImplementedError


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


def element_at(iterable: Union[Iterable, Generator], index: int, default: Any = None) -> Generator:
    """Returns the element at a specified index in an iterable"""
    nth_element = None
    for pos, value in enumerate(iterable):
        nth_element = value
        if pos == index:
            yield nth_element or default
    if index == -1:
        yield nth_element


# TODO: Support railway oriented programming with result monads and allow
# result side effects such as storage and cache.

# TODO: Consider allowing providing multiple bind arguments: on_error, on_next, on_completed
# like rxpy does with its observer pattern. Composition of generators lend itself
# as a very neat way of implementing the observer pattern, since generators
# await and "observe" for results upstream, aand process values as they are
# received.
