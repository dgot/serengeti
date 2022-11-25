import pytest
from typing import Iterable
from serengeti.stream import StreamMonad
from concurrent.futures import ThreadPoolExecutor


def task(value):
    print(f"{value}")
    return value


def task_with_kwarg(value, kwarg=None):
    print(f"{value}")
    return kwarg


properties = (
    ["ild", "brand", "fisk"],
    [1, 2, 3, 4],
    [None, "4", 2, 1.2],
)


@pytest.fixture
def executor():
    """Thread based executor for running tests"""
    threads = ThreadPoolExecutor(4)
    yield threads
    threads.shutdown()


@pytest.mark.parametrize("source", properties)
def test_stream_monad(source, executor):
    assert isinstance(
        StreamMonad(source, executor=executor).bind(task), StreamMonad
    ), "StreamMonad.bind must always return a StreamMonad"
    assert isinstance(
        StreamMonad(source, executor=executor), Iterable
    ), "A StreamMonad is expected to be iterable"
    assert list(
        StreamMonad(source, executor=executor).bind(task).bind(task)
    ), "Iterating over composition expected to yield a result"
    assert list(StreamMonad(source, executor=executor).bind(task_with_kwarg, kwarg="hello")) == [
        "hello" for _ in range(len(source))
    ]
