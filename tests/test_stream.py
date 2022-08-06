import pytest
from typing import Iterable
from functools import partial
from serengeti.stream import StreamMonad


def task(value, i):
    print(f't{i} {value}')
    return value


@pytest.mark.parametrize(
    'source', (
        ['ild', 'brand', 'fisk'],
        [1, 2, 3, 4],
        [None, '4', 2, 1.2],
    )
)
def test_stream_monad(source):
    t1 = partial(task, i=1)
    t2 = partial(task, i=2)
    t3 = partial(task, i=3)
    assert isinstance(StreamMonad(source).bind(t1), StreamMonad), (
        'StreamMonad.bind must always return a StreamMonad'
    )
    assert isinstance(StreamMonad(source), Iterable), (
        'A StreamMonad is expected to be iterable'
    )
    assert list(StreamMonad(source).bind(t1).bind(t2).bind(t3)), (
        'Iterating over composition expected to yield a result'
    )
