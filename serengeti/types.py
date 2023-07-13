from typing import Union, Iterable, Callable, Generator

Streamable = Union[
    Iterable,
    Generator,
    Callable[..., Union[Iterable, Generator]],
]
