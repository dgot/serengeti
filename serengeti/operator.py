from typing import Union, Iterable, Generator, Any


def element_at(
    iterable: Union[Iterable, Generator],
    index: int,
    default: Any = None
) -> Any:
    """A generator that takes an iterable source and yields the element
    at the specified index, or a default value if no element exists at index"

    Parameters
    ----------
    iterable : Union[Iterable, Generator]
        Source to yield element at index from
    index : int
        The element at this index to yield
    default : Any, optional
        Value to yield if no element exists at index, by default None

    Yields
    ------
    Iterator[Any]
        Element at index or default value

    Example
    -------
    >>> data = [1, 2, 3, 4]
    >>> element = element_at(data, index=2)
    """
    nth_element = None
    for pos, value in enumerate(iterable):
        nth_element = value
        if pos == index:
            yield nth_element or default
    if index == -1:
        yield nth_element


def take(iterable: Union[Iterable, Generator], N: int = 0):
    """A generator that takes an iterable source and yields N elements from it"""
    if not isinstance(N, int):
        raise TypeError(
            f"Unsupported type for parameter 'N': "
            f"expceted <class 'int'> but got {type(N)}"
        )
    elements = (value for pos, value in enumerate(iterable) if N > pos)
    yield from elements
