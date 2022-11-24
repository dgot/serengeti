# Serengeti

Orchestrate functions together as a pipeline of "tasks" processing a stream of
data using the `StreamMonad`.

```python
import time
from functools import partial
from serengeti.stream import StreamMonad


def task(value, i):
    time.sleep(i)
    print(f't{i} {value}')
    return value


t1 = partial(task, i=1)
t2 = partial(task, i=2)
t3 = partial(task, i=3)

source = ['ild', 'brand', 'fisk']
stream = StreamMonad(source).bind(t1).bind(t2).bind(t3)

# Get results by iterating over the stream
result = list(stream)

```

Compositions created using `StreamMonad.bind` are asynchronous and distributed
out of the box, using `ray`.

> NOTE: Current implementation is low level and will serve the purpose of
> creating a higher level API.

______

## 1. Install Poetry

Ensure you have poetry installed, which is required for installation. It can
be installed following this [installation guide](https://python-poetry.org/docs/#installation).

## 2. Clone repository from GitHub

```bash
git clone https://github.com/dgot/serengeti.git
```

## 3. Install using poetry

Navigate to repository root and install

```bash
cd ./serengeti
poetry install
```
