import ray
import functools
from concurrent.futures import Executor, Future
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import cpu_count


class RayExecutor(Executor):
    """Executor for asynchronous and distributed execution using Ray.

    Extends the Python `concurrent.futures.Executor` interface with a
    submit() function, for submitting work on Ray.
    """

    @functools.lru_cache
    def _decorate(self, fn):
        return ray.remote(fn)

    def submit(self, fn, *args, **kwargs) -> Future:
        # we wrap due to bad ray function type checking:
        # builtin or partial functions cannot be decorated using ray.remote
        wrapper = lambda *args, **kwargs: fn(*args, **kwargs)  # noqa
        _fn = self._decorate(wrapper)
        object_ref = _fn.remote(*args, **kwargs)
        return object_ref.future()

    def shutdown(self, wait: bool = ..., *, cancel_futures: bool = ...) -> None:
        ray.shutdown()
        return super().shutdown(wait, cancel_futures=cancel_futures)


threads = ThreadPoolExecutor(cpu_count())
process = ProcessPoolExecutor(cpu_count())
distributed = RayExecutor()
