import pyarrow
import pyarrow.parquet as pq
from typing import Iterable


def read_parquet(
    filepath: str, max_chunksize=None
) -> Iterable[pyarrow.RecordBatch]:
    """Stream record batches from a .parquet file

    NOTE: One RowGroup is loaded into memory at a time. If your max_chunksize
    is lower than the size of a RowGroup, the entire RowGroup is still read
    into memory, however `read` will only yield a RecordBatch of max_chunksize.

    NOTE: If your parquet file consists of row_groups with default 1mil record
    size. Then read will load 1mil record into into memory and yield batches
    from this row group. This is not due to read's implementation, but how
    parquet is designed.

    Parameters
    ----------
    filepath : str
        Local path to parquet file
    max_chunksize : int, optional
        Only yield batches of a max size,
        Defaults to the size of the RowGroup defined in the parquet file.

    Yields
    ------
    Generator[pyarrow.RecordBatch]
        A generator of record batches

    """
    parquet_file = pq.ParquetFile(filepath)
    for index in range(parquet_file.num_row_groups):
        row_group: pyarrow.Table = parquet_file.read_row_group(index)
        reader = row_group.to_reader(max_chunksize=max_chunksize)
        yield from reader
