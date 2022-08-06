# 1. Filter Pushdown
# With filter pushdown you can reduce the volume of data read in
# (i.e. by selecting only the columns you actually need).
# Question: How do we do this with arrow?
#
# Dremio supports a query engine on top of a data lake in azure
# for doing pushdown
#
# Arrow.datasets supports pushdown
#
# Possibility is to use ParquetDataset for query of parquet file:
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html

# 2. Memory-map
# But when you want to perform operations on the data, your computer still
# needs to unpack the compressed information and bring it into memory.
# With memory-map we can read 1 megabyte from the middle of a 1 terabyte table,
# and you only pay the cost of performing those random reads totalling 1 megabyte.

# Data Structure using memory map for Mapping Engine?
# A pyarrow.table backed by pyarrow.memory_map?

import os
import math
import psutil
import random
import numpy as np
import pandas as pd
import pyarrow as pa
from functools import partial, reduce
from pyarrow.lib import RecordBatch
from multiprocessing import cpu_count
from pyarrow.dataset import Expression
from itertools import compress

cheese = [
    'gouda', 'parmesan', 'danbo', 'feta',
    'roquefort', 'carmenbert', 'mozarella',
    'cojita', 'emmentaler', 'cheddar', 'cottage',
]


def example(n=1000000, m=4, choices=cheese) -> pa.Table:
    """Generate a pyarrow.Table with random strings

    Parameters
    ----------
    n : int, optional
        number of rows, by default 1000000
    m : int, optional
        number of columns, by default 4
    choices : list[str], optional
        List of strings to randomly choose from

    Returns
    -------
    pa.Table
        Table with random cheese
    """
    return pa.Table.from_arrays(
        arrays=[[random.choice(choices) for _ in range(n)] for _ in range(m)],
        names=[f'cheese_{i}' for i in range(m)],
    )


def memory():
    return psutil.Process(os.getpid()).memory_info().rss >> 20


def generate_df(N, M):
    """Generate N records over M columns"""
    return pd.DataFrame([
        {f'cheese_{i}': random.choice(cheese) for i in range(M)} for _ in range(N)
    ])


def generate_list(N, M):
    """Generate N records over M columns"""
    return {f'cheese_{col}': [random.choice(cheese) for row in range(N)] for col in range(M)}


def as_columns(df) -> pa.Table:
    """Pandas dataframe as a pyarrow.Table"""
    return pa.Table.from_pandas(df)


def get_map(name) -> pa.lib.MemoryMappedFile:
    """Returns a memory map of arrow file"""
    path = f'storage/{name}.arrow'
    return pa.memory_map(path)


# 133 ms ± 65.3 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
# 4 columns of 1 million records
def to_arrow(table, name) -> pa.lib.MemoryMappedFile:
    """Persists table as an arrow file and returns a memory_map"""
    path = f'storage/{name}.arrow'
    os.makedirs('storage') if not os.path.exists('storage') else None
    writer = pa.ipc.new_file(path, table.schema)
    writer.write(table)
    writer.close()


# 43.7 µs ± 419 ns per loop (mean ± std. dev. of 7 runs, 10000 loops each)
# Conclusion, reading from memory map is very performant
def read_arrow(name) -> pa.Table:
    """Read arrow file and returns Table"""
    path = f'storage/{name}.arrow'
    memory_map = pa.memory_map(path)
    return pa.RecordBatchFileReader(memory_map).read_all()


# 96 ms ± 587 µs per loop (mean ± std. dev. of 7 runs, 10 loops each) to memory
# Conclusion, it is feasable to store in arrow format and convert to dict of lists
# on the go, and apply mapping rules on pure python objects.
def read_arrow_to_dict(name):
    """Reads an arrow file from storage and returns it as a dict"""
    # going through pandas is 10ms faster than table.to_pydict()
    return read_arrow(name).to_pandas().to_dict('list')


# 80 ms ± 1.33 ms per loop (mean ± std. dev. of 7 runs, 10 loops each) -> to memory
# 274 ms ± 56 ms per loop (mean ± std. dev. of 7 runs, 10 loops each) -> to disk
def write_dict_to_arrow(dict):
    return pa.Table.from_pydict(dict)


def stream(memory_map) -> RecordBatch:
    """Stream batches from a memory map"""
    reader = pa.RecordBatchFileReader(memory_map)
    for i in range(reader.num_record_batches):
        yield reader.get_record_batch(i)


# Polymorphic application of mapping action on n dimensional array ("table")
# The action can be a composition of actions.
# 45.1 ms when running ' - '.join on two arrays of 500.000 elements
def apply(*table, action) -> map:
    return map(action, zip(*table))


def df_fast_columns(df):
    """Generator that transforms a pandas dataframe into
    a transposed numpy array, on which functions can lazily
    be applied in a map. (30ms on 1x3 mil rows 3 cols) """
    return df.to_numpy().transpose()


def and_masks(*masks):
    """Combine two or more boolean masks as AND"""
    return reduce(np.logical_and, masks)


def or_masks(*masks):
    """Combine two or more boolean masks as OR"""
    return reduce(np.logical_or, masks)


# 140ms to apply a function like this
# Takes 95ms on regular python objects
def df_apply(df, function) -> pd.Series:
    """
    Works like df.apply(func, axis=1), but is 20x faster
    for row wise operations.

    Example
    -------
    # concatenate all columns
    df['target'] = df_apply(df, ' - '.join)

    """
    if isinstance(df, pd.Series):
        return df.apply(function)
    elif len(df.columns) < 2:
        return df[df.columns[-1]].apply(function)
    else:
        arrays = df.to_numpy().transpose()
        result = list(map(function, zip(*arrays)))
    return pd.Series(result)


def get_slices(length, num_batches=None) -> list[slice]:
    """Create slice objects from length and number of batches
    Can be used to process sliceable objects in batches

    Parameters
    ----------
    length : int
        Length of the expected slicable object.
    num_batches : int
        Number of batches to create, default is cpu count
        Will round number of batches down to what is divisble
        with the length provided.

    Returns
    -------
    list[slices]
         List of slices
    """
    num_batches = num_batches or cpu_count()
    batch_size = math.ceil(length / num_batches)
    slices = [slice(pos, pos + batch_size)
              for pos in range(0, length, batch_size)]
    return slices


def arrow_py_stream(array, cast_batch_size=10000):
    """Stream arrow array values as python objects
    Arrow array is read and casted in batches in order to support
    out-of-core python functions on arrow arrays
    NOTE: 80.8 ms, list(arrow.to_numpy(False)), performance  optimized
    NOTE: 105 ms, list(arrow_py_stream(arrow)), memory optimized
    NOTE: 600 ms, arrow.tolist(), don't do this
    """
    offset = 0
    while offset < len(array):
        stop = cast_batch_size + offset
        for value in array[offset:stop].to_numpy(zero_copy_only=False):
            yield value
        offset = stop


# We could put recordBatches in the plasma object store
# This would make distributed computation using Ray possible

# ON TO AND FROM py.arrow AND MEMORY MAPPED FILES
# list --to--> pa.array 18ms --to--> list 60ms (through numpy, data copy)
# tuple --to--> pa.array 18ms --to--> tuple 81.3ms (through numpy)
# np_array --to--> pa.array 70ms --to--> 50ms

# If using py.array we get super fast filtering and memory mapped source data (out of core).
# This however requires transforming to python tuples for fast streaming operations (maybe a zip_batch generator).
# streaming operations on numpy.array is 4 times slower than python.list.
# With copying to python.list or tuple, our operations are still 3 times faster than numpy.
# Conclusion: The most balanced setup, could therefore be memory mapped arrow or parquet files.
# for zero cost selection and filtering of source columns, and memory performant.
# 4ms (filter, using pa.compute) + 80ms (array to tuple) + 120ms (apply) + 18ms (back to array) = 222ms

# A tuple with 1 million values takes ~70mb
# A dataset of 1 million records and 30 columns would require 2gb to load
# And 70mb for each mapping rule result being cached, if not in place transformation
class MappingFrame:
    def __init__(self, table, mask=None, index=None):
        self.mask = mask
        self.index = index if not isinstance(index, type(None)) else np.arange(len(table))
        self._table = table or pa.Table.from_arrays([])

    @property
    def columns(self):
        return self._table.column_names

    @staticmethod
    def _compose(*functions):
        """Compose one or more functions as a single function"""
        def composition(x):
            for f in functions:
                x = f(x)
            return x
        return composition

    @staticmethod
    def from_pandas(df):
        """Create a MappingFrame from a pd.DataFrame
        NOTE: 28ms per column for 1 mil values
        """
        table = pa.Table.from_pandas(df)
        mf = MappingFrame(table)
        return mf

    @staticmethod
    def from_arrow(table):
        """Create a MappingFrame from a pa.Table
        NOTE: 28ms per column for 1 mil values
        """
        mf = MappingFrame(table)
        return mf

    @staticmethod
    def from_record_batch(record_batch):
        return MappingFrame(
            pa.Table.from_arrays(
                record_batch.columns,
                record_batch.schema.names,
            )
        )

    def to_pandas(self):
        """Create a pd.DataFrame from MappingFrame"""
        return self._table.to_pandas()

    def to_arrow(self) -> pa.Table:
        return self._table

    def _arrow_to_list(self, array):
        return array.to_numpy().tolist()

    def apply(self, *actions):
        if len(actions) > 1:
            action = self._compose(*actions)
        else:
            action = actions[-1]
        arrays = list(map(self._arrow_to_list, self._table.columns))
        multiple = len(arrays) > 1
        iterable = arrays[-1] if not multiple else zip(*arrays)  # free
        target = np.empty(len(arrays[-1]), dtype='O').tolist()

        if multiple:
            for i, row in zip(self.index, iterable):
                target[i] = action(filter(None, row))
        else:
            iterable = filter(None, iterable)
            for i, value in zip(self.index, iterable):
                target[i] = action(value)
        return pa.array(target, pa.string())

    def select(self, *columns):
        return MappingFrame(
            index=self.index,
            mask=self.mask,
            table=self._table.select(list(columns)),
        )

    def filter(self, condition):
        mask = to_compute(condition, self._table)().to_numpy().tolist()
        index = compress(self.index, mask)
        return MappingFrame(
            mask=mask,
            index=index,
            table=self._table,
        )

    def __getitem__(self, column):
        return self._table[column]

    def __setitem__(self, column, value):
        if column in self._table.column_names:
            index = self._table.column_names.index(column)
            table = self._table.remove_column(index)
            table = table.add_column(index, column, value)
            self._table = table
        else:
            table = self._table.append_column(column, value)
            self._table = table


# 01.09.2021
# https://arrow.apache.org/docs/python/ipc.html
# Regarding writing and reading streams from arrow files etc.
# (...) An important point is that if the input source supports zero-copy reads
# (e.g. like a memory map, or pyarrow.BufferReader), then the returned batches
# are also zero-copy and do not allocate any new memory on read.
#
# Also showcases how to serialize and deserialize numpy arrays and pandas dataframes
# in a zero copy way, that can be shared betwene processes, as long as the arrays
# contains no python objects. Unsure if this works for python strings.

# Algorithm for persisting intermediate results, that allows for caching of independent
# mapping rules. Can return result as is, if input data is the same

# 1) Load columns and recordbatches needed from dataset using dataset.to_columns.filter
# 1.2) Load columns from sub-directories if using an intermdiate result. using dataset.to_columns.filter and table.concat
# 2) Execute apply in parallel on recordbatches.
# 3) Each thread persists their result as an arrow file in a '<target>/<batch_number>'
# 4)

# Row Group Level Caching OR simple anti-left join?
# Keep a rowGroupHash-<columns> that denotes a hash value for a given schema of columns.
# A dataset may therefore contain several hash values. If a rowGroupHash has been encountered before, return
# anti-left join on the

# Query arrow dataset based on conditions
# Overwrite is a filter of the input indexes, where said indexes in target are Null
# stream batches and apply functions in parallel


def invert_match_substring_regex(array, pattern):
    """Negation of pa.compute.match_substring_regex"""
    matches = pa.compute.match_substring_regex(array=array, pattern=pattern)
    filtered = pa.compute.invert(matches)
    return filtered


def to_compute(condition, table) -> callable:
    """Converts a mapping condition to a pyarrow.compute function

    NOTE: When using pyarrow.compute functions on memory mapped arrow arrays,
    a filter does not require any memory and a dataset larger than memory can thus
    be filtered in this way.

    Parameters
    ----------
    table : pyarrow.Table
        An arrow table of pyarrow.arrays
    condition : dict
        A Mapping Rule condition. See BASE_migration.config.test_mapping

    Example
    -------
    .. code: python
        import pyarrow as pa

        table = pa.Table(
            ['Hello World'],
            ['name__v']
        )

        condition = to_compute({
            'column': 'name__v',
            'value': 'Hello World'
        })

        filtered = table.filter(condition())
        filtered['name__v']

    Benchmark
    ---------
    # On a memory mapped arrow table
    10ms per expression with 1 million str values
    2MB memory usage

    """
    column = condition.get('column')
    value = condition.get('value')
    regex = condition.get('regex')
    negate = condition.get('negate')
    is_empty = condition.get('is_empty')

    array = table if not column else table[column]

    if 'is_empty' in condition.keys():
        if (not is_empty) or (is_empty and negate):
            compute = pa.compute.is_valid
            return partial(compute, values=array)
        else:
            compute = pa.compute.is_null
            return partial(compute, values=array)

    if value and not regex:
        compute = pa.compute.not_equal if negate else pa.compute.equal
        return partial(compute, x=array, y=value)

    if value and regex:
        compute = pa.compute.match_substring_regex
        return partial(compute, array=array, pattern=value)

    if value and regex and negate:
        compute = invert_match_substring_regex
        return partial(compute, array=array, pattern=value)

    raise ValueError(
        'Cannot convert {condition} to a valid compute function'
    )


def and_computes(*computes):
    """Reduce one or more boolean masks as AND into a single mask
    NOTE: Kleene Logic
    """
    return reduce(pa.compute.and_kleene, *computes)


def or_computes(*computes):
    """Reduce one or more boolean masks as OR into a single mask
    NOTE: Kleene Logic
    """
    return reduce(pa.compute.or_kleene, *computes)


def to_expression(condition) -> Expression:
    """Converts a condition to a filter expression"""
    column = condition.get('column')
    value = condition.get('value')
    regex = condition.get('regex')
    negate = condition.get('negate')
    is_empty = condition.get('is_empty')
    field = pa.dataset.field(column)

    if is_empty:
        return field.is_null()

    if is_empty and negate:
        return field.is_valid()

    if value and not regex:
        return field == value

    if value and regex:
        raise NotImplementedError(
            'pyarrow.dataset.Expressions does not support',
            'match substring on regex',
        )

    if value and regex and negate:
        raise NotImplementedError(
            'pyarrow.dataset.Expressions does not support',
            'inverse match substring on regex',
        )

    raise ValueError(
        'Cannot convert {condition} to a valid compute function'
    )