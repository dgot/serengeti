# Notes

## Schemas

Should be possible to infer or define a schema for a rule

## tokenized strings with patterns and pointers

Support for declaring named token patterns that columns can be tokenized with.
Latter supports selection of and operations on specific named tokens.
Like concatenation of specific tokens in different columns.
Reduced memory footprint and reduced number of rules.

## Data Processing Client

Possible small client for applying rules on arrow arrays written in rust.
Alternative: arrow backed numpy arrays in python or vaex,
which also has very good performance using views and inplace mutations.

## Parallelization

Distribute on partition keys.
Parallelize on record batches, or as well as possible local worker
parallelization on a partition key. Format in arrow. dataset -> partition keys -> record_batches

## Pushdown on Dataset

Pushdown on dataset. Only query columns needed for a rule branch in parquet or arrow file.

RecordBatches are read from source file one at a time (or a memory mapped arrow file)
An index is generated for from previous batch end to length of new batch.

Masks are created using pyarrow.compute and arrow table is filtered using mask
We also mask the numpy index to get the index of the rows in the filtered arrow table.

Depending on which is fastest, source fields are turned into numpy arrays or
python lists on demand when an action needs to be mapped.

Targets fields are createad as empty numpy or python lists.
We then apply a function by doing inplace manipulation of the target array
using compress with an iterable and a mask.

With this approach we can make use of very fast arrow filtering as well as
numpy masks, together with fast string manipulations on python lists.

```python
source = recordBatch.to_pandas().to_numpy().transpose()  # 20 ms x column
target = np.array.empty(len(recordBatch), dtype='O')  #1,85 ms x 1 mil
mask = to_compute({'column': 'col1', 'value': 'fire'}, recordBatch)  # 10ms x 1 mil
masked = [array[mask] for array in source]

# Pure Python
py_target = target.tolist() # 8 ms x 1 mil
py_mask = mask.to_numpy().tolist()  # 10ms x 1 mil values
py_arrays = [array.tolist() for array in masked]  # 10 ms per array
iterable = py_arrays[-1] if len(py_arrays) < 2 else zip(*py_arrays)  # free
for i, v in compress(enumerate(iterable, pmask)):
  py_target[i] = action(v)

# Numpy alternative, takes 180ms
iterable = masked[-1] if len(masked) < 2 else zip(*masked)  # free
target = np.array.empty(len(recordBatch), dtype='O')  #1,85 ms x 1 mil
target[mask] = list(map(func, compress(iterable, mask)))
```

tA condition mask is createad using pyarrow.compute

Mod argument: IO might be expensive, could be faster to create numpy views
over all collumns, and do inplace transformations.

Might be slow for distributig data to the workers. But this hould be a one time
staging operation, for the dataset.

## Workers with result buffers as caching

**Pro:** Result buffers can be folded dynamicly as they are a cache. Potential benefits
over time, as you work with your dataset and perfect your rules. or need to combine them
in new ways. You can skip calculation.

**Against:** Might be faster to just calculate result and return target field
directly back to parent.

## Stream data from blob directly to workers, feather format
Pro: Reduce IO (networking roundtrips)
Against: Dataset needs to have some pre-partitioning so the worker
knows what to fetch. When should this partitioning happen?

## Distributed Data Model and Cachine
A rule (digraph of actions) represents a result buffer (cache).
Its target column will be the name of its cache / partition of dataset.

Applying a rule writes transformation result as RecordBatches to buffer.
RecordBatch belongs to a partition.

When you need to calculate the result, of a set of rules, you fold over
the buffers and merge the results into a new dataset.

Since buffers are backed by arrow files, we can be persist them as such,
which may optimize performance greatly when working with mapping rules over time.

Workers operate on same staged source. Using ray we can distribute in arrow format for zero copy reads

## Record Caching Mechanism

Each rule represents a result buffer with a schema.
RecordBatches in the arrow are stored with a hash_value of batch.

This value can be used by a downstream rule for LRU cache of RecordBatches
and return the result of its previous transformation on that RecordBatch

## DSL Examples

```python
mf['title__v'] = mf.select(columns).filter(conditions).apply('-'.join, str.upper)

# A mappingframe is a contextmanager for declaring mappings on a dataset
# It is lazy evaluated and will not carry out actual instructions, but register
# expressions as virtual columns on the mapping frame.
with MappingFrame.open(dataset) as mf:
  title__v = mf.select(columns).filter(conditions).apply('-'.join)
  name__v = mf.select(title__v).apply(str.upper)

# An engine can be used to execute a mapping, locally, distributed etc.
# With cachin using caching services etc.
engine = MappingEngine(distributed)
engine.submit('s3://mydata.parquet', ruleset)
engine.submit('s3://mydataset', mf)

# Or simply use with pandas
df['title__v'] = apply(df[['name', 'title']], '-'.join)

```
