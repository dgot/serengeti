# Serengeti

"The endless plains. Every year millions of Gnus and Zebras migrate over the
serengeti following a strict pattern"

This is a specification of Serengeti, a DSL and SDK for declarative data pipelines. The serengeti SDK is an async arrow based streaming implementation of the DSL with build in telemetry and observability.

SDK Tech: ray, pyarrow, opentelemetry, expression

The aim of the DSL is to make it dead simple to orchestrate code and data in
a declarative way, which can scale to support very large pipelines and datasets, by being async and distributed first.

Syntax wise only three parameters are required for a task in the pipeline: `name`, `task` and `source`. All tasks must take these three parameters, but can define any additinal parameters. A task's parameters are defined by its function parameters.

```yml
PIPELINE:
  - name: extract
    task: read
    source: s3://documents.parquet

  - name: transform
    task: mapping
    source: extract
    rules: mapping.yml

  - name: load
    task: doc_creator
    source: transform

```

## General Parameters

```yaml
  parallel: False  # Will execute task in the current process with no parallelization / distribution of work.

  ordered: False  # Will process, await and yield events in the received order. Usefull when tasks has side-effects, or when an event is dependent on the order of previous events. Default is `False` and task results are yieldid in an async fashion, unorded, as they are completed.

  trigger: [failed, success, completed]  # Mark when a task receives events by an upstream task. Default is `success`. `completed` will trigger task when the upstream task is exhausted of events. `failed` will make task only receive failed upstream events. A task can have multiple triggers.

  retries: 0  # Number of times to retry task if task raises an exceptiption. Default is 0 retries, but can be adjusted.

  batch_size: 500  # maximum batch_size to receive from upstream task

```

## SDK Aim

The aim of the SDK is to make it dead simple to write synchronized code and
orchestrate it in an async streaming pipeline, which can be distributed out of the box on e.g a kubernetes cluster, allowing you to easily create pipelines, that can operate on datasets larger than memory.

The aim of the SDK is also to enable a hybrid cloud architecture out of the box, in which the SDK can be executed on a raspberry pi with local storage and when needed and can scale to use cloud computing resources when available, like datalakes, clusters, queues etc.

It differs from Apache Beam in that it aims to be more simple and light weight, and it differs from frameworks like cellery in that it is not ment to be a framework for creating message broker systems, but an API and SDK for writing scalable data pipelines dead easy.

## SDK Features and Implementation Considerations

- Arrow as the primary memory model of data objects flowing in the stream.
Using arrow as the primary memory model of data objects, will enable support
for pushdown, query and processing of distributed datasets with zero copy reads
between tasks on the same compute as well as zero serialization of data over
network using arrow flight. Furhter it also opens up for potential of tasks being implemented in different languages using Arrow Flight and gRPC. Making serengeti truly language agnostic.

- By using arrow we can ensure type consisteny by utilizing its schema support.

- Auto backpressure and out of core by using reactive streams and arrow.

- parallel execution using ray, making it possible to scale out to
execution in a cluster.

## Storage, rerun and data lineage

One of the design goals of Serengeti is to enable re-streaming from a specific
checkpoint in time, as well as enable auditing and validation of a pipeline
both at runtime and after runtime. If each data object are immutable and persisted between tasks, checkpointing is simply telling the pipeline to re-run from a a specific point in the pipeline. If the source which a particular task depends upon is a persisted dataset of data objects, we can simply iterate data objects in the source and execute the pipeline from there, given that the upstream task definition has not invalidated the source.

## Compose multiple pipelines together

It is easy to compose a single pipeline from multiple pipeline definitions.
Pipelines are composed by referencing tasks as sources of eachother between
files. In other words, if i have a task in `pipeline.yml`, said task can be an
observer of a task in by referencing it like this:

```yaml
tasks:
  - name: clean
    task: mapping
    source: !pipeline my_pipeline.yml::load
```

It features easy persistence of results between tasks, auditing and reporting
of pipeline execution.

As tasks receives data and execute on it, their results are written to a `source`. Any potential downstream task can source another task and ingest results emittet by that task.

A task is an observer of one or more upstream task sources. Sources are both observers of task results as well as an observable

Data streamed between tasks is facilitated its results are published on the source. A source can be viewed as a queue that can be emptied by downstream tasks. As such various types of implementations can support the source object.

The native datastructure for a source is a partitioned arrow dataset.
Partitions are streamed from one task to the next and persisted in a new dataset
between tasks. They can also be kept only in memory to reduce IO.

A source does not have to be an arrow dataset, but can also be a queue of any
python object in memory, or pickled and persisted.

## Telemetry and Logging

A trace_id is asigned a pipeline run and all task invokations in a run will
be correlated on the same trace_id. A span is created each time a task consumes
a data object. Task spans has a <span.id> and is corelated with <task.name> and
<pipeline.trace_id>. A log is kept for each task name and log entries will be
corelated on the <span.id>.

> Perhaps it makes sense to also corelate on the data object as well?
> So as to make it very easy to search and trace how a specific data object
> has moved through the stream? This could make sense for arrow data

## Connections in Pipeline or Config

The `secret` is a component for defining a namespace for a secret which contains
a password or otherwise access granting token which should not be stored in
plain text. How secrets are obtained is up to implemenetation, they could be
stored as environment variables or in an Azure Key Vault. However we need a way
to declare what secrets a configuration and pipeline needs in order to integrate
with various external systems.

```yaml
# SECRET is what CREDENTIALS is in BASE_migration.
# It is for refering and obtaining credentials
SECRET:
  vault_sb: rim_sb
  vault_prod: rim_prod
```

The `connection` component and section in a config / pipeline declaration, can be used to declare a connection which can be used for doing IO with other systems. If a secret is needed, and should be fetched at runtime to be used with the connection, a secret declared in the `SECRET` section can be reffered using the `!secret` node.

```yaml
# The connection section defines the registered external IO connections
# this pipeline should be able to use.
CONNECTION:
  elanco_rim_sb:
    uid: hello
    url: sandbox.veeva.com
    passwd: !secret vault_sb

  elanco_rim_prod:
    uid: hello
    url: sandbox.veeva.com
    passwd: !secret vault_prod
    validated: True  # Mark if a connection is to a validated environment.
                     # Any connection to such requires additional priviliges
                     # and should be requested at runtime.

```

## Full Pipeline Syntax Example

```yml
CONFIG: !parameter config  # point to config to be applied at runtime
                           # or specify directly here

# The parameter section defines any possible parameters
# an instance of this pipeline should be able to take
# when its `.run` method is invoked.
PIPELINE:
  - name: extract
    task: read
    source: !resource
    select: [col1, col2, col3]          # select only a subset of columns
    filter:                             # filter pushdown directly in pipeline
      - [
          [col1, isin, [gouda, danbo]]
          [col2, equals, cojita]
        ]    # With DNF syntax.


  - name: transform
    task: mapping
    source: extract
    rules: rules.yml

  - name: filter
    task: read
    source: mapping
    filter:
      - [col1, isin, ['olah', 'world']]

  - name: load
    task: doc_creator
    source: filter
    connection: elanco_rim_sb
    batch_size: 500

```

## Parameter

Task arguments can receive the input from pipeline parameters at runtime by usng the !parameter node. It has to referene an entry in the `PARAMETER` section in the pipeline.

```yaml
PARAMETER:
  mapping_rules:
    type: string
    default: mapping.yml

PIPELINE:
  - task: mapping
    name: mapping
    source: read
    rules: !parameter mapping_rules
```

A different variation of the syntax with a single dict layer. Everything else
related to the pipeline should exist in a config file somewhere else.
Keeping pipeline tasks as a dict ensures no duplicate task names.

```yml
PIPELINE:

  EXTRACT:
    task: parse
    source: s3://data.parquet
    select: [col1, col2, col3]
    filter:                               # filter pushdown directly in pipeline
        - [col1, isin, [gouda, danbo]]    # With DNF syntax.
        - [col2, equals, cojita]

  TRANSFORM:
    task: mapping
    source: EXTRACT                       # observe upstream tasks as source
    rules: !parameter rules

  VAULT_EXTRACT:
    task: field_extract
    object: document__v

  JOIN:
    task: join
    source: [TRANSFORM, VAULT_EXTRACT]
    how: inner
    left_on:
      - external_id__v
      - major_version_number__v
      - minor_version_number__v

  LOAD:
    task: doc_creator
    source: JOIN

```

## API Definition
All tasks must return a specific result type, which works with a corresponding
storage type. The default message type for serengeti tasks are:

### Source

Task receives

Since a task can be thought of a task which consumes results from a potential upstream task, but also has downtstream observes, it is both an observer as well as an observable.

A `Source` should have a read and a write, which knows how to read and write the specific data object in the source.

Task emit their result objects to storage. `DataBox`. When a task is sourced by another task, it is actually observing the `DataBox` of the upstream task. When the upstream task emits results, these are relayed to the `DataBox` with potential side effects. The downstream tasks observes the `DataBox` and receives new messages when they are available.

```python


@flow
def my_pipeline(flow_run_id: str):
  data = read('s3://data.parquet')
  mapped = mapping(data, 'rules.yml')
  documents = doc_creator(mapped)
  return documents

@flow
def my_pipeline(flow_run_id):
  return read('s3://data.parquet').mapping('rules.yml').doc_creator()

@flow
def my_pipeline(flow_run_id)
  data = read('s3://data.parquet', async=True)
  mapped = mapping(data, 'rules.yml', async=True)
  documents = doc_creator(mapped)

from serengeti import tasks


)
```

## Pipelines

Streams also have a pipe method so you can dot chain transformation pipelines directly on the streams.

```python
from serengeti import tasks
from serengeti import read, pipe

xs = read('s3://data.parquet').pipe(
  tasks.mapping('rules.yml'),
  tasks.vault.create_documents('config.yml'),
)

# OR

ys = pipe(
  tasks.read('data.parqut'),
  tasks.mapping('rules.yml'),
  tasks.vault.create_documents('config.yml'),
)
```
