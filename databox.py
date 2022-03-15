import os
import ray
import uuid
import pyarrow as pa
import polars as pl
from typing import Generator


class RayBox:
    """Represents an appendable box of ray datasets"""
    def __init__(self):
        self.groups = {}

    def append(self, table: pl.DataFrame):
        pa_table = table.to_arrow()
        uid = table.hash_rows()
        if not self.groups.get(uid):
            dataset = ray.data.from_arrow(pa_table)
            self.groups[uid] = dataset


class ArrowBox:
    """Represents an appendable box of arrow data references"""
    def __init__(self, storage='storage/serengeti', schema=None):
        self.storage = storage
        self.schema = schema
        if not os.path.exists(storage):
            os.makedirs(storage)

    def blocks(self) -> Generator:
        for filepath in os.listdir(self.storage):
            if filepath.split('.')[-1] == 'arrow':
                yield os.path.join(self.storage, filepath)

    def append(self, table: pa.Table, uid=None):
        uid = uid or uuid.uuid4().hex
        path = os.path.join(self.storage, f'{uid}.arrow')
        writer = pa.ipc.new_file(path, table.schema)
        writer.write(table)
        writer.close()
        return path

    @staticmethod
    def _read_block(block) -> pa.Table:
        mmap = pa.memory_map(block)
        return pa.RecordBatchFileReader(mmap).read_all()

    def _merge(tables) -> pa.Table:
        columns = []
        fields = []
        for table in tables:
            for name, column in zip(
                table.column_names,
                table.itercolumns(),
            ):
                columns.append(column)
                fields.append(
                    pa.field(name, column.type)
                )
        table = pa.Table.from_arrays(
            columns, schema=pa.schema(fields)
        )
        return table

    def read(self, same_schema=True) -> pa.Table:
        tables = map(self._read_block, self.blocks())
        if not same_schema:
            return self.merge(tables)
        return pa.concat_tables(tables)
