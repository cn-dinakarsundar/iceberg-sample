import os
os.environ["PYICEBERG_CATALOG__DEFAULT__URI"] = "sqlite:///iceberg_catalog.db"
os.environ["PYICEBERG_CATALOG__DEFAULT__WAREHOUSE"] = "/tmp/warehouse"
os.environ["PYICEBERG_CATALOG__DEFAULT__TYPE"] = "sql"

from pyiceberg.catalog import load_catalog

import pyarrow as pa

catalog = load_catalog("default")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType

schema = Schema(
    NestedField(1, "id", IntegerType(), required=False),
    NestedField(2, "name", StringType(), required=False),
)

catalog.create_table(identifier=("default", "employees"), schema=schema)

# Now list tables
print("Tables in 'default':", catalog.list_tables("default"))

# Create an Arrow table with sample data
data = pa.Table.from_pydict(
    {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
)

data = pa.table({
    "id": pa.array([1, 2, 3], type=pa.int32(), mask=[False, False, False]),  # no nulls, required
    "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.string()),
})

# Append the data to the Iceberg table
table = catalog.load_table("default.employees")

table.append(data)