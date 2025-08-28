import os
os.environ["PYICEBERG_CATALOG__DEFAULT__URI"] = "sqlite:///iceberg_catalog.db"
os.environ["PYICEBERG_CATALOG__DEFAULT__WAREHOUSE"] = "/tmp/warehouse"
os.environ["PYICEBERG_CATALOG__DEFAULT__TYPE"] = "sql"

from pyiceberg.catalog import load_catalog


catalog = load_catalog("default")

#catalog.create_namespace("default")


# List all namespaces first
print("Namespaces:", catalog.list_namespaces())

# Pick a namespace (e.g. "default")
tables = catalog.list_tables("default")
print("Tables in 'default':", tables)


from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType

schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
)

catalog.create_table(identifier=("default", "users"), schema=schema)

# Now list tables
print("Tables in 'default':", catalog.list_tables("default"))