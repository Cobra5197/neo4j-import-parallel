# Import Graph Configuration

This JSON file configures the Neo4j graph import process.

## Defaults

- `batch_size`: Number of rows processed per batch.
- `cpu_count`: Number of CPU cores available for parallel processing.
- `duckdb_file`: DuckDB database file used for staging and import management.

## Imports

Each entry in `imports` defines a CSV import job.

### Node Imports

The configuration imports three node types:

- `Person` from `data/Person.csv`
- `Contract` from `data/Contract.csv`
- `Operation` from `data/Operation.csv`

Each node uses `id` as its primary key and is merged into Neo4j using its corresponding label.

### Relationship Imports

The configuration imports two relationship types:

- `IS_LINKED_TO`: Connects `Person` nodes to other `Person` nodes.
- `HAS_OPERATION`: Connects `Contract` nodes to `Operation` nodes.

Relationship imports define source and target keys, identify whether the node sets are disjoint, and use Cypher queries to create relationships only when they do not already exist.

## Configuration Fields

- `name`: Name of the import job.
- `type`: Import category, either `nodes` or `relationships`.
- `csv_file`: Source CSV file.
- `table`: DuckDB staging table name.
- `primary_key`: Primary key for node imports.
- `source_primary_key`: Source node key for relationship imports.
- `target_primary_key`: Target node key for relationship imports.
- `nodes_disjoint`: Indicates whether source and target node sets are distinct.
- `cypher_query`: Cypher statement used to write staged rows to Neo4j.