# Neo4j Import Parallel

A Python tool for bulk importing large datasets into Neo4j graph databases.

## Why this project

Importing nodes and relationships in parallel can cause lock contention in Neo4j when multiple workers access the same nodes. This tool efficiently schedules partitions into conflict-free batches, so concurrent workers never share a partition.

Read the Medium articles:
- [Massive Parallel Imports in Neo4j Without Deadlock and Lock Contention](https://medium.com/neo4j/massive-parallel-imports-in-neo4j-without-deadlock-and-lock-contention-2c003a48d49a)
- [Implementation of the Neo4j Import Without Deadlock and Lock Contention](https://medium.com/neo4j/implementation-of-the-neo4j-import-without-deadlock-and-lock-contention-1755862e6243)

This approach can be replicated with any Neo4j drivers and and not necessarily to create nodes/relationships (e.g delete nodes/relationships).

## Important point

The approach presented in this demo requires `block` format storage, available only with Neo4j Enterprise Edition.
Block format was introduced with Neo4j 5.14 and became the default storage format with Neo4j 5.22.

## Launch the demo

This Docker Compose setup deploys Neo4j Enterprise 2026.09.0 (evaluation license) with the APOC and Graph Data Science plugins, alongside a custom Python application connected to Neo4j. The Neo4j container is limited to 8 GB of RAM, including a 4 GB heap and a 3 GB page cache. The Python version and installed libraries are defined in the Dockerfile and its dependency file `requirements.txt`.

By default, the import is configured to use 6 CPUs. You can update the `cpu_count` parameter [here](app/import_graph_config.json).

1. Start the containers:
```shell
docker compose -f docker-compose.yml up -d
```

2. Connect via the [Neo4j Browser](http://localhost:7474/browser/) with the credentials:
- Database user: neo4j
- Password: password

3. Check the `neo4j` database storage format:
```cypher
// neo4j database must have the value: "block-block-1.1"
SHOW DATABASES YIELD * RETURN name, store;
```

4. Create the unique constraints:
```cypher
// Create the constraints
CREATE CONSTRAINT person_id FOR (n:Person) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT contract_id FOR (n:Contract) REQUIRE n.id IS UNIQUE;
CREATE CONSTRAINT operation_id FOR (n:Operation) REQUIRE n.id IS UNIQUE;
```

5. Start the parallel import:
```shell
docker compose run --remove-orphans python-service python import_graph.py import_graph_config.json
```

## Turn-off resources

Stop the container:
```shell
docker compose -f docker-compose.yml down -v --remove-orphans
```