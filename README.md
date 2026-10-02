# Neo4j Import Parallel

## Important point

The approach presented in this demo requires `block` format storage, available only with Neo4j Enterprise Edition.
Block format was introduced with Neo4j 5.14 and became the default storage format with Neo4j 5.22.

## Launch the demo

1. Start the containers:
```shell
docker compose -f docker-compose.yml up -d
```

2. Connect via the [Neo4j Browser](http://localhost:7474/browser/) with the credentials:
- Database user: neo4j
- Password: passwod

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

4. Start the parallel import:
```shell
docker compose run --remove-orphans python-service python import_graph.py import_graph_config.json
```

## Turn-off ressouces

Stop the container:
```shell
docker compose -f docker-compose.yml down -v --remove-orphans
```