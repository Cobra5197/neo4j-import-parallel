// CSV files must be copied to the import directory of the Neo4j database before running this script.
// This script loads CSV files into a Neo4j database using concurrent transactions and disjoint processing to improve performance and avoid deadlocks.
// These Cypher queries are equivalent to those used in import_graph_config.json.

// Create Person nodes
CYPHER 25
LOAD CSV WITH HEADERS FROM 'file:///Person.csv' AS row
CALL (row) {
    MERGE (n:Person {id: row.id}) SET n += row
} IN 6 CONCURRENT TRANSACTIONS OF 5000 ROWS DISJOINT BY (row.id);

// Create Contract nodes
CYPHER 25
LOAD CSV WITH HEADERS FROM 'file:///Contract.csv' AS row
CALL (row) {
    MERGE (n:Contract {id: row.id}) SET n += row
} IN 6 CONCURRENT TRANSACTIONS OF 5000 ROWS DISJOINT BY (row.id);

// Create Operation nodes
CYPHER 25
LOAD CSV WITH HEADERS FROM 'file:///Operation.csv' AS row
CALL (row) {
    MERGE (n:Operation {id: row.id}) SET n += row
} IN 6 CONCURRENT TRANSACTIONS OF 5000 ROWS DISJOINT BY (row.id);

// Create IS_LINKED_TO relationships
CYPHER 25
LOAD CSV WITH HEADERS FROM 'file:///IS_LINKED_TO.csv' AS row
CALL (row) {
    MATCH (src:Person {id: row.id_source})
    MATCH (tgt:Person {id: row.id_target})
    OPTIONAL MATCH (src)-[r:IS_LINKED_TO]->(tgt)
    CALL (src, tgt, r) {
        WHEN r IS NULL THEN {
            CREATE (src)-[nr:IS_LINKED_TO]->(tgt)
            RETURN nr
        } ELSE {
            RETURN r AS nr
        }
    }
    FINISH
} IN 6 CONCURRENT TRANSACTIONS OF 5000 ROWS DISJOINT BY (row.id_source, row.id_target);

// Create HAS_OPERATION relationships
CYPHER 25
LOAD CSV WITH HEADERS FROM 'file:///HAS_OPERATION.csv' AS row
CALL (row) {
    MATCH (src:Contract {id: row.id_source})
    MATCH (tgt:Operation {id: row.id_target})
    OPTIONAL MATCH (src)-[r:HAS_OPERATION]->(tgt)
    CALL (src, tgt, r) {
        WHEN r IS NULL THEN {
            CREATE (src)-[nr:HAS_OPERATION]->(tgt)
            RETURN nr
        } ELSE {
            RETURN r AS nr
        }
    }
    FINISH
} IN 6 CONCURRENT TRANSACTIONS OF 5000 ROWS DISJOINT BY (row.id_source, row.id_target);