# -*- coding: utf-8 -*-

import json
import os
import sys
import duckdb
import itertools
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from pathlib import Path
from queue import Empty
from neo4j import GraphDatabase
from dotenv import load_dotenv


load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")


def diagonals(partition_count: int) -> list[list[str]]:
    """Return parallel batches (cyclic diagonals) from a source-target partition matrix.
    
    The source and target node sets are disjoint, and each set is divided
    into ``partition_count`` partitions. Their combinations form a square
    matrix containing ``partition_count ** 2`` source-target partition pairs.

    The function divides this matrix into ``partition_count`` cyclic
    diagonals. Each cyclic diagonal represents a batch of partitions that can be
    processed in parallel. Within a batch, every source partition and every
    target partition appears exactly once, preventing concurrent tasks from
    accessing the same partition.

    Args:
        partition_count: Number of partitions in each node set.

    Returns:
        A list of parallel batches. Each batch represents a cyclic diagonal
        and contains partition pairs in ``"source - target"`` format.
    """
    return [
        [
            f"{(diagonal_index + target_partition) % partition_count}"
            f" - {target_partition}"
            for target_partition in range(partition_count)
        ]
        for diagonal_index in range(partition_count)
    ]


def relationship_batches(partition_count: int) -> list[list[str]]:
    """Return parallel batches for a non-disjoint partition matrix.

    The source and target nodes belong to the same node set, which is divided
    into ``partition_count`` partitions. Their combinations form a square
    matrix containing ``partition_count ** 2`` source-target partition pairs.

    Each batch contains pairs that do not share any partition. Therefore, all
    pairs in the same batch can be processed in parallel without concurrently
    accessing the same node partition.

    The batches are generated using a round-robin rotation. Forward and reverse
    pairs are placed in separate batches because they access the same
    partitions.

    Args:
        partition_count: Number of partitions in the node set.

    Returns:
        A list of parallel batches containing partition pairs in
        ``"source - target"`` format.
    """
    # Self-referencing pairs use distinct partitions and can therefore be
    # processed together.
    batches = [
        [
            f"{partition} - {partition}"
            for partition in range(partition_count)
        ]
    ]

    partitions = list(range(partition_count))

    # Round-robin pairing requires an even number of values. For an odd number
    # of partitions, None represents the partition that rests during a round.
    if partition_count % 2:
        partitions.append(None)

    # Keep one partition fixed while rotating all the others around it.
    # The fixed partition acts as an anchor and prevents the rotations from
    # generating the same pairs repeatedly.
    fixed_partition = partitions[0]
    rotating_partitions = partitions[1:]

    # Keeping one value fixed and rotating the remaining values generates
    # every possible unordered partition pair exactly once.
    for _ in range(len(partitions) - 1):
        current_partitions = [
            fixed_partition,
            # Unpack the rotating partitions to form the current round of pairs.
            *rotating_partitions
        ]

        # Pair values placed at opposite positions in the current round.
        # Each real partition can appear in at most one pair.
        pairs = [
            (current_partitions[index], current_partitions[-1 - index])
            for index in range(len(current_partitions) // 2)
            if current_partitions[index] is not None
            and current_partitions[-1 - index] is not None
        ]

        # Forward pairs do not share any physical partition and can therefore
        # be processed in parallel.
        batches.append([
            f"{source_partition} - {target_partition}"
            for source_partition, target_partition in pairs
        ])

        # Reverse pairs must be placed in a separate batch because they use
        # the same physical partitions as their corresponding forward pairs.
        batches.append([
            f"{target_partition} - {source_partition}"
            for source_partition, target_partition in pairs
        ])

        # Rotate every partition except the fixed anchor. The last rotating
        # partition moves to the front for the next round.
        rotating_partitions = [
            rotating_partitions[-1],
            *rotating_partitions[:-1],
        ]

    return batches


def init_worker(progress_queue):
    """Initialize a worker process with the shared progress queue.

    This function is called once when each worker process starts. It stores
    the shared queue in a process-level global variable so that worker tasks
    can send progress events to the main process without receiving the queue
    as an argument for every task.

    Args:
        progress_queue: Multiprocessing queue used to send progress events
            from the worker processes to the main process.
    """
    global PROGRESS_QUEUE
    PROGRESS_QUEUE = progress_queue


def prepare_nodes(
    csv_file: str,
    duckdb_file: str,
    table: str,
    partition_count: int,
    primary_key: str
) -> None:
    """Load and partition a node CSV file into a DuckDB table.

    The primary key is converted to a string, and each node is assigned to an
    export partition by hashing its primary key. The number of partitions is
    determined by ``partition_count``.

    The table is ordered by ``export_part`` so that rows belonging to the same
    partition are stored close together. This ordering allows DuckDB to skip
    unrelated row groups more efficiently when workers filter the table by
    partition, reducing the amount of data scanned during parallel imports.

    Args:
        csv_file: Path to the source CSV file.
        duckdb_file: Path to the DuckDB database file.
        table: Name of the DuckDB table to create or replace.
        partition_count: Number of export partitions to generate.
        primary_key: Name of the node primary-key column.
    """
    with duckdb.connect(duckdb_file) as connection:
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {table} AS
            WITH csv AS (
                SELECT * REPLACE(
                    CAST({primary_key} AS VARCHAR) AS {primary_key}
                )
                FROM read_csv_auto(?, header=true)
            )
            SELECT *,
                CAST(
                    ABS(HASH({primary_key})) % {partition_count}
                    AS VARCHAR
                ) AS export_part
            FROM csv

            -- Group rows from the same partition together so DuckDB can
            -- reduce the amount of data scanned by partition queries.
            ORDER BY export_part
            """,
            [csv_file],
        )


def prepare_relationships(
    csv_file: str,
    duckdb_file: str,
    table: str,
    partition_count: int,
    source_primary_key: str,
    target_primary_key: str
) -> None:
    """Load and partition a relationship CSV file into a DuckDB table.

    The source and target primary keys are converted to strings. Each endpoint
    is assigned to a partition by hashing its primary key.

    The two partition numbers are combined into an ``export_part`` value using
    the ``"source - target"`` format. This creates a square partition matrix
    containing up to ``partition_count ** 2`` source-target combinations.

    The table is ordered by ``export_part`` so that relationships belonging to
    the same matrix cell are stored close together. This ordering allows DuckDB
    to skip unrelated row groups more efficiently when workers filter the table
    by partition, reducing the amount of data scanned during parallel imports.

    Args:
        csv_file: Path to the source CSV file.
        duckdb_file: Path to the DuckDB database file.
        table: Name of the DuckDB table to create or replace.
        partition_count: Number of partitions for each node set.
        source_primary_key: Name of the relationship source-key column.
        target_primary_key: Name of the relationship target-key column.
    """
    with duckdb.connect(duckdb_file) as connection:
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {table} AS
            WITH csv AS (
                SELECT * REPLACE(
                    CAST({source_primary_key} AS VARCHAR)
                        AS {source_primary_key},
                    CAST({target_primary_key} AS VARCHAR)
                        AS {target_primary_key}
                )
                FROM read_csv_auto(?, header=true)
            )
            SELECT *,
                CAST(
                    ABS(HASH({source_primary_key})) % {partition_count}
                    AS VARCHAR
                )
                || ' - ' ||
                CAST(
                    ABS(HASH({target_primary_key})) % {partition_count}
                    AS VARCHAR
                ) AS export_part
            FROM csv

            -- Group relationships from the same matrix cell together so
            -- DuckDB can reduce the data scanned by partition queries.
            ORDER BY export_part
            """,
            [csv_file],
        )


def partition_rows_count(duckdb_file: str, table: str) -> dict[str, int]:
    """Count the number of rows in each export partition.

    The DuckDB database is opened in read-only mode because this function only
    retrieves partition statistics. The resulting counts are used to calculate
    the global import progress and the progress of each individual partition.

    Args:
        duckdb_file: Path to the DuckDB database file.
        table: Name of the table containing the ``export_part`` column.

    Returns:
        A dictionary mapping each export partition identifier to its number
        of rows.
    """
    with duckdb.connect(duckdb_file, read_only=True) as connection:
        rows = connection.execute(
            f"""
            SELECT export_part, COUNT(*)
            FROM {table}
            GROUP BY export_part
            """
        ).fetchall()

    return {
        str(partition): int(count)
        for partition, count in rows
    }


def open_driver():
    """Create and validate a Neo4j driver.

    The connection settings are read from the global Neo4j environment
    variables. The driver automatically retries transient transaction failures
    for up to 30 seconds.

    Connectivity is verified immediately so that connection or authentication
    problems are detected before the import begins.

    Returns:
        A connected and verified Neo4j driver.
    """
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
        max_transaction_retry_time=30,
    )

    driver.verify_connectivity()

    return driver


def read_dataset(connection, query, parameters):
    """Read query results from DuckDB as a stream of dictionaries.

    Rows are fetched in groups of 10,000 to avoid loading the complete dataset
    into memory. Column names are converted to lowercase so they can be
    referenced consistently in Cypher queries.

    Each returned row is represented as a dictionary mapping column names to
    their corresponding values.

    Args:
        connection: Open DuckDB connection.
        query: SQL query to execute.
        parameters: Parameters passed to the SQL query.

    Yields:
        One dictionary for each row returned by the query.
    """
    cursor = connection.execute(query, parameters)

    columns = [
        column[0].lower()
        for column in cursor.description
    ]

    while True:
        # Limit memory usage by reading only 10,000 rows at a time.
        rows = cursor.fetchmany(10_000)

        if not rows:
            break

        for row in rows:
            yield dict(zip(columns, row))


def make_batches(iterator, batch_size):
    """Group records from an iterator into fixed-size batches.

    Records are consumed lazily, so the complete dataset does not need to be
    stored in memory. The final batch may contain fewer records than
    ``batch_size``.

    Args:
        iterator: Iterator providing the records to group.
        batch_size: Maximum number of records in each batch.

    Yields:
        Lists containing at most ``batch_size`` records.
    """
    while True:
        batch = list(itertools.islice(iterator, batch_size))

        if not batch:
            break

        yield batch


def write_batch(tx, query, batch):
    """Execute a Cypher query for one batch of records.

    The batch is passed to Neo4j through the ``rows`` query parameter. The
    result is consumed immediately to ensure that the transaction has completed
    and to return its execution summary.

    Args:
        tx: Active Neo4j transaction.
        query: Cypher query to execute.
        batch: Records to pass through the ``rows`` query parameter.

    Returns:
        The Neo4j result summary for the executed transaction.
    """
    result = tx.run(query, rows=batch)

    return result.consume()


def print_global_progress(
    import_name,
    event,
    current,
    total_rows,
    begin
):
    """Print the aggregated progress of the current import.

    This function is called by the main process after receiving a progress
    event from a worker. It calculates the global processing rate, estimated
    remaining time, global completion percentage, and progress of the
    partition associated with the latest event.

    Args:
        import_name: Name used to identify the current import.
        event: Progress information sent by a worker. It must contain the
            partition identifier, partition row counts, and batch timings.
        current: Total number of rows processed by all workers.
        total_rows: Total number of rows to import.
        begin: Timestamp recorded when the import started.
    """
    # Calculate the global processing rate since the import started.
    elapsed = time.time() - begin
    rows_per_second = current / elapsed if elapsed else 0

    # Estimate the remaining duration from the global processing rate.
    eta = (
        (total_rows - current) / rows_per_second
        if rows_per_second
        else 0
    )

    partition_total = event["partition_total"]
    partition_current = event["partition_current"]

    partition_percent = (
        partition_current / partition_total * 100
        if partition_total
        else 100
    )

    global_percent = (
        current / total_rows * 100
        if total_rows
        else 100
    )

    # flush=True makes the progress immediately visible in terminals and
    # container logs.
    print(
        f"### {import_name}: {current}/{total_rows} - "
        f"{global_percent:.2f}% - "
        f"debit: {rows_per_second:.0f} l/s - "
        f"ETA: {eta / 60:.0f} min | "
        f"partition {event['partition']}: "
        f"{partition_current}/{partition_total} "
        f"({partition_percent:.2f}%) - "
        f"load: {event['load_time']:.2f}s - "
        f"ingestion: {event['ingestion_time']:.2f}s ###",
        flush=True,
    )


def load_partition(
    cypher_query: str,
    batch_size: int,
    duckdb_file: str,
    table: str,
    partition: str,
    partition_rows: dict[str, int]
) -> int:
    """Load one DuckDB partition into Neo4j.

    This function is executed inside a worker process. It opens its own
    read-only DuckDB connection and Neo4j driver, selects the rows belonging
    to the requested partition, and sends them to Neo4j in batches.

    After each batch, the worker sends a progress event through the global
    ``PROGRESS_QUEUE``. The main process uses these events to calculate and
    display the aggregated import progress.

    Args:
        cypher_query: Cypher query used to import one batch into Neo4j.
        batch_size: Maximum number of records in each Neo4j transaction.
        duckdb_file: Path to the DuckDB database file.
        table: Name of the DuckDB table containing the records.
        partition: Identifier of the export partition to load.
        partition_rows: Mapping between partition identifiers and their total
            number of rows.

    Returns:
        Total number of rows imported from the partition.
    """
    # Each worker uses its own DuckDB connection and Neo4j driver because these
    # objects must not be shared between processes.
    connection = duckdb.connect(duckdb_file, read_only=True)
    driver = open_driver()

    total = 0

    try:
        # Read only the requested partition and exclude the technical column
        # that must not be imported into Neo4j.
        query = f"""
            SELECT * EXCLUDE(export_part)
            FROM {table}
            WHERE export_part = ?
        """

        rows = read_dataset(connection, query, [partition])
        start_reload_batch = time.time()

        for batch in make_batches(rows, batch_size):
            # Measure the time required to retrieve and prepare the next batch.
            end_reload_batch = time.time()
            start_neo4j = time.time()

            # Open a Neo4j session for the batch and execute the write
            # transaction with automatic retry support.
            with driver.session(
                database=NEO4J_DATABASE,
            ) as neo4j_session:
                neo4j_session.execute_write(
                    write_batch,
                    cypher_query,
                    batch,
                )

            end_neo4j = time.time()
            total += len(batch)

            # Send the worker progress to the main process. Only the main
            # process is responsible for displaying the global progress.
            PROGRESS_QUEUE.put({
                "partition": partition,
                "rows": len(batch),
                "partition_current": total,
                "partition_total": partition_rows[partition],
                "load_time": end_reload_batch - start_reload_batch,
                "ingestion_time": end_neo4j - start_neo4j,
            })

            # Start measuring the loading time of the next batch.
            start_reload_batch = time.time()

    finally:
        # Always release process-specific resources when the partition import
        # finishes or when an exception occurs.
        connection.close()
        driver.close()

    return total


def export_import(
    import_name: str,
    duckdb_file: str,
    table: str,
    cypher_query: str,
    batch_size: int,
    max_workers: int,
    partition_batches: list[list[str]]
) -> int:
    """Import all scheduled DuckDB partitions into Neo4j.

    Each inner list in ``partition_batches`` represents a batch of independent
    partitions that can be processed in parallel. The batches themselves are
    processed sequentially to prevent concurrent workers from accessing
    conflicting node partitions.

    Worker processes send progress events through a shared multiprocessing
    queue. The main process consumes these events, aggregates the processed
    row counts, and displays the global import progress.

    Args:
        import_name: Name used to identify the import in progress messages.
        duckdb_file: Path to the DuckDB database file.
        table: Name of the DuckDB table containing the records.
        cypher_query: Cypher query used to import each record batch.
        batch_size: Maximum number of records in each Neo4j transaction.
        max_workers: Maximum number of worker processes.
        partition_batches: Ordered list of partition batches. Partitions within
            the same batch can be processed in parallel.

    Returns:
        Total number of rows imported into Neo4j.
    """
    partition_rows = partition_rows_count(duckdb_file, table)

    begin = time.time()
    total_rows = sum(partition_rows.values())
    current = 0
    total = 0

    with Manager() as manager:
        progress_queue = manager.Queue()

        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=init_worker,
            initargs=(progress_queue,),
        ) as pool:
            # Process batches sequentially and the partitions of each batch
            # concurrently.
            for batch_index, partitions in enumerate(
                partition_batches,
                1,
            ):
                pending_futures = {
                    pool.submit(
                        load_partition,
                        cypher_query,
                        batch_size,
                        duckdb_file,
                        table,
                        partition,
                        partition_rows,
                    )
                    for partition in partitions
                }

                batch_total = 0
                batch_reported = 0

                while pending_futures:
                    try:
                        event = progress_queue.get(timeout=0.2)
                    except Empty:
                        pass
                    else:
                        current += event["rows"]
                        batch_reported += event["rows"]

                        print_global_progress(
                            import_name,
                            event,
                            current,
                            total_rows,
                            begin,
                        )

                    completed_futures = {
                        future
                        for future in pending_futures
                        if future.done()
                    }

                    for future in completed_futures:
                        batch_total += future.result()

                    pending_futures -= completed_futures

                # Consume progress events that were queued immediately before
                # the workers completed.
                while batch_reported < batch_total:
                    event = progress_queue.get()

                    current += event["rows"]
                    batch_reported += event["rows"]

                    print_global_progress(
                        import_name,
                        event,
                        current,
                        total_rows,
                        begin,
                    )

                total += batch_total

                print(
                    f"@@@ {import_name} / batch "
                    f"{batch_index}/{len(partition_batches)} complete @@@",
                    flush=True,
                )

    return total


def resolve_path(
    base_directory: str | Path,
    path: str | Path
) -> str:
    """Resolve a file path relative to a base directory.

    Absolute paths are returned unchanged. Relative paths are combined with
    ``base_directory``, which is typically the directory containing the
    configuration file.

    Args:
        base_directory: Directory used to resolve relative paths.
        path: Absolute or relative file path.

    Returns:
        The resulting path as a string.
    """
    base_directory = Path(base_directory)
    input_path = Path(path)

    # An absolute path does not depend on the configuration directory.
    if input_path.is_absolute():
        return str(input_path)

    # Resolve relative paths from the configuration file directory.
    return str(base_directory / input_path)


def run_import(
    import_config: dict,
    defaults: dict,
    base_directory: str | Path
) -> None:
    """Prepare and execute one configured Neo4j import.

    The function reads the import-specific and default settings, resolves the
    CSV and DuckDB paths, prepares the DuckDB table, and determines the
    partition batches that can be processed in parallel.

    Node partitions are independent and can all be processed in one parallel
    batch. Relationship partitions are scheduled differently depending on
    whether the source and target node sets are disjoint.

    Args:
        import_config: Configuration of the node or relationship import.
        defaults: Shared settings such as CPU count, batch size, and DuckDB
            file path.
        base_directory: Directory used to resolve relative file paths.
    """
    import_name = import_config["name"]
    import_type = import_config["type"]

    cpu_count = defaults["cpu_count"]
    batch_size = defaults["batch_size"]

    # Use one data partition and one worker process per configured CPU.
    partition_count = cpu_count
    max_workers = cpu_count

    # Resolve paths relative to the configuration file directory.
    csv_file = resolve_path(
        base_directory,
        import_config["csv_file"],
    )
    duckdb_file = resolve_path(
        base_directory,
        defaults["duckdb_file"],
    )

    table = import_config["table"]
    cypher_query = import_config["cypher_query"]

    print(
        f"=== Start import: {import_name} ===",
        flush=True,
    )

    if import_type == "nodes":
        prepare_nodes(
            csv_file,
            duckdb_file,
            table,
            partition_count,
            import_config["primary_key"],
        )

        # Node partitions do not share relationships during their creation,
        # so all partitions can be processed in parallel in a single batch.
        partition_batches = [
            [
                str(partition)
                for partition in range(partition_count)
            ]
        ]

    else:
        prepare_relationships(
            csv_file,
            duckdb_file,
            table,
            partition_count,
            import_config["source_primary_key"],
            import_config["target_primary_key"],
        )

        if import_config["nodes_disjoint"]:
            # With disjoint source and target node sets, cyclic diagonals
            # provide independent source-target partition combinations.
            partition_batches = diagonals(partition_count)
        else:
            # When source and target nodes belong to the same set, use
            # round-robin batches so no physical partition appears twice
            # within the same parallel batch.
            partition_batches = relationship_batches(partition_count)

    total = export_import(
        import_name,
        duckdb_file,
        table,
        cypher_query,
        batch_size,
        max_workers,
        partition_batches,
    )

    print(
        f"=== Import complete: {import_name} / {total} lines ===",
        flush=True,
    )


if __name__ == "__main__":
    config_file = Path(sys.argv[1]).resolve()

    with open(config_file, encoding="utf-8") as file:
        config = json.load(file)

    defaults = config["defaults"]

    for import_config in config["imports"]:
        run_import(import_config, defaults, config_file.parent)
