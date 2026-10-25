"""Create the `f1_analysis` keyspace and all tables in Cassandra (idempotent, never drops data).

    python create_schema.py
"""
import logging
import sys

import cassandra_db as cdb
from config import get_cassandra_settings


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_cassandra_settings()
    try:
        cluster, session = cdb.connect(settings, use_keyspace=False)
    except cdb.CassandraError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    try:
        count = cdb.apply_schema(session)
    except cdb.CassandraError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        cluster.shutdown()
    print(f"OK: executed {count} statements from cql/schema.cql (keyspace `{settings.keyspace}`).")
    if settings.keyspace != "f1_analysis":
        print("NOTE: cql/schema.cql creates the keyspace `f1_analysis`; CASSANDRA_KEYSPACE is set to "
              f"`{settings.keyspace}`, which will not exist unless you also edit the schema file.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
