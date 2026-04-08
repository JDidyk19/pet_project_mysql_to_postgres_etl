from enum import Enum

# Mapping MySQL column types to PostgreSQL types
MYSQL_TO_POSTGRES_TYPE_MAP: dict[str, str] = {
    # Numeric types
    "tinyint": "SMALLINT",
    "tinyint(1)": "BOOLEAN",
    "smallint": "SMALLINT",
    "mediumint": "INTEGER",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "float": "REAL",
    "double": "DOUBLE PRECISION",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",

    # String types
    "char": "CHAR",
    "varchar": "VARCHAR",
    "tinytext": "TEXT",
    "text": "TEXT",
    "mediumtext": "TEXT",
    "longtext": "TEXT",

    # Binary types
    "binary": "BYTEA",
    "varbinary": "BYTEA",
    "tinyblob": "BYTEA",
    "blob": "BYTEA",
    "mediumblob": "BYTEA",
    "longblob": "BYTEA",

    # Date/Time types
    "date": "DATE",
    "time": "TIME",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMPTZ",
    "year": "SMALLINT",

    # Other types
    "bit": "BIT",
    "bit(1)": "BOOLEAN",
    "enum": "VARCHAR(255)",
    "set": "TEXT",
    "json": "JSONB",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
}


class LoadStrategy(str, Enum):
    """
    Load strategies for data pipelines.

    FULL_REFRESH: overwrite the entire target table.
    APPEND: insert new records only, no updates.
    UPSERT: insert new records and update existing ones based on primary key.
    """
    
    FULL_REFRESH = "full_refresh"
    APPEND = "append"
    UPSERT = "upsert"


class ExtractStrategy(str, Enum):
    """
    Extraction strategies for source data.

    FULL: extract all rows from the source.
    INCREMENTAL: extract only new or changed rows.
    """
    
    FULL = "full"
    INCREMENTAL = "incremental"
    