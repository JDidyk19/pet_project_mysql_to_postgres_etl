import uuid
from datetime import date, datetime

from airflow.operators.python import get_current_context
from helpers.checks import col_is_uuid, is_decimal
from helpers.constants import MYSQL_TO_POSTGRES_TYPE_MAP


def transform_mysql_to_postgres(rows: tuple[tuple[str]]) -> list[str]:
    """
    Transform MySQL table column definitions to PostgreSQL-compatible definitions.

    Args:
        rows: Tuple of tuples representing MySQL columns:
              (column_name, column_type, is_nullable, column_key)

    Returns:
        List of PostgreSQL column definition strings.
        Also pushes PRIMARY KEY column name to Airflow XCom.
    """
    
    columns = list()
    context = get_current_context()
    
    for row in rows:
        column_name, column_type, is_nullable, column_key = row
        column_name, pg_type, size, pg_nullable, pg_column_key = transform_type(column_name, column_type, is_nullable, column_key)
        
        new_column = f"{column_name} {pg_type}{size} {pg_nullable} {pg_column_key}"
        columns.append(new_column.strip())

        if pg_column_key == 'PRIMARY KEY':
            context['ti'].xcom_push(key='primary_key_column', value=column_name)
            
    return columns
        

def transform_type(column_name: str, column_type: str, is_nullable: str, column_key: str) -> tuple[any]:
    """
    Transform a single MySQL column type to PostgreSQL type and properties.

    Args:
        column_name: Name of the column.
        column_type: MySQL column type (e.g., 'varchar(100)').
        is_nullable: 'YES' or 'NO'.
        column_key: Column key (e.g., 'PRI').

    Returns:
        Tuple of:
            - column_name
            - PostgreSQL type
            - size string (e.g., '(100)')
            - nullable string ('NOT NULL' or '')
            - column key string ('PRIMARY KEY' or '')
    """
    
    column_type = column_type.lower().strip().split()[0]
    pg_type = MYSQL_TO_POSTGRES_TYPE_MAP.get(column_type, None)
    size = None
    
    # extract size if present in type definition (e.g., varchar(100))
    if pg_type is None:
        base_type = column_type.split('(')[0]
        if '(' in column_type and ')' in column_type:
            open_idx = column_type.index('(') + 1
            close_idx = column_type.index(')')
            size = column_type[open_idx: close_idx]
            
        pg_type = MYSQL_TO_POSTGRES_TYPE_MAP.get(base_type, None)
        
        # if mysql type is as UUID
        if col_is_uuid(f'{base_type} {size}'):
            pg_type = 'UUID'
            size = None
        
        # if mysql type is still missing
        if pg_type is None:
            pg_type = 'TEXT'
    
    pg_nullable = '' if is_nullable == 'YES' else 'NOT NULL'
    pg_column_key = 'PRIMARY KEY' if column_key == 'PRI' else ''
    pg_size = f'({size})' if size is not None and is_decimal(size) else ''
    
    return column_name, pg_type, pg_size, pg_nullable, pg_column_key


def transform_row(row: dict[str, any], column_types: dict[str, str]) -> dict[str, any]:
    """
    Transform a single data row from MySQL types to Python/PostgreSQL types.

    Args:
        row: Dict of column_name -> value
        column_types: Dict of column_name -> MySQL type string

    Returns:
        Transformed row as dict
    """
    
    return {
        col: transform_value(value, column_types.get(col))
        for col, value in row.items()
    }


def transform_value(value: any, mysql_type: str | None) -> any:
    """
    Transform a single value based on its MySQL type.

    Args:
        value: Original value
        mysql_type: MySQL type string

    Returns:
        Transformed value suitable for PostgreSQL
    """
    
    if value is None:
        return None
    
    mysql_type = (mysql_type or '').lower()
    
    if mysql_type in ('tinyint(1)', 'bit(1)'):
        return bool(value)
    
    if mysql_type in ('char(36)', 'binary(36)'):
        try:
            return str(uuid.UUID(value))
        except (ValueError, TypeError):
            return None
    
    if mysql_type == "year":
        return int(value) if value else None
    
    if mysql_type.startswith('datetime') or mysql_type.startswith('timestamp'):
        return _transform_datetime(value)

    if mysql_type.startswith('date'):
        return _transform_date(value)
    
    return value


def _transform_datetime(value: any) -> datetime | None:
    """
    Safely convert value to datetime object.
    Accepts string in ISO format or datetime object.
    """
    
    if value is None:
        return None

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace(" ", "T"))
        except ValueError:
            return None

    if isinstance(value, datetime):
        return value

    return None


def _transform_date(value: any) -> date | None:
    """
    Safely convert value to date object.
    Accepts string in ISO format or date object.
    """
    
    if value is None:
        return None

    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    return None