import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.constants import ExtractStrategy, LoadStrategy
from helpers.transformation import transform_mysql_to_postgres, transform_row

logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='mysql_to_postgres_etl',
    default_args=DEFAULT_ARGS,
    description='ETL process: Migrate data from MySQL to Postgres',
    schedule=None,
    start_date=None,
    catchup=False,
    tags=['etl', 'mysql', 'postgres'],
    params={
        'table_name': 'users',
        'batch_size': 2000,
        'extract_strategy': ExtractStrategy.FULL,
        'load_strategy': LoadStrategy.FULL_REFRESH
    }
) as dag:
    """ETL DAG to migrate data from a MySQL table to PostgreSQL."""
    
    @task
    def get_source_table(params: dict[str, any]) -> dict[str, any]:
        """
        Extract metadata and row count from the source MySQL table.

        Args:
            params: Dictionary with DAG parameters

        Returns:
            Dictionary containing:
                - 'columns': list of column definitions from information_schema
                - 'row_count': total number of rows in the table
        """
        
        table_name = params.get('table_name')
        
        if not table_name:
            logger.error("No table_name provided in DAG params.")
            raise ValueError("table_name is required in params")
        
        logger.info(f"Connecting to MySQL and fetching schema for table: {table_name}")
        
        mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')

        # Fetch column metadata
        query = """
            SELECT
                column_name,
                column_type,
                is_nullable,
                column_key
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
            AND table_name = %s;
        """
        columns = mysql_hook.get_records(query, parameters=[table_name])
        column_count = len(columns)
        logger.info(f"Fetched {column_count} columns for table {table_name}")

        # Fetch total row count
        row_count_query = f"SELECT COUNT(*) FROM `{table_name}`"
        row_count = mysql_hook.get_first(row_count_query)[0]
        logger.info(f"Table {table_name} has {row_count} rows")

        return {
            'columns': columns,
            'row_count': row_count
        }
    
    @task
    def create_target_table(source_table: dict[str, any], params: dict[str, any]):
        """
        Create the target PostgreSQL table if it does not exist.

        Args:
            source_table: Dict with keys 'columns' (list of column tuples)
            params: Dictionary with DAG parameters
        """
        
        table_name = params.get('table_name')
        if not table_name:
            logger.error("No table_name provided in DAG params.")
            raise ValueError("table_name is required in params")

        source_table_columns = source_table.get('columns', [])
        transformed_columns = ', '.join(transform_mysql_to_postgres(source_table_columns))

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {transformed_columns}, ts_db TIMESTAMP
            );
        """

        logger.info(f"Creating target table {table_name} with columns: {transformed_columns}")
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        pg_hook.run(query)
        logger.info(f"Target table {table_name} created or already exists")
    
    @task
    def evaluate_batches(source_table: dict[str, any], params: dict[str, any]) -> list[dict[str, int]]:
        """
        Generate batch definitions for extracting data in chunks from MySQL.

        Args:
            source_table: Dict with keys 'row_count' and 'columns'
            params: Dictionary with DAG parameters.

        Returns:
            List of batch dictionaries with keys:
                - 'limit': batch size
                - 'offset': starting row index
                - 'columns': list of column names
        """
        
        row_count = source_table.get('row_count', 0)
        batch_size = params.get('batch_size', 1000)
        columns = [col[0] for col in source_table.get('columns', [])]
        
        if row_count == 0:
            logger.info("Source table is empty, no batches to process")
            return []
        
        offset = 0
        batches = list()

        while offset < row_count:
            batches.append(
                {
                'limit': batch_size,
                'offset': offset,
                'columns': columns
                }
            )

            offset += batch_size

        logger.info(f"Evaluated {len(batches)} batches for {row_count} rows with batch size {batch_size}")
        return batches
    
    @task
    def perform_extract_strategy(params: dict[str, any]) -> str:
        """
        Determine the filter query for incremental extraction.

        Args:
            params: Dictionary with DAG parameters.

        Returns:
            SQL filter string to append to the WHERE clause for incremental extraction.
            Empty string if FULL extraction or no previous data exists.
        """
    
        extract_strategy = params.get('extract_strategy', None)
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        target_table = params.get('table_name')
        
        if not target_table:
            logger.error("No table_name provided in params.")
            raise ValueError("table_name is required in params")
    
        
        filter_query = ''
                
        if extract_strategy == ExtractStrategy.INCREMENTAL:
            last_inserted_query = f"""
            SELECT MAX(ts_db) AS max_ts_db
            FROM {target_table};
            """
            
            last_date = pg_hook.get_first(last_inserted_query)[0]
            if last_date is not None:
                filter_query = f'AND GREATEST(created_at, updated_at) > "{last_date}"'
                logger.info(f"Incremental extract filter applied: {filter_query}")
            else:
                logger.info("No previous data found, performing full extract.")
        
        else:
            logger.info("Full extract selected, no filter applied.")
        
        return filter_query
    
    @task
    def truncate_target_table(params: dict[str, any]) -> None:
        """
        Truncate target PostgreSQL table if load strategy is FULL_REFRESH.

        Args:
            params: Dictionary with DAG parameters.
        """
    
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        target_table = params.get('table_name')
        load_strategy = params.get('load_strategy', None)
        
        if not target_table:
            logger.error("No table_name provided in params.")
            raise ValueError("table_name is required in params")
        
        if load_strategy == LoadStrategy.FULL_REFRESH:
            logger.info(f"Truncating table {target_table} due to FULL_REFRESH strategy")
            query = f'TRUNCATE TABLE {target_table} RESTART IDENTITY;'
            pg_hook.run(query)
            logger.info(f"Table {target_table} truncated successfully")
        else:
            logger.info(f"No truncation performed, load_strategy={load_strategy}")

        return
    
    @task
    def extract_batches(batch: dict[str, int], ti: TaskInstance | None = None, params: dict[str, any] | None = None) -> list[dict[str, any]]:
        """
        Extract a batch of rows from the source MySQL table.

        Args:
            batch: Dict with keys 'limit', 'offset', 'columns'
            ti: TaskInstance for XCom pulls
            params: Dictionary with DAG parameters.
            
        Returns:
            List of dicts, where each dict represents a row (column_name: value)
        """
    
        filter_query = ti.xcom_pull(task_ids='perform_extract_strategy') or ''
        columns = batch.get('columns', [])
        mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')
        table_name = params.get('table_name')
        
        if not table_name or not columns:
            logger.warning("No table_name or columns provided, returning empty batch")
            return []
        
        query = f"""
        SELECT {', '.join(columns)}
        FROM {params.get('table_name')}
        WHERE 1=1
        """
        
        if filter_query:
            query += f'\n{filter_query} '
        query += f'LIMIT {batch.get('limit')} OFFSET {batch.get('offset')}'
        
        logger.info(f"Extracting batch with LIMIT {batch.get('limit')} OFFSET {batch.get('offset')}")
        query_result = mysql_hook.get_records(query)
        logger.info(f"Extracted {len(query_result)} rows")
        
        return [dict(zip(columns, row)) for row in query_result]

    @task
    def transform_rows(rows_batch: list[dict[str, any]], ti: TaskInstance | None = None) -> list[dict[str, any]]:
        """
        Transform a batch of rows from MySQL to Postgres types.

        Args:
            rows_batch: List of dicts representing rows extracted from MySQL
            ti: TaskInstance for pulling source table metadata from XCom

        Returns:
            List of transformed rows as dicts
        """
        
        source_columns = ti.xcom_pull(task_ids='get_source_table').get('columns', [])
        column_types = {col[0]: col[1] for col in source_columns}

        logger.info(f"Transforming {len(rows_batch)} rows")
        
        transformed = [transform_row(row, column_types) for row in rows_batch]
        
        logger.info(f"Transformed {len(transformed)} rows")
        return transformed
    
    @task
    def load_bathes(batch: list[dict[str, any]], params: dict[str, any], ti: TaskInstance | None = None, data_interval_end=None) -> None:
        """
        Load transformed batch into PostgreSQL with optional UPSERT or APPEND strategy.

        Args:
            batch: List of transformed rows
            params: Dictionary with DAG parameters
            ti: TaskInstance for XCom pulls
            data_interval_end: Timestamp for ts_db column
        """
        
        if not batch:
            logger.info("No rows to load, skipping batch")
            return
        
        load_strategy = params.get('load_strategy')
        target_table = params.get('table_name')
        columns = [row[0] for row in ti.xcom_pull(task_ids='get_source_table').get('columns')]
        
        if not target_table or not columns:
            logger.warning("Missing target_table or columns, skipping batch")
            return
        
        placeholders = ('%s, ' * len(columns)) + '%s' # add placeholder for ts_db
        ts_db = data_interval_end
        primary_key_column = ti.xcom_pull(task_ids='create_target_table', key='primary_key_column')
        pg_hook = PostgresHook(postgres_conn_id='postgres_connection')
        
        if primary_key_column:
            update_data = [f'"{col}" = EXCLUDED."{col}"'for col in columns if col != primary_key_column]
        
        insert_query = f"""
        INSERT INTO {target_table} ({', '.join(columns)}, ts_db)
        VALUES ({placeholders})
        """
        
        if load_strategy == LoadStrategy.APPEND and primary_key_column:
            insert_query += f'\nON CONFLICT({primary_key_column}) DO NOTHING'
        elif load_strategy == LoadStrategy.UPSERT and primary_key_column:
            insert_query += f'\nON CONFLICT({primary_key_column}) DO UPDATE SET {', '.join(update_data)}'
                    
        data_rows = [
            tuple(row.get(col) for col in columns) + (ts_db, )
            for row in batch
        ]
        
        logger.info(f"Inserting {len(data_rows)} rows into {target_table} with strategy {load_strategy}")
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, data_rows)
            conn.commit()
            
        logger.info(f"Loaded {len(data_rows)} rows into {target_table}")
                    
    get_source_table_task = get_source_table()
    create_target_table_task = create_target_table(source_table=get_source_table_task)
    evaluate_batches_task = evaluate_batches(source_table=get_source_table_task)
    perform_extract_strategy_task = perform_extract_strategy()
    truncate_target_table_task = truncate_target_table()
    extract_batches_task = extract_batches.expand(batch=evaluate_batches_task)
    
    [evaluate_batches_task, create_target_table_task] >> perform_extract_strategy_task
    perform_extract_strategy_task >> truncate_target_table_task >> extract_batches_task
    
    transform_rows_task = transform_rows.expand(rows_batch=extract_batches_task)
    load_bathes_task = load_bathes.expand(batch=transform_rows_task)
    