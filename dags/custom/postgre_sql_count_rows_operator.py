from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class PostgreSQLCountRowsOperator(BaseOperator):
    """
    Operator that fetches count rows from table.

    Parameters
    ----------
    conn_id : str
        ID of the connection
    table_name : str
    """

    def __init__(self, conn_id, table_name, **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._table_name = table_name

    def execute(self, context):
        hook = PostgresHook(self._conn_id)
        engine = hook.get_sqlalchemy_engine()
        query = f"SELECT COUNT(*) FROM {self._table_name};"
        result = engine.execute(query).fetchone()[0]

        self.log.info(f"Rows count: {result}")

        return result

