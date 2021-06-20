from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 load_sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"Loading fact table {self.table} in Redshift")
        redshift.run(f"""
            BEGIN
            INSERT INTO {self.table}
            {self.load_sql_stmt};
            COMMIT;
        """)
