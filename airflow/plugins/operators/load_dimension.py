from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 load_sql_stmt="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info(f"Loading dimension table {self.table} in Redshift")
        
        if self.append_data == False:
            self.log.info(f"Deleting all data of table {self.table} in Redshift")
            redshift.run(f"""
                BEGIN;
                TRUNCATE TABLE {self.table};
                COMMIT;
            """)

        redshift.run(f"""
            BEGIN;
            INSERT INTO {self.table} 
            {self.load_sql_stmt};
            COMMIT;
        """)
