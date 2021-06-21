from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def check_result(self, redshift_hook, check):
        """
        Execute Data Quality verification
        """
        sql = check.get('check_sql')
        expected = check.get('expected_records')

        self.log.info(f"Data Quality :: Verifying [{sql}].")
        
        records = redshift_hook.get_records(sql)[0]
        self.log.info(f"Data Quality :: Verifying [{sql}] RECORDS [{records}].")
        if len(records) < 1:
            raise ValueError(f"Data Quality :: Failed. Run [{sql}] with no result.")
        
        num_records = records[0]
        self.log.info(f"Data Quality :: Verifying [{sql}] num_records [{num_records}].")
        if num_records != expected:
            raise ValueError(f"Data Quality :: Failed. Run [{sql}] expecting result '{expected}', given '{num_records}'.")
        
        self.log.info(f"Data Quality :: OK [{sql}].")
        return True

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            self.log.info(f"Data Quality :: Checking {table} table")
            if len(self.dq_checks):
                for check in self.dq_checks:
                    self.check_result(redshift_hook, check) if check.get('table') == table else None
            else:
                records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data Quality :: Failed. Table {table} returned no results")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data Quality :: Failed. Table {table} contained 0 rows")
            self.log.info(f"Data Quality :: Success! Table {table} passed all verifications")
