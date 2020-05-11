from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_stmts=(),
                 result_checkers=(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_stmts = sql_stmts
        self.result_checkers = result_checkers

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for sql_stmt, result_checker in zip(self.sql_stmts, self.result_checkers):
            records = redshift_hook.get_records(sql_stmt)
            if not result_checker(records):
                raise ValueError('Data quality check failed. SQL: {}'.format(sql_stmt))
            self.log.info("Passed Quality Check: '{}'.".format(sql_stmt))