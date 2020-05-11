from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = '',
                 redshift_conn_id = 'redshift',
                 select_sql_stmt = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql_stmt = select_sql_stmt

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        ## Postgre Hook
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Deleting previous data from {}...".format(self.table))
        delete_sql_stmt = 'DELETE FROM {}'.format(self.table)
        redshift_hook.run(delete_sql_stmt)
        
        insert_sql_stmt = 'INSERT INTO {} ({})'.format(self.table, self.select_sql_stmt)

        # Load table
        self.log.info("Loading data into {}.".format(self.table))
        redshift_hook.run(insert_sql_stmt)
        self.log.info("Finished loading data into {}.".format(self.table))
