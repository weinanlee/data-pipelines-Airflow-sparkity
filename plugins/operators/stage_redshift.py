from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id = 'redshift',
                 aws_conn_id = 'aws_credentials',
                 copy_options ='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id =redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    def execute(self, context):
        ## AWS setup
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")

        self.log.info(f'Preparing to stage data from {self.s3_bucket}/{s3.s3_prefix} to {self.table} table... ')

        copy_query = """
        COPY {table} 
        FROM 's3://{s3_bucket}/{s3_prefix}'
        WITH credentials
        'aws_access_id={access_key};
        aws_secret_access_key={secrect_key}'
        {copy_options};
        """.format(table = self.table,
                   s3_bucket = self.s3_bucket,
                   access_key = self.credentials.access_key,
                   secrect_key =self.credentials.secrect_key,
                   copy_options = self.copy_options)

        self.log.info("Excuting COPY command...")
        redshift_hook.run(copy_query)
        self.log.info("COPY command completed.")





