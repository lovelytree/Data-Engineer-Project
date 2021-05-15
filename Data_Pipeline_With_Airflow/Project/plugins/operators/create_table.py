from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """
    create all related tables in redshift
    """
        
    ui_color = '#F98866'
    
    sql_file = "/home/workspace/airflow/create_tables.sql"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Creating tables')        
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        create_sql_stmt = open(CreateTableOperator.sql_file, 'r').read()
        redshift.run(create_sql_stmt)
        
        
