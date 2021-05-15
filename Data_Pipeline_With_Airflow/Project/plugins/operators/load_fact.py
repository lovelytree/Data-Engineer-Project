from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    load data to fact table from staging tables
    """

    ui_color = '#F98866'
   
    insert_stmt = """
        INSERT INTO {table_name} 
        {select_sql}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
             
        self.log.info(f"Loading data to fact table {self.table}")
        formatted_sql = LoadFactOperator.insert_stmt.format(
            table_name = self.table,
            select_sql = self.sql_stmt
        )
        
        redshift.run(formatted_sql)
        
        self.log.info(f"Completed : {self.table} !")
        
