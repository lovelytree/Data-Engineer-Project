from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    load data to dimension table from staging tables
    """
    
    ui_color = '#80BD9E'
    
    insert_stmt = """
        INSERT INTO {table_name} 
        {select_sql}
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 table="",
                 sql_stmt="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.truncate = truncate


    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info(f"Clearing data from dimension table {self.table}")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f"Loading data to dimension table {self.table}")
        
        formatted_sql = LoadDimensionOperator.insert_stmt.format(
            table_name=self.table,
            select_sql=self.sql_stmt
        )
        redshift.run(formatted_sql)
        
        self.log.info(f"Completed : {self.table} !")
        