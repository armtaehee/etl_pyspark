import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql import functions as pysparkFunctions, Window
from dotenv import load_dotenv

class EtlJobForSertis:
    #This class is a ETL job using PySpark that read transaction.csv,transforms the data and loads the results into multiple destinations (Parquet,CSV,PostgreSQL)
    def __init__(self, **kwargs):
        load_dotenv(override=False) 
        self.spark_session = (
                SparkSession.builder .master(os.getenv('SPARK_MASTER_URL', 'local[*]')) .appName("SparkAppForEtlJob1").config("spark.jars", "/opt/postgresql-jdbc.jar").getOrCreate())
        self.input_file = kwargs['source']
        self.log = logging.getLogger(__name__)
        #Determine Load Type which is from command run
        if 'database' in kwargs and kwargs['database']:
            self.load_type="database"
            self.database=kwargs['database']
            self.table=kwargs['table']
        elif 'destination' in kwargs and kwargs['destination']:
            self.output = kwargs['destination']
            self.load_type = "parquet"

    def extract_transactions(self):
        #Extract transaction data from the CSV file and loads into a dataframe
        schema = StructType([
            StructField("transactionId", StringType(), True),
            StructField("custId", StringType(), True),
            StructField("transactionDate", DateType(), True),
            StructField("productSold", StringType(), True),
            StructField("unitsSold", IntegerType(), True)
            ])
        df_transactions = self.spark_session.read.schema(schema).csv(
                self.input_file,
                sep='|',
                header=True,
                escape="\"")
        return df_transactions

    def transform(self, df_transactions):
        #Transforms the extracted data to find customers favorite and return as dataframe
        df_transactions.createOrReplaceTempView('view_transactions')

        #Transforms the extracted data to find total transactions and total customers
        total_transactions = self.spark_session.sql("""
            SELECT * from view_transactions
            """).count()
        unique_customers = self.spark_session.sql("""
            SELECT custId from view_transactions
                group by custId
            """).count()
        
        self.log.info('''
        SUMMARY:
            Total Transactions: %s
            Total Customers   : %s
        ''', total_transactions, unique_customers)    
        #Transforms the extracted data to find customers favorite
        self.log.info('''TASK:  
                    Calculating the favorites
        ''')
        df_favorites = self.spark_session.sql("""
            WITH a as (
                SELECT * FROM (SELECT
                    custId       as custId,
                    productSold as productSold,
                    sum(vt.unitsSold) as totalUnitsSold,
                    RANK() OVER (partition by custId ORDER BY sum(vt.unitsSold) desc) AS ranking
                  FROM view_transactions vt
                  GROUP BY custId,productSold
                  )
                  where ranking = 1
            )
            SELECT custId, productSold, count(custId) as cnt from a GROUP BY custId,productSold ORDER BY cnt DESC
            -- SELECT custId, count(*) as cnt from a GROUP BY custId ORDER BY cnt DESC
        """)
        return df_favorites

    def load(self, load_type, df_to_load):
        #Load the transformed data into a parquet or PostgreSQL database check by load type
        if (load_type == 'parquet'): #check the load type that is parquet to continuse load in parquet
            df_to_load.write.mode("overwrite").parquet(self.output)
            self.log.info(
                    'Data successfully written on the path: %s',
                    self.output)
        elif (load_type == 'database'): #check the load type that is database to continuse load in portgressql
            self.log.info('Kicking off load to postgresql')
            #get the data from env
            db_host=os.getenv("POSTGRES_HOST")
            db_port=os.getenv("POSTGRES_PORT")
            db_user=os.getenv("POSTGRES_USERNAME")
            db_password=os.getenv("POSTGRES_PASSWORD")
            db_name=os.getenv("POSTGRES_DEFAULT_DB")
            self.log.info(f"Connecting to DB:{db_host}:{db_port},Database:{db_name},User:{db_user}")
            
            db_url=f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
            db_properties={
                "user": db_user,
                "password": db_password,
                "driver": "org.postgresql.Driver"
            }
            #load to database
            df_to_load.write.jdbc(url=db_url, table=self.table, mode="overwrite", properties=db_properties)
            self.log.info("Data has successfully written to postgresql")
        elif (load_type not in ['parquet', 'database']):
            self.log.info("The current output format is yet not supported")

    def convert(self):
        #Converts the prquet file to readable format(csv)
        if self.load_type == 'parquet':
            self.log.info('Kicking off convert job...')
            df=self.spark_session.read.parquet(self.output)
            df.write.csv("/opt/data/out.csv", header=True, mode="overwrite")
            self.log.info('Data csv has successfully written on the path: /opt/data/out.csv')
        else:
            self.log.info('pass')

    def analyze(self):  
        #I unsure if I understand the question correctly, that I have to read or analyze further more and then load the results in CSV format 
        #or convert parquet to csv and just write an explanation about read or analyze df_favorites
        #So I decided to do both. Since I work on weekends, HR won't reply to my emails, that's not their fault, I just explained the situation.
        self.log.info('Kicking off analyse job...')
        df=self.spark_session.read.option("header", "true").csv("/opt/data/out.csv") #read csv
        df.createOrReplaceTempView('customers') #create temp table
        self.log.info("Calculating the Most Popular Product and number favorite of Customer")
        #find top spender and top product
        Ranking_product_sold = self.spark_session.sql("""
            SELECT productSold, count(*) AS totalproductsale
            FROM customers
            GROUP BY productSold
            ORDER BY totalproductsale DESC
        """)
        #Number of customer favorite products
        Ranking_transaction_customer = self.spark_session.sql("""
            SELECT custId, COUNT(*) AS totaltransaction
            FROM customers
            GROUP BY custId
            ORDER BY totaltransaction DESC
        """)
        Ranking_product_sold.show()
        Ranking_transaction_customer.show()
        Ranking_transaction_customer.coalesce(1).write.mode("overwrite").option("header", "true").csv("/opt/data/Top_Spenders.csv")
        Ranking_product_sold.coalesce(1).write.mode("overwrite").option("header", "true").csv("/opt/data/Top_Productsold.csv")
        
    def run(self):
        #Executes the ETL
        self.log.info('Kicking off the etl job... ')
        self.load(self.load_type,
                self.transform(
                    self.extract_transactions(),
                    ))
        self.convert() 
        self.analyze()