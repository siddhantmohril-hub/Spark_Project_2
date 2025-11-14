from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,concat,col,hash,isnull,current_timestamp
from datetime import datetime
import cx_Oracle
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars "F:\\JAVA\\JDBC_connection\\jars\\ojdbc11.jar" pyspark-shell'
# initialize Spark Session
spark = SparkSession.builder.appName("Spark_Project")\
        .config("spark.sql.streaming.fileStream.log.level", "ERROR")\
        .config("spark.sql.streaming.log.level", "ERROR")\
        .config("spark.log.level", "ERROR")\
        .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# adding a hash column
def add_hash(s_df):
    tdf=s_df.withColumn("key_hash",hash(concat(col("VendorID"),col("tpep_pickup_datetime"),col("tpep_dropoff_datetime"),\
                                         col("PULocationID"),col("DOLocationID"))))
    return tdf
# adding create timestamp to data
def add_timestamp(s_df):
    t_df=s_df.withColumn("CREATE_TS",current_timestamp())
    return t_df
# reading the Data already in output table
def read_tgt_data(cur,hsh):
    #hsh=[-2100697113,-2100731790]
    try:
        p=",".join(":"+str(i+1) for i in range(len(hsh)))
        qry=f"SELECT * FROM TAXI_TRIPDATA WHERE \"key_hash\" IN ({p})"
        cur.execute(qry,hsh)
        col=[i[0] for i in cur.description]
        df1=spark.createDataFrame(cur.fetchall(),col)
        return df1
    except Exception as e:
        print(e)
        return None
def dbconnect():
    #cx_Oracle.init_oracle_client(lib_dir=r"C:\instantclient_21_7")
    hostname='localhost'
    username='system'
    password='Oct_2k25'
    SID='oracldb'
    try:
        connection=cx_Oracle.connect(username,password,'{0}/{1}'.format(hostname,SID))
        print('Connection successful')
        cur=connection.cursor()
        return cur
    except Exception as e:
        return e
def main():
    #read data from input file into dataframe
    try:
        dt=datetime.now()
        year=dt.strftime("%Y")
        month=dt.strftime("%m")
        tst="F:\\Spark\\Project_1\\Spark_dataset\\yellow_tripdata_{}-{}.parquet".format("2025","01")
        taxi_df=spark.read.parquet(tst)
        #print(taxi_df.count()) #3475  
        temp_df=taxi_df.sample(0.0001)
        #temp_df.coalesce(1).write.csv("F:\\Spark\\Temp",header=True,mode="append")
    except Exception as e:
        print('No Data Available for the month')
        return
    cursor=dbconnect()
    temp_df=add_hash(temp_df)
    temp_df=add_timestamp(temp_df)
    x_lst=temp_df.select("key_hash")
    x_lst=[int(i["key_hash"]) for i in x_lst.collect()]
    #print(type(cursor))
    tgt_df=read_tgt_data(cursor,x_lst)
    #print(tgt_df.count()) 
    if tgt_df is not None:
        final_df=type_2_upd(tgt_df,temp_df)
    else:
        final_df=temp_df
    final_df=add_timestamp(final_df)
    dataload_tgt(cursor,final_df)
    return
main()