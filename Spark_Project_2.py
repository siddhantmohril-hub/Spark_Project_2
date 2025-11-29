from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,concat,col,hash,isnull,current_timestamp,lit
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
def conv_to_flt(val):
    #print(type(val))
    if isinstance(val,type(None)):
        return 0
    return val
def read_tgt_data(cur,hsh):
    hsh=[-1087655974,248726584]
    try:
        p=",".join(":"+str(i+1) for i in range(len(hsh)))
        qry=f"SELECT * FROM TAXI_TRIPDATA_HIST WHERE \"key_hash\" IN ({p}) and \"cur_rec_ind\"=\'Y\'"
        cur.execute(qry,hsh)
        col=[i[0] for i in cur.description]
        a=cur.fetchall()
        #print(a)
        rows_as_dict=[dict(zip(col,[conv_to_flt(x) for x in r])) for r in a]
        #print(rows_as_dict)
        df1=spark.createDataFrame(rows_as_dict)
        return df1
    except Exception as e:
        print(e)
        print("in read_tgt_data")
        return None
def temp_hash(df):
    tmp_df=df.withColumn("temp_hash",hash(concat(col("passenger_count"),col("trip_distance"),col("RatecodeID"),col("store_and_fwd_flag"),\
                                            col("payment_type"),col("fare_amount"),col("extra"),col("mta_tax"),\
                                                col("tip_amount"),col("tolls_amount"),col("improvement_surcharge"),col("total_amount"),\
                                                    col("congestion_surcharge"),col("Airport_fee"),col("cbd_congestion_fee"))))
    return tmp_df
def type_2_upd(cur,tgt_df,temp_df):
    tgt_df=temp_hash(tgt_df)
    temp_df=temp_hash(temp_df)
    upd_data(cur,tgt_df,temp_df)
    final_df=temp_df.join(tgt_df,temp_df.temp_hash==tgt_df.temp_hash,"left")
    final_df=final_df.select(temp_df.VendorID,temp_df.tpep_pickup_datetime,temp_df.tpep_dropoff_datetime,temp_df.passenger_count,temp_df.trip_distance,\
                    temp_df.RatecodeID,temp_df.store_and_fwd_flag,temp_df.PULocationID,temp_df.DOLocationID,temp_df.payment_type,temp_df.fare_amount,temp_df.extra,\
                        temp_df.mta_tax,temp_df.tip_amount,temp_df.tolls_amount,temp_df.improvement_surcharge,temp_df.total_amount,temp_df.congestion_surcharge,\
                            temp_df.Airport_fee,temp_df.cbd_congestion_fee,temp_df.key_hash)\
                                .filter(isnull(tgt_df.temp_hash))
    return final_df  
def dataload_tgt(cursor,final_df):
    cursor.execute("SELECT ts.table_name FROM all_tables ts WHERE ts.tablespace_name = 'EXAMPLE' ORDER BY ts.table_name")
    p=cursor.fetchall()
    if p is None:
        q='CREATE TABLE \
            TAXI_TRIPDATA_HIST (\
                VendorID NUMBER(10),\
                tpep_pickup_datetime TIMESTAMP,\
                tpep_dropoff_datetime TIMESTAMP,\
                passenger_count NUMBER(10),\
                trip_distance NUMBER(10,2),\
                RatecodeID NUMBER(10,2),\
                store_and_fwd_flag   CHAR(1),\
                PULocationID         NUMBER(10),\
                DOLocationID         NUMBER(10),\
                payment_type         NUMBER(10),\
                fare_amount          NUMBER(10,2),\
                extra                NUMBER(10,2),\
                mta_tax              NUMBER(10,2),\
                tip_amount           NUMBER(10,2),\
                tolls_amount         NUMBER(10,2),\
                improvement_surcharge   NUMBER(10,2),\
                total_amount         NUMBER(10,2),\
                congestion_surcharge NUMBER(10,2),\
                Airport_fee          NUMBER(10,2),\
                cbd_congestion_fee   NUMBER(10,2),\
                cur_rec_ind     CHAR(1),\
                key_hash NUMBER(10,2),\
                CREATE_TS TIMESTAMP)'
        cursor.execute(q)
        cursor.execute('COMMIT')
    final_df=final_df.withColumn("cur_rec_ind",lit('Y'))
    oracle_properties={
        "driver":"oracle.jdbc.driver.OracleDriver",
        "url": "jdbc:oracle:thin:@localhost:1521:oracldb",
        "user": "system",
        "password": "Oct_2k25"
    }
    final_df.write.format("jdbc") \
    .option("url", oracle_properties["url"]) \
    .option("driver", oracle_properties["driver"]) \
    .option("dbtable", "TAXI_TRIPDATA_HIST") \
    .option("user", oracle_properties["user"]) \
    .option("password", oracle_properties["password"]) \
    .mode("append").save()
    return
def upd_data(cur,tgt_df,temp_df):
    try:
        upd_df=tgt_df.join(temp_df,tgt_df.key_hash==temp_df.key_hash,"inner")
        upd_df=upd_df.select(tgt_df.key_hash).filter(tgt_df.temp_hash!=temp_df.temp_hash)
        upd_lst=[int(i["key_hash"]) for i in upd_df.collect()]
        upd_lst.append(248726584)
        p=",".join(":"+str(i+1) for i in range(len(upd_lst)))
        if p is not None:
            qry=f"UPDATE TAXI_TRIPDATA_HIST SET \"cur_rec_ind\"=\'N\' WHERE \"key_hash\" in ({p})"
            print(qry)
            cur.execute(qry,upd_lst)
            cur.execute('COMMIT')
        return
    except Exception as e:
        print(e)
        print("in upd_data")
        return
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
    #tgt_df.show(1)
    #print(tgt_df.count()) 
    if tgt_df is not None:
        final_df=type_2_upd(cursor,tgt_df,temp_df)
    else:
        final_df=temp_df
    final_df=add_timestamp(final_df)
    dataload_tgt(cursor,final_df)
    return
main()