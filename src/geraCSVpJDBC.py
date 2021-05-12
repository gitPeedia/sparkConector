import json
import findspark
findspark.init('/home/diego/spark-3.1.1-bin-hadoop2.7')

import pyspark
from pyspark import SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
spark = SparkSession.builder.appName("dfBuilder").getOrCreate()

def geraDF(conexao, CSV_path, first_row_is_header, delimiter, file_type, table, num_rows_partition, temporaria, escrever):
    
    Path = CSV_path.format(conexao.getDb(), table)

    print(Path)

    partitioned_CSV_path = Path + table

    if temporaria:
        df = geraTemporaria(conexao.getPassword(), conexao.getUser(), conexao.getDb(), conexao.getUrl(), table, conexao.getDriver())
    else:
        df = spark.read \
            .format("jdbc") \
            .option("url", conexao.getUrl()) \
            .option("fetchsize", "10000") \
            .option("driver", conexao.getDriver()) \
            .option("dbtable", table) \
            .option("user", conexao.getUser()).option("password", conexao.getPassword()) \
            .load()

    #df.show()

    if escrever:
        df.write \
            .format(file_type) \
            .option("header", first_row_is_header) \
            .option("delimiter", delimiter) \
            .save(Path)

        if (df.count() >= num_rows_partition):
            df = df.repartition(df.count() // num_rows_partition)
            print("tabela {} tem {} registros".format(table, df.count() // num_rows_partition))

        df.write.format(file_type) \
            .option("header", first_row_is_header) \
            .option("delimiter", delimiter) \
            .option("scape", "\"") \
            .option("dateFormat", "yyyy-MM-dd HH:mm:ss") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .option('compression', 'gzip') \
            .save(partitioned_CSV_path)
    else:
        return df

def geraTemporaria(password, username, db, url, table, driver_url):

    df = spark.read \
        .format("jdbc") \
        .option("url", url.format(db)) \
        .option("user", username) \
        .option("password", password) \
        .option("dbtable", table) \
        .option("fetchsize", "10000") \
        .option("driver", driver_url) \
        .load()#.limit(1)

    df.withColumn("data_exportacao", current_date().cast("string")) \
        .registerTempTable(table);

    return df