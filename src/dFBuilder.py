import os
import re
from src import geraCSVpJDBC
#import geraCSVpRedshift

import findspark
findspark.init('/home/diego/spark-3.1.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit
spark = SparkSession.builder.appName("dfBuilder").getOrCreate()

######  JSON TEMP TABLES ######
def tempView(struct, conexao):
    for db, tables in struct.items():
        print(db)
        for table, param in tables.items():

            geraCSVpJDBC.geraTemporaria(conexao.getPassword(), conexao.getUser(), db, conexao.getUrl(), table, conexao.getDriver())

            campos = param.get("estrutura")
            pks = param.get("pk")

            query = '\n select '

            if (len(campos) > 0):
                for item in campos:
                    query += str(item[0])+', '
            else:
                query += '*, '

            queryTempTable = '{} row_number() over (partition by {} order by data_exportacao desc) as linha from {}'.format(query, ', '.join(pks), table)
            #print(queryTempTable)
            dataframe = spark.sql(queryTempTable)
            dataframe.registerTempTable(table);