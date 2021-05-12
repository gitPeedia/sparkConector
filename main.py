## Arqiuvos Auxiliares
from src import dFBuilder, geraCSVpJDBC, conexoes
from getname import random_name
import datetime
import radar 
import random

## Bibliotecas
import os
import glob
import json
import shutil

# PySpark
import findspark
findspark.init('/<<SUA PASTA>>/spark-3.1.1-bin-hadoop2.7')
import pyspark
from pyspark.sql import Column, DataFrame, Row, SparkSession
spark = SparkSession.builder.appName("dfBuilder").getOrCreate()

######  Parametros Banco de dados ######
enviroment=None

######  CSV criaçao ######
file_type = "csv"
CSV_path = "/home/diego/Documentos/arquivos/carga/{}/{}/"
file_path = "/home/diego/Documentos/arquivos/carga/json/"
arquivo = "/home/diego/Documentos/arquivos/carga/planodb/cliente"
table = "acionamento"
delimiter = ","
first_row_is_header = "true"
num_rows_partition=5
op = "U"

####  Query  Inserts PA ####
queryAcionamento = """
    select {} as idacionamento
        , aci.dataAlteracao
        , aci.dataInclusao
        , aci.usuario
        , '{}' as dataDoAcionamento
        , '{}' as dataDoAtendimento
        , {} as idPet
        , {} as idplanoServico
        from acionamento aci
"""

queryListaServicos = """
        select idplanoServico
        from planoServico ps
        join contrato cont
            on ps.idPlano = cont.idPlano
        join pet
            on pet.idCliente = cont.idCliente
        where pet.idPet = {}
"""

######  json das tabelas ######
struct = json.loads(open("aux/estrutura_outra.json", "r").read())

######  colocando campo linha nas tabelas  #######
conexao = conexoes.ConexFactrory.getConex("NOME_DA_CONEXAO")

dFBuilder.tempView(struct, conexao)

connectionProperties = {
  "user" : conexao.getUser(),
  "password" : conexao.getPassword(),
  "driver" : conexao.getDriver()
}

idAcionamento = 1
for item in range(100):
    id_pet = random.randrange(1, 200)
    listaServicos = spark.sql(queryListaServicos.format(id_pet)).limit(100).collect()
    for planoServico in listaServicos:
        data_inicio = radar.random_datetime(start='2021-01-08', stop='2021-03-08T23:59:59')
        data_fim = radar.random_datetime(start='2021-03-08', stop='2021-05-08T23:59:59')
        df = spark.sql(queryAcionamento.format(idAcionamento, data_inicio, data_fim, id_pet, planoServico[0])).limit(1)
        idAcionamento += 1
        df.write.mode('append').jdbc(url=conexao.getUrl(), table=table, properties=connectionProperties)