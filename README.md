## sparkConector



### Projeto focado em transacionar dados em diversos datasources usando spark dataFrames
Project focused on transacting data in several datasources using spark dataFrames
-   Certifique-se de ter instalado em seu computador uma versão igual ou superior ao java 8 (Make sure you have a version equal to or higher than java 8 on your computer)
-   Preparar ambiente seguindo passos listados abaixo (Prepare environment by following steps below)
-   Abra o arquivo credenciais.json na pasta aux e sete as informações necessária para conexão com sua base de dados:
-   Após configurar o ambiente instale o findspark, caso seu código não esteja na pasta do pyspark
(After setting up the environment, install findspark, if your code is not in the pyspark folder)
-   Após instalar o findspark, edite o arquio main.py colocando a pasta na qual seu spark foi descompactado
seguindo o código em exemplo (After installing findspark, edit the main file by placing the folder in which your spark was unzipped
following the code in example)
- Acesse via terminal seus a pasta onde esta o seu main e execute o comando ```python3 main.py```


#### Achando seu spark
find your spark
```python

import findspark
findspark.init('<<PASTA DE DESCONPACTAÇÃO DO SPARK>>')
import pyspark
```

#### Altere arquivo credenciais.json com suas credenciais de banco de dados
Change credentials.json file with your database credentials
./aux/credenciais.json
```json
{
    "<<NOME DA CONEXÃO>>": {
        "user": "<<CREDENCIAL DE USUÁRIO DA BASE A ACESSAR>>",
        "password": "<<SENHA DE USUÁRIO>>",
        "host": "<<jdbc:<<SUA BASE>>://URL DO SERVIDOR OU IP LOCAL>>",
        "name": "<<NOME DA INSTÂNCIA>>",
        "db": "<<NOME DA BASE>>",
        "port": "<<PORTA DEFAULT OU CUSTOM DO DATA SERVER>>",
        "folder": "<<NOME DA PASTA QUE IRÁ SEUS DATAFRAMES>>"
    }
}
```

### Preparando o ambiente Red Hat (Para outro distribuição Linux, aplicar comandos equivalentes)

Preparing the Red Hat environment (For other Linux distribution, apply equivalent commands)


#### python3-pip & py4j

```bash
yum update -y \
&& yum install -y wget \
&& yum install -y python3-pip \
&& pip3 install py4j
```

#### scala & spark & hadoop
```bash
wget http://downloads.lightbend.com/scala/2.11.8/scala-2.11.8.rpm && yum install -y scala-2.11.8.rpm \
&& wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz && tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz
```

#### variáveis de ambiente
env variables
```bash
export SPARK_HOME="/<<SUA PASTA>>/spark-3.1.1-bin-hadoop2.7"
export PATH="$SPARK_HOME:$PATH"
export PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
export PYSPARK_PYTHON="python3"
```
