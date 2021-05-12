import json
from abc import ABCMeta, abstractmethod

struct = json.loads(open("aux/credenciais.json", "r").read())

class IConex(metaclass=ABCMeta):

    def getUrl():
        "com.mysql.cj.jdbc.Driver"
    def getUser():
        "com.mysql.cj.jdbc.Driver"
    def getPassword():
        "com.mysql.cj.jdbc.Driver"
    def getDriver():
        "com.mysql.cj.jdbc.Driver"
    def getPath():
        "com.mysql.cj.jdbc.Driver"

class MySqlConex(IConex):

    def __init__(self):
        self.driver = "com.mysql.cj.jdbc.Driver"
        self.path = "minhaConexao"
        self.choice = struct["NOME_DA_CONEXAO"]
        self.db = self.choice['db']
        self.user = self.choice['user']
        self.password = self.choice['password']

    def getDb(self):
        return self.db

    def getUser(self):
        return self.user

    def getPassword(self):
        return self.password

    def getDriver(self):
        return self.driver
    
    def getPath(self):
        return self.path    

    def getUrl(self):
        return "{}{}/{}?zeroDateTimeBehavior = convertToNull".format(self.choice['host'], self.choice['port'], self.choice['db'])

class ConexFactrory():

    def getConex(typedataconexion):
        try:
            if typedataconexion == "NOME_DA_CONEXAO":
                return MySqlConex()
            raise AssertionError("Não foi possível encontrar a conexão")
        except AssertionError as _e:
            print(_e)