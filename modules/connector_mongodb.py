# ##############################################################
# ########### Bruno Yaporandy - Connector MongoDB #############
# ##############################################################

from pymongo import MongoClient

class Connector_mongodb():
    cliente = ""
    database = ""
    collection = ""
    
    def __init__(self, database = 'teste', collection = 'testando'):
        self.cliente = MongoClient("mongodb://127.0.0.1:27017/")
        self.database =  self.cliente[database]
        self.collection =self.database[collection]
        
    def set_database(self,database):
        self.database = database
        
    def set_collection(self, collection):
        self.collection = collection  
        
    def get_database(self):
        return self.database
    
    def get_collection(self):
        return self.collection
    

    # Métodos da classe:
    def insert(self,dados):
        self.collection.insert_many(dados)
        
    def find(self):
        lista_itens = []
        itens_db = self.collection.find()
        for i in itens_db:
            lista_itens.append(i)
        return lista_itens
    
    def delete_one(self):
        
        coluna = input("Você deseja excluir por qual dado? ")
        
        valor = input("Qual o valor desse dado do item que você deseja excluir? ")
        
        filter = {coluna: valor}
            
        self.collection.delete_one(filter)
        
    def delete_many(self):
        
        coluna = input("Você deseja excluir por qual dado? ")
        
        valor = input("Qual o valor desse dado dos itens que você deseja excluir? ")
        
        filter = {coluna: valor}
            
        self.collection.delete_many(filter)
        
    
    def update_one(self):
            
        coluna_escolhida = input("Digite a coluna que você deseja realizar uma alteração: ")
        old_value = input("Digite o valor antigo desse item nessa coluna: ")
        new_value= input("Digite o novo valor para esse item: ")
        
        
        filter = {coluna_escolhida: old_value}
        newvalues = {"$set": {coluna_escolhida: new_value}}
        
        self.collection.update_one(filter,newvalues)