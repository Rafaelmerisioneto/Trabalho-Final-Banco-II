from pymongo import MongoClient
import os

# Conexão e coleções exportadas para compatibilidade com serviços
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongo:27017"))
_db = client[os.getenv("MONGO_DB", "shop")]

profiles = _db["profiles"]
clientes = _db["clientes"]

def get_mongo_conn():
    return _db

def fetch_mongo_data():
    return list(clientes.find({}))
