from fastapi import FastAPI
from .db.redis_db import redis_client
from .db.pg import fetch_postgres_data
from .db.mongo import fetch_mongo_data
from .db.neo4j import fetch_neo4j_data

# Tags metadata for OpenAPI grouping
tags_metadata = [
    {"name": "Clientes", "description": "Create/Update/Delete clients across all databases (Postgres, Mongo, Neo4j) and replicate consolidated records to Redis."},
    {"name": "Compras", "description": "Purchase endpoints (record purchases in Postgres and update cache)."},
    {"name": "Produtos", "description": "Products stored in Postgres or Neo4j (CRUD)."},
    {"name": "Profiles", "description": "MongoDB profiles and interests (document store)."},
    {"name": "Neo4j", "description": "Graph nodes and relationships (persons and product nodes)."},
    {"name": "Cache", "description": "Cache management and queries (Redis)."},
    {"name": "Admin", "description": "Administrative endpoints: seeding and migration."},
]

app = FastAPI(openapi_tags=tags_metadata)

from .routes.api_routes import router as api_router
app.include_router(api_router)

@app.get("/replicar")
def replicar_dados():

    # 🔵 POSTGRES
    rows = fetch_postgres_data()
    for i, row in enumerate(rows):
        redis_client.set(f"postgres:produto:{i}", str(row))

    # 🟢 MONGO
    docs = fetch_mongo_data()
    for doc in docs:
        redis_client.set(f"mongo:cliente:{doc['_id']}", str(doc))

    # 🟣 NEO4J
    nodes = fetch_neo4j_data()
    for i, node in enumerate(nodes):
        redis_client.set(f"neo4j:produto:{i}", str(node))

    return {
        "status": "OK",
        "postgres_registros": len(rows),
        "mongo_registros": len(docs),
        "neo4j_registros": len(nodes)
    }
