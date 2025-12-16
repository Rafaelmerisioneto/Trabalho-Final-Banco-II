from neo4j import GraphDatabase
import os

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
    auth=(
        os.getenv("NEO4J_USER", "neo4j"),
        os.getenv("NEO4J_PASSWORD", "123456")  # troque
    )
)

def fetch_neo4j_data():
    with driver.session() as session:
        result = session.run("MATCH (p:Produto) RETURN p")  # SEUS NODES EXISTENTES
        return [record["p"] for record in result]


def run_query(cypher, params=None):
    with driver.session() as session:
        result = session.run(cypher, params or {})
        return [record.data() for record in result]
