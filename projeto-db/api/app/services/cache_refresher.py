from ..db.pg import query
from ..db.mongo import profiles
from ..db.neo4j import run_query
from ..db.redis_db import redis_db
import json

def clear_cache():
    redis_db.flushdb()


def refresh_cache():

    # Postgres
    clientes = query("SELECT * FROM clientes;")
    compras = query("SELECT * FROM compras;")
    produtos = query("SELECT * FROM produtos;")

    # Mongo
    perfil_map = {p["idCliente"]: p for p in profiles.find({})}

    # Neo4j
    neo = run_query("""
        MATCH (p:Person)-[:FRIEND]->(f:Person)
        RETURN p, collect(f) AS amigos
    """)

    amizade_map = {}
    for r in neo:
        pid = r["p"]["id"]
        amizade_map[pid] = [dict(f) for f in r["amigos"]]

    # Consolidação → salvar no Redis
    for c in clientes:
        # prefer external_id (UUID) when present; fall back to integer id for legacy rows
        pid_int = c.get("id")
        external = c.get("external_id")
        cid = str(external) if external is not None else str(pid_int)

        compras_cliente = [
            {**comp, "produto": next((p for p in produtos if p["id"] == comp["id_produto"]), None)}
            for comp in compras if comp.get("id_cliente") == pid_int
        ]

        consolidado = {
            "cliente": c,
            "perfil": perfil_map.get(cid),
            "amigos": amizade_map.get(cid, []),
            "compras": compras_cliente
        }

        redis_db.hset(f"cliente:{cid}", "data", json.dumps(consolidado, default=str))

    return True


# --- helper: build/replicate single client ---
def build_consolidated_for_client(cid: str):
    # try external_id (UUID-like) first, else try integer id
    client_row = None
    if isinstance(cid, str) and "-" in cid:
        rows = query("SELECT * FROM clientes WHERE external_id = %s", (cid,))
        client_row = rows[0] if rows else None
    if not client_row:
        try:
            pid = int(cid)
            rows = query("SELECT * FROM clientes WHERE id = %s", (pid,))
            client_row = rows[0] if rows else None
        except Exception:
            client_row = None

    if not client_row:
        return None

    pid_int = client_row.get("id")
    compras = query("SELECT * FROM compras WHERE id_cliente = %s", (pid_int,))
    produtos = query("SELECT * FROM produtos;")
    perfil = profiles.find_one({"idCliente": str(cid)})

    neo_rows = run_query("MATCH (p:Person {id:$id})-[:FRIEND]->(f:Person) RETURN collect(f) AS amigos", {"id": str(cid)})
    amigos = [dict(f) for f in neo_rows[0]["amigos"]] if neo_rows else []

    compras_cliente = [
        {**comp, "produto": next((p for p in produtos if p["id"] == comp["id_produto"]), None)}
        for comp in compras
    ]

    consolidado = {
        "cliente": client_row,
        "perfil": perfil,
        "amigos": amigos,
        "compras": compras_cliente
    }

    return consolidado


def replicate_client_to_redis(cid: str, consolidado: dict):
    redis_db.hset(f"cliente:{cid}", "data", json.dumps(consolidado, default=str))


def compute_recommendations(cid: str, top_n: int = 5):
    """Compute simple recommendations for client `cid` based on friends' purchases.
    Stores recommendations as a Redis list `recomendacoes:{cid}` and also injects into client hash `cliente:{cid}`.
    Returns a list of product dicts with counts sorted by popularity among friends."""
    consolidado = build_consolidated_for_client(cid)
    if not consolidado:
        return []

    # Client's purchased product ids
    client_purchased_ids = {c.get("id_produto") for c in consolidado.get("compras", [])}

    # Gather friends' purchases
    friends = consolidado.get("amigos", [])
    product_counts = {}
    for friend in friends:
        fid = str(friend.get("id")) or friend.get("id")
        # friend id might be numeric or external; try both
        friend_consol = build_consolidated_for_client(fid)
        if not friend_consol:
            continue
        for comp in friend_consol.get("compras", []):
            pid = comp.get("id_produto")
            if pid and pid not in client_purchased_ids:
                product_counts[pid] = product_counts.get(pid, 0) + 1

    if not product_counts:
        recs = []
    else:
        # sort product ids by count desc
        sorted_pids = sorted(product_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
        recs = []
        produtos = query("SELECT * FROM produtos;")
        produto_map = {p["id"]: p for p in produtos}
        for pid, cnt in sorted_pids:
            prod = produto_map.get(pid)
            if prod:
                recs.append({**prod, "score": cnt})

    # store recommendations in Redis list and also update client consolidated
    key = f"recomendacoes:{cid}"
    # replace list (delete existing and push new)
    try:
        redis_db.delete(key)
    except Exception:
        pass
    for item in recs:
        redis_db.rpush(key, json.dumps(item, default=str))

    # update consolidated object with recommendations
    consolidado["recomendacoes"] = recs
    replicate_client_to_redis(cid, consolidado)

    return recs


