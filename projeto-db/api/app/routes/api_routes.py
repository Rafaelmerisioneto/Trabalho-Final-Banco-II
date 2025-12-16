from fastapi import APIRouter, HTTPException, Response, status
from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal
from uuid import uuid4, UUID
from fastapi import BackgroundTasks
from ..services.cache_refresher import (refresh_cache, clear_cache, build_consolidated_for_client,
                                           replicate_client_to_redis, compute_recommendations)
from ..db.redis_db import redis_client as redis_db
from ..db import pg as db_pg
from ..db.mongo import clientes, profiles
from ..db.neo4j import run_query as neo_run
import json

router = APIRouter()

# --- Response models to improve OpenAPI visuals ---
class ClienteOut(BaseModel):
    id: Optional[int] = None
    external_id: Optional[str] = None
    cpf: Optional[str] = None
    nome: Optional[str] = None
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    email: Optional[str] = None

class PerfilOut(BaseModel):
    idCliente: str
    idade: Optional[int] = None
    interesses: Optional[List[str]] = None

class ProdutoSimple(BaseModel):
    id: int
    produto: str
    valor: Decimal
    quantidade: int
    tipo: Optional[str] = None

class CompraOut(BaseModel):
    id: int
    id_produto: int
    data: Optional[str] = None
    id_cliente: int
    produto: Optional[ProdutoSimple] = None

class ConsolidatedCliente(BaseModel):
    cliente: ClienteOut
    perfil: Optional[PerfilOut] = None
    amigos: List[dict] = []
    compras: List[CompraOut] = []
    recomendacoes: Optional[List[dict]] = None


# --- cache endpoints ---
@router.post("/cache/refresh", tags=["Cache"], summary="Refresh cache")
def refresh():
    clear_cache()
    refresh_cache()
    return {"status": "cache atualizado"}

@router.get("/redis/clientes", tags=["Cache"], summary="List clients from Redis")
def get_clientes():
    keys = redis_db.keys("cliente:*")
    data = []
    for k in keys:
        raw = redis_db.hget(k, "data")
        data.append(json.loads(raw))
    return data

# Unified consolidated clients endpoint (visual, uses 'Clientes' tag)
@router.get("/clientes", tags=["Clientes"], response_model=List[ConsolidatedCliente], summary="List consolidated clients (from Redis)")
def list_consolidated_clients():
    return get_clientes()

@router.get("/redis/clientes/friends", tags=["Cache"], summary="List clients and their friends")
def get_clientes_friends():
    # return consolidated clients with their amigos field
    all_clients = get_clientes()
    return [{"cliente": c.get("cliente"), "amigos": c.get("amigos", [])} for c in all_clients]

@router.get("/redis/clientes/compras", tags=["Cache"], summary="List clients and their purchases")
def get_clientes_compras():
    all_clients = get_clientes()
    return [{"cliente": c.get("cliente"), "compras": c.get("compras", [])} for c in all_clients]

@router.get("/redis/clientes/{id}/recomendacoes", tags=["Cache"], summary="Compute and store recommendations for a client")
def get_recommendations_for_client(id: str):
    recs = compute_recommendations(id)
    return {"id": id, "recomendacoes": recs}

@router.post("/seed/run", tags=["Admin"], summary="Run seed files to populate DBs (optional purge)")
def run_seed(purge: bool = False):
    """Reads ./mongo/seed_profiles.json, ./neo4j/init.cql, ./postgres/01_schema.sql and applies inserts. If purge=True, clears existing data first."""
    # purge if requested
    if purge:
        # Redis
        try:
            redis_db.flushdb()
        except Exception:
            pass
        # Postgres: delete child tables first
        try:
            db_pg.execute("DELETE FROM compras;")
            db_pg.execute("DELETE FROM produtos;")
            db_pg.execute("DELETE FROM clientes;")
        except Exception:
            pass
        # Mongo
        try:
            profiles.delete_many({})
            clientes.delete_many({})
        except Exception:
            pass
        # Neo4j
        try:
            neo_run("MATCH (n) DETACH DELETE n")
        except Exception:
            pass

    # Apply Mongo seed
    try:
        import json as _json
        with open('/app/seeds/seed_profiles.json', 'r') as fh:
            profiles_data = _json.load(fh)
        for doc in profiles_data:
            profiles.replace_one({'idCliente': doc['idCliente']}, doc, upsert=True)
    except Exception as e:
        return {"error": f"mongo seed failed: {e}"}

    # Apply Neo4j seed
    try:
        with open('/app/seeds/init.cql', 'r') as fh:
            cql = fh.read()
        # split by ; and execute statements
        for stmt in [s.strip() for s in cql.split(';') if s.strip()]:
            neo_run(stmt)
    except Exception as e:
        return {"error": f"neo4j seed failed: {e}"}

    # Apply Postgres seed (only handle INSERTs with care for FK references)
    try:
        with open('/app/seeds/01_schema.sql', 'r') as fh:
            sql = fh.read()
        import re
        # find specific INSERT statements
        clientes_ins = re.search(r"INSERT INTO\s+clientes\s*\(.*?\)\s*VALUES\s*(\(.*?\));", sql, flags=re.S | re.I)
        produtos_ins = re.search(r"INSERT INTO\s+produtos\s*\(.*?\)\s*VALUES\s*(\(.*?\));", sql, flags=re.S | re.I)
        compras_ins = re.search(r"INSERT INTO\s+compras\s*\(.*?\)\s*VALUES\s*(\(.*?\));", sql, flags=re.S | re.I)

        # insert clientes if present and capture original order mapping by CPF
        client_cpfs = []
        if clientes_ins:
            tuples_text = clientes_ins.group(1)
            parts = re.findall(r"\(([^)]*)\)", tuples_text)
            for ptext in parts:
                # ptext: '\'111.111.111-11','Ana','Rua A','SP','SP','ana@email.com'\n'
                m = re.match(r"\s*'([^']+)'\s*,\s*'([^']+)'", ptext)
                if m:
                    client_cpfs.append(m.group(1))
            stmt = "INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email) VALUES " + clientes_ins.group(1) + ";"
            db_pg.execute(stmt)

        # insert produtos and capture mapping from original position to real id
        product_names = []
        if produtos_ins:
            # extract tuples
            tuples_text = produtos_ins.group(1)
            parts = re.findall(r"\(([^)]*)\)", tuples_text)
            for ptext in parts:
                m = re.match(r"\s*'([^']+)'\s*,", ptext)
                if m:
                    product_names.append(m.group(1))
            stmt = "INSERT INTO produtos (produto, valor, quantidade, tipo) VALUES " + produtos_ins.group(1) + ";"
            db_pg.execute(stmt)

        # build mapping from original product index (1-based) to actual product id
        prod_map = {}
        if product_names:
            for idx, pname in enumerate(product_names, start=1):
                rows = db_pg.query("SELECT id FROM produtos WHERE produto = %s ORDER BY id DESC LIMIT 1", (pname,))
                if rows:
                    prod_map[idx] = rows[0]["id"]

        # build mapping for clients: original index (1-based) -> actual id
        client_map = {}
        if client_cpfs:
            for idx, cpf in enumerate(client_cpfs, start=1):
                rows = db_pg.query("SELECT id FROM clientes WHERE cpf = %s ORDER BY id DESC LIMIT 1", (cpf,))
                if rows:
                    client_map[idx] = rows[0]["id"]

        # insert compras using mapped product ids and client ids
        if compras_ins:
            tuples_text = compras_ins.group(1)
            parts = re.findall(r"\(([^)]*)\)", tuples_text)
            for ptext in parts:
                # expect: 1, '2025-12-01', 1
                vals = [v.strip() for v in ptext.split(',')]
                orig_pid = int(vals[0])
                date_val = vals[1].strip().strip("'")
                orig_cid = int(vals[2])
                real_pid = prod_map.get(orig_pid, orig_pid)
                real_cid = client_map.get(orig_cid, orig_cid)
                db_pg.execute("INSERT INTO compras (id_produto, data, id_cliente) VALUES (%s, %s, %s);", (real_pid, date_val, real_cid))
    except Exception as e:
        return {"error": f"postgres seed failed: {e}"}

    # rebuild cache
    try:
        refresh_cache()
    except Exception as e:
        return {"error": f"refresh_cache failed: {e}"}

    return {"status": "seed applied"}

@router.get("/redis/cliente/{id}", tags=["Cache"], summary="Get single client from Redis")
def get_cliente(id: str):
    data = redis_db.hget(f"cliente:{id}", "data")
    if not data:
        raise HTTPException(status_code=404, detail="cliente not found in cache")
    return json.loads(data)


# --- Postgres Produtos CRUD ---

# --- Compras (Postgres) ---
class CompraIn(BaseModel):
    id_produto: int
    id_cliente: str  # accepts numeric id or external_id (UUID)
    data: Optional[str] = None

@router.post("/compras", status_code=status.HTTP_201_CREATED, tags=["Postgres - Compras"], summary="Create a compra (purchase)")
def create_compra(c: CompraIn):

    # map id_cliente to numeric id in Postgres; create client if it exists only in Neo4j
    id_cliente = c.id_cliente
    pid = None
    # if looks like uuid, try external_id
    if isinstance(id_cliente, str) and "-" in id_cliente:
        rows = db_pg.query("SELECT id FROM clientes WHERE external_id = %s", (id_cliente,))
        if rows:
            pid = rows[0]["id"]
        else:
            # try to fetch person from neo4j and create client
            person = neo_run("MATCH (p:Person {id:$id}) RETURN p LIMIT 1", {"id": id_cliente})
            if person:
                p = dict(person[0]["p"])
                # create client in Postgres and Mongo
                res = db_pg.execute(
                    "INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email, external_id) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
                    (p.get("cpf"), p.get("nome"), None, None, None, None, id_cliente),
                    returning=True,
                )
                pid = res.get("id") if res else None
                # insert to Mongo
                doc = {"_id": id_cliente, "idCliente": id_cliente, "cpf": p.get("cpf"), "nome": p.get("nome")}
                clientes.insert_one(doc)
    else:
        try:
            pid = int(id_cliente)
        except Exception:
            pid = None

    if pid is None:
        raise HTTPException(status_code=400, detail="could not resolve id_cliente to a Postgres id")

    # insert compra
    date_val = c.data or None
    res = db_pg.execute(
        "INSERT INTO compras (id_produto, data, id_cliente) VALUES (%s, %s, %s) RETURNING id;",
        (c.id_produto, date_val, pid),
        returning=True,
    )
    if not res:
        raise HTTPException(status_code=500, detail="failed to create compra")

    # rebuild consolidated for the client and replicate
    # resolve cid for consolidated function (prefer external_id if exists)
    rows = db_pg.query("SELECT external_id FROM clientes WHERE id = %s", (pid,))
    cid = rows[0]["external_id"] if rows and rows[0].get("external_id") else str(pid)
    consolidado = build_consolidated_for_client(str(cid))
    if consolidado:
        replicate_client_to_redis(str(cid), consolidado)

    return {"id": res.get("id"), "cliente": consolidado}


# --- Postgres Clientes (direct) ---
@router.post("/postgres/clientes", status_code=status.HTTP_201_CREATED, tags=["Clientes"], response_model=ConsolidatedCliente, summary="Create a client in Postgres and replicate to other DBs")
def create_postgres_cliente(c: "ClienteIn", background_tasks: BackgroundTasks):
    # allow optional idCliente (if provided it must be a valid UUID, otherwise generate one)
    if c.idCliente:
        try:
            # validate UUID format
            UUID(str(c.idCliente))
            external_id = str(c.idCliente)
        except Exception:
            raise HTTPException(status_code=400, detail="idCliente provided must be a valid UUID")
    else:
        external_id = str(uuid4())

    # insert into Postgres with external_id
    res = db_pg.execute(
        "INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email, external_id) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id;",
        (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, external_id),
        returning=True,
    )
    if not res:
        raise HTTPException(status_code=500, detail="failed to create cliente in postgres")

    # insert into Mongo
    c_data = c.dict()
    c_data["idCliente"] = external_id
    doc = {"_id": external_id, **c_data}
    clientes.insert_one(doc)

    # create Person in Neo4j
    neo_run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;")
    neo_run("MERGE (p:Person {id:$id}) SET p.cpf=$cpf, p.nome=$nome RETURN p", {"id": external_id, "cpf": c.cpf, "nome": c.nome})

    # replicate consolidated to Redis (background)
    consolidado = build_consolidated_for_client(external_id)
    if consolidado:
        background_tasks.add_task(replicate_client_to_redis, external_id, consolidado)

    return consolidado


@router.put("/postgres/clientes/{id}", tags=["Clientes"], response_model=ConsolidatedCliente, summary="Update a client in Postgres and replicate")
def update_postgres_cliente(id: str, c: "ClienteIn"):
    # update Postgres by external_id or numeric id
    if isinstance(id, str) and "-" in id:
        db_pg.execute(
            "UPDATE clientes SET cpf=%s, nome=%s, endereco=%s, cidade=%s, uf=%s, email=%s WHERE external_id=%s;",
            (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, id),
        )
    else:
        try:
            pid = int(id)
            db_pg.execute(
                "UPDATE clientes SET cpf=%s, nome=%s, endereco=%s, cidade=%s, uf=%s, email=%s WHERE id=%s;",
                (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, pid),
            )
        except Exception:
            pass

    # update Mongo
    clientes.update_one({"idCliente": str(id)}, {"$set": c.dict()})

    # update Neo4j
    neo_run("MATCH (p:Person {id:$id}) SET p.cpf=$cpf, p.nome=$nome RETURN p", {"id": id, "cpf": c.cpf, "nome": c.nome})

    # rebuild and replicate
    consolidado = build_consolidated_for_client(id)
    if not consolidado:
        raise HTTPException(status_code=404, detail="cliente not found")
    replicate_client_to_redis(id, consolidado)
    return consolidado


@router.delete("/postgres/clientes/{id}", tags=["Clientes"], response_model=ConsolidatedCliente, summary="Delete a client from Postgres and replicate deletion")
def delete_postgres_cliente(id: str):
    consolidado = build_consolidated_for_client(id)
    if not consolidado:
        raise HTTPException(status_code=404, detail="cliente not found")

    # delete postgres
    if isinstance(id, str) and "-" in id:
        db_pg.execute("DELETE FROM clientes WHERE external_id=%s;", (id,))
    else:
        try:
            pid = int(id)
            db_pg.execute("DELETE FROM clientes WHERE id=%s;", (pid,))
        except Exception:
            pass

    # delete mongo
    clientes.delete_one({"idCliente": str(id)})
    # delete neo4j person
    neo_run("MATCH (p:Person {id:$id}) DETACH DELETE p", {"id": id})
    # delete redis
    redis_db.delete(f"cliente:{id}")

    return consolidado

class ProdutoIn(BaseModel):
    produto: str
    valor: Decimal
    quantidade: int
    tipo: Optional[str] = None

class Produto(ProdutoIn):
    id: int

@router.get("/produtos", response_model=List[Produto], tags=["Postgres - Produtos"], summary="List products from Postgres")
def list_produtos():
    return db_pg.query("SELECT * FROM public.produtos ORDER BY id;")

@router.get("/produtos/{id}", response_model=Produto, tags=["Postgres - Produtos"], summary="Get a product by id")
def get_produto(id: int):
    rows = db_pg.query("SELECT * FROM public.produtos WHERE id = %s", (id,))
    if not rows:
        raise HTTPException(status_code=404, detail="produto not found")
    return rows[0]

@router.post("/produtos", status_code=status.HTTP_201_CREATED, response_model=Produto, tags=["Postgres - Produtos"], summary="Create a new product")
def create_produto(p: ProdutoIn):
    res = db_pg.execute(
        "INSERT INTO public.produtos (produto, valor, quantidade, tipo) VALUES (%s, %s, %s, %s) RETURNING id;",
        (p.produto, p.valor, p.quantidade, p.tipo),
        returning=True,
    )
    if not res:
        raise HTTPException(status_code=500, detail="failed to create produto")
    new_id = res.get("id")
    new_row = db_pg.query("SELECT * FROM public.produtos WHERE id = %s", (new_id,))
    return new_row[0]

@router.put("/produtos/{id}", response_model=Produto, tags=["Postgres - Produtos"], summary="Update an existing product")
def update_produto(id: int, p: ProdutoIn):
    db_pg.execute(
        "UPDATE public.produtos SET produto=%s, valor=%s, quantidade=%s, tipo=%s WHERE id=%s;",
        (p.produto, p.valor, p.quantidade, p.tipo, id),
    )
    updated = db_pg.query("SELECT * FROM public.produtos WHERE id = %s", (id,))
    if not updated:
        raise HTTPException(status_code=404, detail="produto not found")
    return updated[0]

@router.delete("/produtos/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Postgres - Produtos"], summary="Delete a product")
def delete_produto(id: int):
    db_pg.execute("DELETE FROM public.produtos WHERE id=%s;", (id,))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- Mongo Clientes CRUD ---
class ClienteIn(BaseModel):
    idCliente: Optional[str] = None
    cpf: Optional[str] = None
    nome: Optional[str] = None
    endereco: Optional[str] = None
    cidade: Optional[str] = None
    uf: Optional[str] = None
    email: Optional[str] = None

# --- Profiles (Mongo) ---
class ProfileIn(BaseModel):
    idCliente: str
    idade: Optional[int] = None
    interesses: Optional[List[str]] = None

@router.get("/profiles", tags=["Mongo - Profiles"], summary="List profiles")
def list_profiles():
    return [p for p in profiles.find({})]

@router.get("/profiles/{id}", tags=["Mongo - Profiles"], summary="Get profile by id")
def get_profile(id: str):
    doc = profiles.find_one({"idCliente": str(id)})
    if not doc:
        raise HTTPException(status_code=404, detail="profile not found")
    return doc

@router.post("/profiles", status_code=status.HTTP_201_CREATED, tags=["Mongo - Profiles"], summary="Create a profile")
def create_profile(p: ProfileIn):
    profiles.insert_one(p.dict())
    return p.dict()

@router.put("/profiles/{id}", tags=["Mongo - Profiles"], summary="Update a profile")
def update_profile(id: str, p: ProfileIn):
    profiles.update_one({"idCliente": str(id)}, {"$set": p.dict()})
    doc = profiles.find_one({"idCliente": str(id)})
    if not doc:
        raise HTTPException(status_code=404, detail="profile not found")
    return doc

@router.delete("/profiles/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Mongo - Profiles"], summary="Delete a profile")
def delete_profile(id: str):
    profiles.delete_one({"idCliente": str(id)})
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _serialize(doc: dict):
    if not doc:
        return None
    d = dict(doc)
    _id = d.pop("_id", None)
    if _id is not None:
        d["_id"] = str(_id)
    return d

@router.get("/mongo/clientes", tags=["Clientes"], summary="List raw clients from Mongo (for debugging)")
def list_clientes():
    return [_serialize(d) for d in clientes.find({})]

@router.get("/clientes/{id}", tags=["Clientes"], response_model=ConsolidatedCliente, summary="Get client by id (prefer Redis consolidated view)")
def get_cliente_mongo(id: str):
    # prefer the Redis consolidated object; if missing, build and replicate
    data = redis_db.hget(f"cliente:{id}", "data")
    if data:
        return json.loads(data)

    consolidado = build_consolidated_for_client(id)
    if not consolidado:
        raise HTTPException(status_code=404, detail="cliente not found")

    replicate_client_to_redis(id, consolidado)
    return consolidado

@router.post("/clientes", status_code=status.HTTP_201_CREATED, tags=["Clientes"], response_model=ConsolidatedCliente, summary="Create a client across Postgres/Mongo/Neo4j and replicate to Redis")
def create_cliente(c: ClienteIn, background_tasks: BackgroundTasks):
    # generate a single UUID that will be used across Postgres, Mongo and Neo4j
    new_id = str(uuid4())

    # Postgres: insert with external_id to link across systems
    db_pg.execute(
        "INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email, external_id) VALUES (%s, %s, %s, %s, %s, %s, %s);",
        (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, new_id),
    )

    # Mongo: set idCliente to the generated external id and insert
    c_data = c.dict()
    c_data["idCliente"] = new_id
    doc = {"_id": new_id, **c_data}
    clientes.insert_one(doc)

    # Neo4j: ensure uniqueness constraint and create node
    neo_run("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;")
    neo_run("MERGE (p:Person {id:$id}) SET p.nome=$nome RETURN p", {"id": new_id, "nome": c_data.get("nome")})

    # Build consolidated object and replicate to Redis in background
    consolidado = build_consolidated_for_client(new_id)
    background_tasks.add_task(replicate_client_to_redis, new_id, consolidado)

    return consolidado
@router.put("/clientes/{id}", tags=["Clientes"], response_model=ConsolidatedCliente, summary="Update a client")
def update_cliente(id: str, c: ClienteIn):
    # update Postgres (by external_id or by numeric id)
    if isinstance(id, str) and "-" in id:
        db_pg.execute(
            "UPDATE clientes SET cpf=%s, nome=%s, endereco=%s, cidade=%s, uf=%s, email=%s WHERE external_id=%s;",
            (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, id),
        )
    else:
        try:
            pid = int(id)
            db_pg.execute(
                "UPDATE clientes SET cpf=%s, nome=%s, endereco=%s, cidade=%s, uf=%s, email=%s WHERE id=%s;",
                (c.cpf, c.nome, c.endereco, c.cidade, c.uf, c.email, pid),
            )
        except Exception:
            pass

    # update Mongo
    clientes.update_one({"idCliente": str(id)}, {"$set": c.dict()})

    # update Neo4j
    neo_run("MATCH (p:Person {id:$id}) SET p.cpf=$cpf, p.nome=$nome RETURN p", {"id": id, "cpf": c.cpf, "nome": c.nome})

    # build consolidated and replicate to Redis
    consolidado = build_consolidated_for_client(id)
    if not consolidado:
        raise HTTPException(status_code=404, detail="cliente not found")

    replicate_client_to_redis(id, consolidado)
    return consolidado

@router.delete("/clientes/{id}", tags=["Clientes"], response_model=ConsolidatedCliente, summary="Delete a client")
def delete_cliente(id: str):
    # fetch consolidated (to return to caller) before deletion
    consolidado = build_consolidated_for_client(id)
    if not consolidado:
        raise HTTPException(status_code=404, detail="cliente not found")

    # delete Postgres (by external_id or id)
    if isinstance(id, str) and "-" in id:
        db_pg.execute("DELETE FROM clientes WHERE external_id=%s;", (id,))
    else:
        try:
            pid = int(id)
            db_pg.execute("DELETE FROM clientes WHERE id=%s;", (pid,))
        except Exception:
            pass

    # delete Mongo
    clientes.delete_one({"idCliente": str(id)})

    # delete Neo4j node
    neo_run("MATCH (p:Person {id:$id}) DETACH DELETE p", {"id": id})

    # delete Redis key
    redis_db.delete(f"cliente:{id}")

    return consolidado


# --- Neo4j endpoints ---
from ..db.neo4j import run_query as neo_run


def _serialize_neo(value):
    # Convert neo4j Node or Relationship to dict
    try:
        return dict(value)
    except Exception:
        return value


# Produtos (Neo4j)
class NeoProdutoIn(BaseModel):
    id: Optional[int] = None
    produto: str
    valor: str

class NeoProduto(NeoProdutoIn):
    id: int

@router.get("/neo4j/produtos", tags=["Neo4j - Produtos"], summary="List products in Neo4j")
def neo_list_produtos():
    rows = neo_run("MATCH (p:Produto) RETURN p")
    return [ _serialize_neo(r["p"]) for r in rows ]

@router.get("/neo4j/produtos/{id}", tags=["Neo4j - Produtos"], summary="Get a product from Neo4j by id")
def neo_get_produto(id: int):
    rows = neo_run("MATCH (p:Produto {id:$id}) RETURN p LIMIT 1", {"id": id})
    if not rows:
        raise HTTPException(status_code=404, detail="produto not found")
    return _serialize_neo(rows[0]["p"])

@router.post("/neo4j/produtos", status_code=status.HTTP_201_CREATED, tags=["Neo4j - Produtos"], summary="Create a product in Neo4j")
def neo_create_produto(p: NeoProdutoIn):
    # allow explicit id (from seed) or auto-generate
    if p.id is not None:
        rows = neo_run(
            "MERGE (p:Produto {id:$id}) SET p.produto=$produto, p.valor=$valor RETURN p",
            {"id": p.id, "produto": p.produto, "valor": p.valor}
        )
        return _serialize_neo(rows[0]["p"]) if rows else {}

    # get max id
    mx = neo_run("MATCH (p:Produto) RETURN max(p.id) AS max_id")
    max_id = mx[0].get("max_id") if mx and mx[0].get("max_id") is not None else 0
    new_id = int(max_id) + 1
    rows = neo_run(
        "CREATE (p:Produto {id:$id, produto:$produto, valor:$valor}) RETURN p",
        {"id": new_id, "produto": p.produto, "valor": p.valor}
    )
    return _serialize_neo(rows[0]["p"]) if rows else {}

    new_id = int(max_id) + 1
    rows = neo_run(
        "CREATE (p:Produto {id:$id, produto:$produto, valor:$valor}) RETURN p",
        {"id": new_id, "produto": p.produto, "valor": p.valor}
    )
    return _serialize_neo(rows[0]["p"]) if rows else {}

@router.put("/neo4j/produtos/{id}", tags=["Neo4j - Produtos"], summary="Update a product in Neo4j")
def neo_update_produto(id: int, p: NeoProdutoIn):
    rows = neo_run(
        "MATCH (p:Produto {id:$id}) SET p.produto=$produto, p.valor=$valor RETURN p",
        {"id": id, "produto": p.produto, "valor": p.valor}
    )
    if not rows:
        raise HTTPException(status_code=404, detail="produto not found")
    return _serialize_neo(rows[0]["p"])

@router.delete("/neo4j/produtos/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Neo4j - Produtos"], summary="Delete a product in Neo4j")
def neo_delete_produto(id: int):
    res = neo_run("MATCH (p:Produto {id:$id}) WITH count(p) AS c, p DETACH DELETE p RETURN c", {"id": id})
    deleted = res[0].get("c") if res else 0
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# Persons and friendships
class PersonIn(BaseModel):
    id: str
    nome: str

@router.get("/neo4j/persons", tags=["Neo4j - Persons"], summary="List persons in Neo4j")
def neo_list_persons():
    rows = neo_run("MATCH (p:Person) RETURN p")
    return [ _serialize_neo(r["p"]) for r in rows ]

@router.get("/neo4j/persons/{id}", tags=["Neo4j - Persons"], summary="Get a person by id")
def neo_get_person(id: str):
    rows = neo_run("MATCH (p:Person {id:$id}) RETURN p LIMIT 1", {"id": id})
    if not rows:
        raise HTTPException(status_code=404, detail="person not found")
    return _serialize_neo(rows[0]["p"])

@router.post("/neo4j/persons/{id}/purchase", tags=["Neo4j - Persons"], summary="Record a purchase made by a person; if person is not a client, create client and record purchase")
def neo_person_purchase(id: str, payload: dict):
    """payload should contain: {"id_produto": int, "data": "YYYY-MM-DD" (optional)}"""
    # Check person exists
    rows = neo_run("MATCH (p:Person {id:$id}) RETURN p LIMIT 1", {"id": id})
    if not rows:
        raise HTTPException(status_code=404, detail="person not found")
    # Delegate to /compras logic by building CompraIn-like dict
    compra = {"id_produto": payload.get("id_produto"), "id_cliente": id, "data": payload.get("data")}
    # Reuse create_compra
    return create_compra(CompraIn(**compra))

@router.post("/neo4j/persons", status_code=status.HTTP_201_CREATED, tags=["Neo4j - Persons"], summary="Create a person")
def neo_create_person(p: PersonIn):
    rows = neo_run("CREATE (p:Person {id:$id, nome:$nome}) RETURN p", {"id": p.id, "nome": p.nome})
    return _serialize_neo(rows[0]["p"]) if rows else {}

@router.put("/neo4j/persons/{id}", tags=["Neo4j - Persons"], summary="Update a person")
def neo_update_person(id: str, p: PersonIn):
    rows = neo_run("MATCH (p:Person {id:$id}) SET p.nome=$nome RETURN p", {"id": id, "nome": p.nome})
    if not rows:
        raise HTTPException(status_code=404, detail="person not found")
    return _serialize_neo(rows[0]["p"])

@router.delete("/neo4j/persons/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["Neo4j - Persons"], summary="Delete a person")
def neo_delete_person(id: str):
    res = neo_run("MATCH (p:Person {id:$id}) WITH count(p) AS c, p DETACH DELETE p RETURN c", {"id": id})
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.post("/neo4j/persons/{id}/friend/{friend_id}", tags=["Neo4j - Persons"], summary="Add friend relationship")
def neo_add_friend(id: str, friend_id: str):
    rows = neo_run(
        "MATCH (a:Person {id:$id}), (b:Person {id:$friend_id}) MERGE (a)-[:FRIEND]->(b) RETURN a, b",
        {"id": id, "friend_id": friend_id}
    )
    if not rows:
        raise HTTPException(status_code=404, detail="one or both persons not found")
    return {"status": "friend added"}

@router.delete("/neo4j/persons/{id}/friend/{friend_id}", tags=["Neo4j - Persons"], summary="Remove friend relationship")
def neo_remove_friend(id: str, friend_id: str):
    res = neo_run(
        "MATCH (a:Person {id:$id})-[r:FRIEND]->(b:Person {id:$friend_id}) WITH count(r) AS c DELETE r RETURN c",
        {"id": id, "friend_id": friend_id}
    )
    removed = res[0].get("c") if res else 0
    return {"removed": removed}