import xml.etree.ElementTree as ET
import psycopg2

## ⚙️ 1. Configuração e Conexão com PostgreSQL
# ---

# Tente usar uma biblioteca mais atual como 'psycopg' para novos projetos, 
# mas manteremos 'psycopg2' pois é o que você estava usando.
try:
    conn = psycopg2.connect(
        database="postgres",
        host="postgres",
        user="postgres",
        password="senha12345",
        port="5433"
    )
    print("Conexão com PostgreSQL estabelecida com sucesso.")
except psycopg2.Error as e:
    print(f"Erro ao conectar ao PostgreSQL: {e}")
    # Se a conexão falhar, o script deve parar aqui
    exit() 

sql_peca = "SELECT codigo, pnome, cor, peso, cidade FROM peca"
cursor = conn.cursor()
cursor.execute(sql_peca)
rows = cursor.fetchall()


## 🛠️ 2. Processamento dos Dados do PostgreSQL
# ---

# Cria um dicionário para acesso rápido aos dados da peça pelo Código (Chave).
# Isso é essencial para cruzar com os dados do XML eficientemente.
pecas_dict = {}

print("\n--- Dados da Tabela 'peca' (PG) ---")
for row in rows:
    # row[0] é o 'codigo', que será a chave do dicionário
    codigo = str(row[0]).strip()
    
    # Imprimindo os valores lidos (igual ao seu exemplo anterior)
    pnome = str(row[1]).strip()
    cor = str(row[2]).strip()
    peso = str(row[3]).strip()
    cidade = str(row[4]).strip()
    print(f"{codigo} {pnome} {cor} {peso} {cidade}")

    # Armazena os dados no dicionário usando o código como chave
    pecas_dict[codigo] = {
        'pnome': pnome,
        'cor': cor,
        'peso': peso,
        'cidade': cidade
    }

# Fecha o cursor e a conexão com o banco de dados, 
# pois não precisaremos mais dele para o processamento do XML.
cursor.close()
conn.close()
print("Conexão com PostgreSQL fechada.")


## 📄 3. Leitura e Processamento do Arquivo XML
# ---

try:
    tree = ET.parse("Fornecimento.xml")
    root = tree.getroot()
except FileNotFoundError:
    print("\nERRO: Arquivo 'Fornecimento.xml' não encontrado.")
    exit()

# Opcional: Estrutura para armazenar todos os dados do XML, 
# caso as consultas futuras sejam complexas no lado do XML.
fornecimentos_list = []

print("\n--- Dados do Arquivo 'Fornecimento.xml' ---")
for f in root.findall("row"):
    cod = f.find("codigo").text.strip()
    codf = f.find("cod_fornec").text.strip()
    codp = f.find("cod_peca").text.strip()
    codj = f.find("cod_proj").text.strip()
    qtde = f.find("quantidade").text.strip()
    val = f.find("valor").text.strip()
    
    # Armazena os dados do fornecimento em uma lista de dicionários
    fornecimentos_list.append({
        'cod': cod,
        'codf': codf,
        'codp': codp,
        'codj': codj,
        'qtde': int(qtde),  # Converte para inteiro para cálculos futuros
        'val': float(val)   # Converte para float para cálculos futuros
    })
    
    # Imprimindo os valores lidos (igual ao seu exemplo anterior)
    print(f"{codf} {codp} {codj} {qtde} {val}")


## 🤝 4. Exemplo de Cruzamento de Dados (PG + XML)
# ---

print("\n--- Exemplo de Cruzamento (PG e XML) ---")
# Objetivo: Listar Fornecimento + Nome da Peça (obtido do PostgreSQL)

for fornecimento in fornecimentos_list:
    cod_peca = fornecimento['codp']
    quantidade = fornecimento['qtde']
    
    # Usa o dicionário 'pecas_dict' para buscar a informação do PG de forma rápida
    info_peca = pecas_dict.get(cod_peca)
    
    if info_peca:
        nome_peca = info_peca['pnome']
        cidade_peca = info_peca['cidade']
        print(f"Fornecimento Cód Peca: {cod_peca} | Nome: {nome_peca} | Quantidade: {quantidade} | Cidade da Peça: {cidade_peca}")
    else:
        print(f"Fornecimento Cód Peca: {cod_peca} | AVISO: Peça não encontrada no PostgreSQL.")