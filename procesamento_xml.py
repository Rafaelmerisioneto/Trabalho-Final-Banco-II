import xml.etree.ElementTree as ET

# ---
# 1. FUNÇÃO AUXILIAR PARA CARREGAR QUALQUER ARQUIVO XML
# ---

def carregar_xml_para_dict(nome_arquivo, nome_elemento_raiz, chave_principal):
    """
    Carrega dados de um arquivo XML e retorna um dicionário 
    onde a chave é o valor do campo especificado (chave_principal).
    """
    dados_dict = {}
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        
        for elemento in root.findall(nome_elemento_raiz):
            # Cria um dicionário com todos os subelementos (tag e valor)
            item_data = {
                sub_elem.tag: sub_elem.text.strip()
                for sub_elem in elemento
            }
            
            # Usa o valor da chave principal como a chave do dicionário principal
            chave = item_data.get(chave_principal)
            if chave:
                dados_dict[chave] = item_data
            
        print(f"✅ Dados de '{nome_arquivo}' carregados com sucesso. Total de itens: {len(dados_dict)}")
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo '{nome_arquivo}' não encontrado.")
    except Exception as e:
        print(f"❌ ERRO ao processar '{nome_arquivo}': {e}")
        
    return dados_dict

# ---
# 2. CARREGAMENTO DOS ARQUIVOS DE DOMÍNIO
# ---

# Carregar dados de PEÇAS (Chave: Cod_Peca)
pecas_dict = carregar_xml_para_dict(
    "fornecedor.zip/peca.xml", 
    "peca", 
    "Cod_Peca"
)

# Carregar dados de FORNECEDORES (Chave: Cod_Fornec)
fornecedores_dict = carregar_xml_para_dict(
    "fornecedor.zip/fornecedor.xml", 
    "fornecedor", 
    "Cod_Fornec"
)

# Carregar dados de PROJETOS (Chave: Cod_Proj)
projetos_dict = carregar_xml_para_dict(
    "fornecedor.zip/projeto.xml", 
    "projeto", 
    "Cod_Proj"
)


# ---
# 3. CARREGAMENTO DOS DADOS DE RELAÇÃO (FORNECIMENTO)
# ---

def carregar_fornecimento(nome_arquivo):
    """
    Carrega os dados de fornecimento em uma lista de dicionários.
    """
    fornecimentos_list = []
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        
        for fornecimento in root.findall("fornecimento"):
            # Cria um dicionário para cada registro de fornecimento
            item_data = {
                sub_elem.tag: sub_elem.text.strip()
                for sub_elem in fornecimento
            }
            # Converte a quantidade para inteiro, importante para somas futuras
            try:
                item_data['Quantidade'] = int(item_data['Quantidade'])
            except ValueError:
                print(f"Aviso: Quantidade inválida em um registro de fornecimento.")

            fornecimentos_list.append(item_data)
            
        print(f"✅ Dados de '{nome_arquivo}' carregados com sucesso. Total de registros: {len(fornecimentos_list)}")
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo '{nome_arquivo}' não encontrado.")
    except Exception as e:
        print(f"❌ ERRO ao processar '{nome_arquivo}': {e}")
        
    return fornecimentos_list

fornecimentos_list = carregar_fornecimento("fornecedor.zip/fornecimento.xml")


# ---
# 4. EXEMPLO DE CONSULTA (LISTAR TODOS OS FORNECIMENTOS)
# ---

print("\n--- Exemplo de Consulta: Listar Fornecimentos e Detalhes ---")
for f in fornecimentos_list:
    cod_f = f['Cod_Fornec']
    cod_p = f['Cod_Peca']
    cod_j = f['Cod_Proj']
    qtde = f['Quantidade']
    
    # Busca o nome do Fornecedor, Peça e Projeto nos dicionários de lookup
    nome_fornec = fornecedores_dict.get(cod_f, {}).get('FNome', 'N/A')
    nome_peca = pecas_dict.get(cod_p, {}).get('PNome', 'N/A')
    nome_proj = projetos_dict.get(cod_j, {}).get('Jnome', 'N/A')
    
    print(f"Fornecedor: {nome_fornec} ({cod_f}) | Peça: {nome_peca} ({cod_p}) | Projeto: {nome_proj} ({cod_j}) | Qtde: {qtde}")


# ---
# 5. ESTRUTURAS PRONTAS PARA SUAS CONSULTAS
# ---
print("\nVariáveis prontas para uso nas suas consultas:")
print("* `pecas_dict` (Dicionário de Peças)")
print("* `fornecedores_dict` (Dicionário de Fornecedores)")
print("* `projetos_dict` (Dicionário de Projetos)")
print("* `fornecimentos_list` (Lista de Registros de Fornecimento)")