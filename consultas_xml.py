import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

# =================================================================
# BLOCO 1: FUNÇÕES DE CARREGAMENTO E ESTRUTURAÇÃO DE DADOS
# =================================================================

def carregar_xml_para_dict(nome_arquivo, nome_elemento_raiz, chave_principal):
    """
    Carrega dados de um arquivo XML para um dicionário de acesso rápido 
    usando a chave_principal (ex: Cod_Peca).
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
            chave = item_data.get(chave_principal)
            if chave:
                dados_dict[chave] = item_data
            
        print(f"✅ Dados de '{nome_arquivo}' carregados. Total: {len(dados_dict)}")
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo '{nome_arquivo}' não encontrado.")
    except Exception as e:
        print(f"❌ ERRO ao processar '{nome_arquivo}': {e}")
        
    return dados_dict

def carregar_fornecimento(nome_arquivo):
    """
    Carrega os dados de fornecimento em uma lista de dicionários, 
    convertendo 'Quantidade' para inteiro.
    """
    fornecimentos_list = []
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        
        for fornecimento in root.findall("fornecimento"):
            item_data = {
                sub_elem.tag: sub_elem.text.strip()
                for sub_elem in fornecimento
            }
            # Converte a quantidade para inteiro
            try:
                item_data['Quantidade'] = int(item_data['Quantidade'])
            except (ValueError, TypeError):
                item_data['Quantidade'] = 0 

            # Inclui atributos na leitura, se existirem (útil após consulta b)
            if fornecimento.attrib:
                 item_data.update(fornecimento.attrib)
                 
            fornecimentos_list.append(item_data)
            
        print(f"✅ Dados de '{nome_arquivo}' carregados. Total: {len(fornecimentos_list)}")
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo '{nome_arquivo}' não encontrado.")
        
    return fornecimentos_list

# =================================================================
# BLOCO 2: FUNÇÕES DE CONSULTA (A a J)
# =================================================================

# --- Consultas de Leitura (Internas) ---

def consulta_a():
    """a) Retornar os dados da penúltima peça da árvore XML."""
    print("\n--- a) Dados da Penúltima Peça (peca.xml) ---")
    try:
        tree = ET.parse("fornecedor/peca.xml")
        root = tree.getroot()
        pecas = root.findall("peca")
        
        if len(pecas) >= 2:
            penultima_peca = pecas[-2]
            print(f"Penúltima Peça Encontrada:")
            for sub_elem in penultima_peca:
                print(f"  {sub_elem.tag}: {sub_elem.text.strip()}")
        else:
            print("Aviso: Menos de duas peças encontradas no XML.")
    except Exception as e:
        print(f"Erro na consulta A: {e}")

def consulta_d(pecas_dict):
    """d) Retornar o código, a cidade e cor de todas as peças."""
    print("\n--- d) Código, Cidade e Cor das Peças ---")
    for codigo, dados in pecas_dict.items():
        cor = dados.get('Cor', 'N/A')
        cidade = dados.get('Cidade', 'N/A')
        print(f"Código: {codigo} | Cor: {cor} | Cidade: {cidade}")

def consulta_e(fornecimentos_list):
    """e) Obter o somatório das quantidades dos fornecimentos."""
    soma_quantidades = sum(f.get('Quantidade', 0) for f in fornecimentos_list)
    print("\n--- e) Somatório das Quantidades de Fornecimento ---")
    print(f"Somatório Total: {soma_quantidades}")

def consulta_f(projetos_dict):
    """f) Obter os nomes dos projetos de Paris."""
    nomes_projetos_paris = []
    for _, dados in projetos_dict.items():
        cidade = dados.get('Cidade', '').strip().upper()
        if cidade == "PARIS":
            nomes_projetos_paris.append(dados.get('Jnome'))
            
    print("\n--- f) Nomes dos Projetos de Paris ---")
    if nomes_projetos_paris:
        print(", ".join(nomes_projetos_paris))
    else:
        print("Nenhum projeto encontrado em Paris.")

def consulta_g(fornecimentos_list):
    """g) Obter o código dos fornecedores que forneceram pecas em maior quantidade."""
    quantidade_por_fornecedor = defaultdict(int)
    for f in fornecimentos_list:
        cod_f = f.get('Cod_Fornec')
        qtde = f.get('Quantidade', 0)
        quantidade_por_fornecedor[cod_f] += qtde
        
    print("\n--- g) Código do(s) Fornecedor(es) de Maior Quantidade ---")
    if not quantidade_por_fornecedor:
        print("Nenhum fornecimento registrado.")
        return

    max_quantidade = max(quantidade_por_fornecedor.values())
    fornecedores_max = [
        cod for cod, qtde in quantidade_por_fornecedor.items() 
        if qtde == max_quantidade
    ]
    
    print(f"Maior Quantidade Fornecida (Total): {max_quantidade}")
    print(f"Código(s) do(s) Fornecedor(es): {', '.join(fornecedores_max)}")

def consulta_i(pecas_dict, fornecimentos_list):
    """i) Obter os nomes das peças e seus dados de fornecimento."""
    print("\n--- i) Peças e Seus Fornecimentos ---")
    
    fornecimentos_por_peca = defaultdict(list)
    for f in fornecimentos_list:
        fornecimentos_por_peca[f['Cod_Peca']].append(f)
        
    for cod_peca, lista_fornec in fornecimentos_por_peca.items():
        nome_peca = pecas_dict.get(cod_peca, {}).get('PNome', 'NOME INDISPONÍVEL')
        
        print(f"\nPEÇA: {nome_peca} (Código: {cod_peca})")
        for f in lista_fornec:
            data = f.get('Data', 'N/A') # Inclui a data se foi inserida pela consulta b
            print(f"  - Fornecedor: {f['Cod_Fornec']} | Projeto: {f['Cod_Proj']} | Quantidade: {f['Quantidade']} | Data: {data}")

def consulta_j(pecas_dict):
    """j) Obter o preço médio das peças."""
    soma_precos = 0.0
    contagem = 0
    
    for _, dados in pecas_dict.items():
        try:
            preco = float(dados.get('Preco', 0.0))
            soma_precos += preco
            contagem += 1
        except ValueError:
            continue
            
    print("\n--- j) Preço Médio das Peças ---")
    if contagem > 0:
        preco_medio = soma_precos / contagem
        print(f"Preço Médio: R$ {preco_medio:.2f}")
    else:
        print("Nenhuma peça com preço válido encontrada para calcular a média.")

# --- Consultas de Modificação (Escrevem no XML) ---

def consulta_b():
    """b) Inserir um atributo com a data em todos os fornecimentos."""
    print("\n--- b) INSERÇÃO de Atributo (Modifica XML) ---")
    nome_arquivo = "fornecedor/fornecimento.xml"
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        data_atual = datetime.now().strftime("%Y-%m-%d")
        modificacoes = 0
        
        for fornecimento in root.findall("fornecimento"):
            fornecimento.set('Data', data_atual)
            modificacoes += 1
            
        tree.write(nome_arquivo, encoding="utf-8", xml_declaration=True)
        print(f"✅ {modificacoes} registros de fornecimento atualizados com Data='{data_atual}'.")
        
    except Exception as e:
        print(f"Erro na consulta B: {e}")

def consulta_c():
    """c) Atualizar o status dos fornecedores de Londres para 50."""
    print("\n--- c) ATUALIZAÇÃO de Status (Modifica XML) ---")
    nome_arquivo = "fornecedor/fornecedor.xml"
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        modificacoes = 0
        
        for fornecedor in root.findall("fornecedor"):
            cidade = fornecedor.find("Cidade")
            if cidade is not None and cidade.text.strip().upper() == "LONDRES":
                status = fornecedor.find("Status")
                if status is not None:
                    status.text = "50"
                    modificacoes += 1
            
        tree.write(nome_arquivo, encoding="utf-8", xml_declaration=True)
        print(f"✅ {modificacoes} fornecedores em Londres tiveram o Status atualizado para 50.")
        
    except Exception as e:
        print(f"Erro na consulta C: {e}")

def consulta_h():
    """h) Excluir os projetos da cidade de Atenas."""
    print("\n--- h) EXCLUSÃO de Projetos (Modifica XML) ---")
    nome_arquivo = "fornecedor/projeto.xml"
    try:
        tree = ET.parse(nome_arquivo)
        root = tree.getroot()
        excluidos = 0
        
        for projeto in root.findall("projeto"):
            cidade = projeto.find("Cidade")
            if cidade is not None and cidade.text.strip().upper() == "ATENAS":
                root.remove(projeto)
                excluidos += 1
            
        tree.write(nome_arquivo, encoding="utf-8", xml_declaration=True)
        print(f"✅ {excluidos} projetos da cidade de Atenas foram excluídos.")
        
    except Exception as e:
        print(f"Erro na consulta H: {e}")


# =================================================================
# BLOCO 3: EXECUÇÃO PRINCIPAL
# =================================================================

def main():
    print("=====================================================")
    print("INICIANDO PROCESSAMENTO E CONSULTAS XML")
    print("=====================================================")
    
    # 1. Carregamento Inicial dos Dados
    # ------------------------------------
    
    # Carregamos APENAS os dados XML, ignorando a conexão com o PostgreSQL
    # que não é mais necessária para estas consultas.
    
    pecas_dict = carregar_xml_para_dict(
        "fornecedor/peca.xml",  # <- Caminho corrigido
        "peca", 
        "Cod_Peca"
)
    fornecedores_dict = carregar_xml_para_dict(
        "fornecedor/fornecedor.xml", 
        "fornecedor", 
        "Cod_Fornec"
    )
    projetos_dict = carregar_xml_para_dict(
        "fornecedor/projeto.xml", 
        "projeto", 
        "Cod_Proj"
    )
    # A lista de fornecimentos é carregada por último
    fornecimentos_list = carregar_fornecimento("fornecedor/fornecimento.xml")

    print("\n=====================================================")
    print("EXECUTANDO CONSULTAS EM SEQUÊNCIA (A-J)")
    print("=====================================================")
    
    # 2. Execução das Consultas na Ordem
    # ------------------------------------
    
    # Execução das consultas que NÃO modificam o XML (apenas leitura/cálculo)
    consulta_a()
    consulta_d(pecas_dict)
    consulta_e(fornecimentos_list)
    consulta_f(projetos_dict)
    consulta_g(fornecimentos_list)
    consulta_i(pecas_dict, fornecimentos_list)
    consulta_j(pecas_dict)

    # Execução das consultas que MODIFICAM o XML
    # ATENÇÃO: Essas funções reescrevem os arquivos.
    consulta_b()
    consulta_c()
    consulta_h()
    
    print("\n=====================================================")
    print("EXECUÇÃO CONCLUÍDA")
    print("=====================================================")

if __name__ == "__main__":
    main()