import os
import psycopg2
from psycopg2 import sql
import csv
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re
import uuid

# Dados de acesso ao banco de dados
DB_HOST = "jsm-prd-all-apps-cluster.cluster-cqgdocww9p8f.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "pentagon_prd"
DB_USER = "---"
DB_PASSWORD = "--"

# Credenciais de acesso ao Django
DJANGO_USER = "---"
DJANGO_PASSWORD = "---"
DJANGO_LOGIN_URL = 'https://duomo.juntossomosmaisi.com.br/admin/login/?next=/admin/'
DJANGO_IMPORT_ITEM_URL = 'https://duomo.juntossomosmaisi.com.br/admin/core/item/'
DJANGO_IMPORT_URL = 'https://duomo.juntossomosmaisi.com.br/admin/core/item/import/'
DJANGO_PROCESS_IMPORT_URL = 'https://duomo.juntossomosmaisi.com.br/admin/core/item/process_import/'

def get_points_balance(participant_id):
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Query para saldo de pontos
        query = sql.SQL("""
            SELECT 
            SUM(points - used_points) AS points 
            FROM engine_customerlot ecl
            LEFT JOIN engine_customer ec ON ec.id = ecl.customer_id
            WHERE ec.external_id = %s
            AND point_expired_date > NOW()
        """)

        cursor.execute(query, (participant_id,))
        result = cursor.fetchone()

        # Fechar conexão
        cursor.close()
        conn.close()

        if result and result[0] is not None:
            return result[0]
        else:
            raise ValueError(f"Não foram encontrados pontos para o proprietário {participant_id}.")

    except Exception as e:
        raise e

def create_csv(old_points_balance, old_proprietario_cpf, new_proprietario_cpf, identifier):
    # Obter o mês e ano atuais
    now = datetime.now()
    month = now.strftime('%m')
    year = now.strftime('%Y')

    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')
    csv_filename = f'{identifier}.csv'
    csv_path = os.path.join(desktop, csv_filename)
    
    try:
        with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(["month", "year", "id", "lot", "cpf", "points", "event_type", "sponsor_name", "details", "lot_slug", "identifier"])
            writer.writerow([month, year, "", f"estorno_jsm_{month}_{year}", old_proprietario_cpf, old_points_balance, "bonus", "JS+ Varejo", "Migração de pontos", "free-points-retail", identifier])
            writer.writerow([month, year, "", f"debit_jsm_{month}_{year}", new_proprietario_cpf, -old_points_balance, "debitPoints", "JS+ Varejo", "Migração de pontos", "free-points-retail", identifier])

        print(f"Arquivo CSV '{csv_filename}' criado com sucesso na área de trabalho.")
        return csv_path

    except Exception as e:
        raise e

def import_csv_to_django(csv_path):
    session = requests.Session()

    try:
        # Acessar a página de login para obter o token CSRF
        login_page_response = session.get(DJANGO_LOGIN_URL)
        if login_page_response.status_code != 200:
            raise ValueError(f"Falha ao acessar a página de login. Status Code: {login_page_response.status_code}")
        
        soup = BeautifulSoup(login_page_response.text, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
        
        # Dados de login
        login_data = {
            'username': DJANGO_USER,
            'password': DJANGO_PASSWORD,
            'csrfmiddlewaretoken': csrf_token
        }
        
        # Login para obter os cookies de sessão
        login_response = session.post(DJANGO_LOGIN_URL, data=login_data, headers={
            'Referer': DJANGO_LOGIN_URL,
            'X-CSRFToken': csrf_token
        })
        
        if login_response.status_code != 200:
            raise ValueError(f"Falha no login. Status Code: {login_response.status_code}")

        # Acessar a página do item para garantir que o acesso está autorizado
        import_item_page_response = session.get(DJANGO_IMPORT_ITEM_URL)
        if import_item_page_response.status_code != 200:
            raise ValueError(f"Falha ao acessar a página do item. Status Code: {import_item_page_response.status_code}")

        # Obter o token CSRF da página de importação
        import_page_response = session.get(DJANGO_IMPORT_URL)
        if import_page_response.status_code != 200:
            raise ValueError(f"Falha ao acessar a página de importação. Status Code: {import_page_response.status_code}")
        
        soup = BeautifulSoup(import_page_response.text, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']

        # Preparar arquivo para upload
        files = {'import_file': open(csv_path, 'rb')}
        headers = {
            'X-CSRFToken': csrf_token,
            'Referer': DJANGO_IMPORT_URL
        }
        response = session.post(DJANGO_IMPORT_URL, files=files, data={'input_format': '0'}, headers=headers)
        
        if response.status_code != 200:
            raise ValueError(f"Falha na importação do arquivo CSV. Status Code: {response.status_code}")
        
        # Verificar o conteúdo da resposta para obter o formulário de confirmação
        import_response_text = response.text
        print("Conteúdo da resposta de importação:", import_response_text)  # Adicione esta linha
        
        soup = BeautifulSoup(import_response_text, 'html.parser')
        csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
        confirm_data = {
            'csrfmiddlewaretoken': csrf_token
        }
        
        # Processar a importação
        process_import_response = session.post(DJANGO_PROCESS_IMPORT_URL, data=confirm_data, headers={
            'X-CSRFToken': csrf_token
        })
        
        # Imprimir resposta de erro para depuração
        if process_import_response.status_code == 200:
            print("Arquivo CSV importado e processado com sucesso.")
        else:
            print(f"Status Code ao processar a importação: {process_import_response.status_code}")
            print("Resposta ao processar a importação:", process_import_response.text)
            raise ValueError(f"Falha ao processar a importação do arquivo CSV. Status Code: {process_import_response.status_code}")

    except Exception as e:
        print(f"Ocorreu um erro ao importar o arquivo CSV: {e}")

def validate_uuid(val):
    try:
        uuid_obj = uuid.UUID(val)
        return str(uuid_obj) == val
    except ValueError:
        return False

def clean_cpf(cpf):
    return re.sub(r'\D', '', cpf)

def validate_cpf(cpf):
    cpf = clean_cpf(cpf)
    return len(cpf) == 11 and cpf.isdigit()

if __name__ == "__main__":
    while True:
        try:
            old_proprietario_id = input("Por favor, insira o Id do proprietário antigo: ")
            while not validate_uuid(old_proprietario_id):
                print("O Id do proprietário antigo está incorreto. Deve estar no formato UUID.")
                old_proprietario_id = input("Por favor, insira o Id do proprietário antigo: ")

            new_proprietario_id = input("Por favor, insira o Id do novo proprietário: ")
            while not validate_uuid(new_proprietario_id):
                print("O Id do novo proprietário está incorreto. Deve estar no formato UUID.")
                new_proprietario_id = input("Por favor, insira o Id do novo proprietário: ")

            identifier = input("Por favor, insira o número do chamado de origem (identifier): ")

            old_proprietario_cpf = input("Por favor, insira o CPF do proprietário antigo: ")
            old_proprietario_cpf = clean_cpf(old_proprietario_cpf)
            while not validate_cpf(old_proprietario_cpf):
                print("O CPF inserido está incorreto. Deve ter 11 dígitos.")
                old_proprietario_cpf = clean_cpf(input("Por favor, insira o CPF do proprietário antigo: "))

            new_proprietario_cpf = input("Por favor, insira o CPF do novo proprietário: ")
            new_proprietario_cpf = clean_cpf(new_proprietario_cpf)
            while not validate_cpf(new_proprietario_cpf):
                print("O CPF inserido está incorreto. Deve ter 11 dígitos.")
                new_proprietario_cpf = clean_cpf(input("Por favor, insira o CPF do novo proprietário: "))

            old_points_balance = get_points_balance(old_proprietario_id)
            csv_path = create_csv(old_points_balance, old_proprietario_cpf, new_proprietario_cpf, identifier)
            import_csv_to_django(csv_path)
            break

        except Exception as e:
            print(f"Ocorreu um erro: {e}")