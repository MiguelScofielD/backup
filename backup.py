import subprocess
import os
from datetime import datetime

# Configurações dos bancos de dados e backup
DATABASES = [
    {"name": "banco1", "user": "postgres", "host": "localhost", "port": "5432"},
    {"name": "banco2", "user": "postgres", "host": "localhost", "port": "5432"},
    # Adicione mais bancos de dados conforme necessário
]

BACKUP_DIR = "C:/Users/antdp/Downloads/backup"

def backup_database(db_name, user, host, port):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(BACKUP_DIR, f"{db_name}_{timestamp}.sql")

    # Atualize o caminho para pg_dump
    pg_dump_path = "C:/Program Files/PostgreSQL/15/bin/pg_dump.exe"

    # Construa o comando
    cmd = [
        pg_dump_path,
        "-U", user,
        "-h", host,
        "-p", port,
        "-F", "c",
        "-f", backup_file,
        db_name
    ]
    
    # Define a variável de ambiente para a senha
    env = os.environ.copy()
    env["PGPASSWORD"] = "postgres"
    
    # Exibir o comando para debugging
    print(f"Executando comando: {' '.join(cmd)}")
    
    # Execute o comando e capture a saída de erro
    result = subprocess.run(cmd, env=env, text=True, capture_output=True)
    
    # Exibir o código de retorno e mensagem de erro para debugging
    if result.returncode == 0:
        print(f"Backup do banco {db_name} concluído com sucesso.")
    else:
        print(f"Erro ao fazer backup do banco {db_name}: Código de erro {result.returncode}")
        print(f"Saída de erro: {result.stderr}")

# Cria o diretório de backup se não existir
os.makedirs(BACKUP_DIR, exist_ok=True)

for db in DATABASES:
    backup_database(db["name"], db["user"], db["host"], db["port"])
