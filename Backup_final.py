# -*- coding: utf-8 -*-

from PyQt5.QtCore import QCoreApplication
from qgis.core import (QgsProcessing, QgsProcessingException, QgsProcessingAlgorithm,
                       QgsProcessingParameterString, QgsProcessingParameterBoolean,
                       QgsProcessingParameterEnum, QgsProcessingParameterFolderDestination)
import os
import subprocess
from datetime import datetime
import psycopg2  # Biblioteca para conexão com PostgreSQL

class MultiBackup(QgsProcessingAlgorithm):

    HOST = 'HOST'
    USER = 'USER'
    PORT = 'PORT'
    PASSWORD = 'PASSWORD'
    VERSION = 'VERSION'
    FOLDER = 'FOLDER'
    DATABASES = 'DATABASES'
    ALL_DATABASES = 'ALL_DATABASES'
    BACKUP_FORMAT = 'BACKUP_FORMAT'

    versions = ['9.5', '9.6', '10', '11', '12', '13', '14', '15', '16']
    backup_formats = ['plain', 'custom', 'directory']  # Tipos de formato de backup suportados

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterString(
                self.HOST,
                self.tr('Host', 'Host'),
                defaultValue='localhost'
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PORT,
                self.tr('Port', 'Porta'),
                defaultValue='5432'
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.USER,
                self.tr('User', 'Usuário'),
                defaultValue='postgres'
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.PASSWORD,
                self.tr('Password', 'Senha'),
                defaultValue='',
                optional=False
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                self.VERSION,
                self.tr('PostgreSQL version', 'Versão do PostgreSQL'),
                options=self.versions,
                defaultValue=7
            )
        )

        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.FOLDER,
                self.tr('Folder to save backups', 'Pasta para salvar os backups')
            )
        )

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.ALL_DATABASES,
                self.tr('Backup all databases in localhost?', 'Backup de todos os bancos de dados em localhost?'),
                defaultValue=False
            )
        )

        self.addParameter(
            QgsProcessingParameterString(
                self.DATABASES,
                self.tr('Database names (comma-separated)', 'Nomes dos bancos de dados (separados por vírgula)'),
                optional=True
            )
        )

        self.addParameter(
            QgsProcessingParameterEnum(
                self.BACKUP_FORMAT,
                self.tr('Backup format', 'Formato de backup'),
                options=self.backup_formats,
                defaultValue=1  # Default to 'custom'
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        host = self.parameterAsString(parameters, self.HOST, context)
        port = self.parameterAsString(parameters, self.PORT, context)
        user = self.parameterAsString(parameters, self.USER, context)
        password = self.parameterAsString(parameters, self.PASSWORD, context)
        version = self.parameterAsEnum(parameters, self.VERSION, context)
        folder = self.parameterAsString(parameters, self.FOLDER, context)
        all_databases = self.parameterAsBoolean(parameters, self.ALL_DATABASES, context)
        format_index = self.parameterAsEnum(parameters, self.BACKUP_FORMAT, context)
        backup_format = self.backup_formats[format_index]
        databases = self.parameterAsString(parameters, self.DATABASES, context).split(',') if not all_databases else []

        if not folder:
            raise QgsProcessingException(self.tr('Folder is required!', 'Pasta é obrigatória!'))
        
        if all_databases:
            databases = self.list_databases(host, port, user, password, feedback)
            if not databases:
                raise QgsProcessingException(self.tr('No databases found or failed to connect.', 'Nenhum banco de dados encontrado ou falha na conexão.'))
        elif not databases:
            raise QgsProcessingException(self.tr('At least one database must be specified!', 'Pelo menos um banco de dados deve ser especificado!'))

        # Caminho do pg_dump
        pg_dump_path = self.find_pg_dump(version)
        if not pg_dump_path:
            raise QgsProcessingException(self.tr('Could not find pg_dump executable for the specified PostgreSQL version.',
                                                 'Não foi possível encontrar o executável pg_dump para a versão do PostgreSQL especificada.'))

        # Realizando o Backup
        os.makedirs(folder, exist_ok=True)
        os.environ['PGPASSWORD'] = password

        for db in databases:
            db = db.strip()
            if not db:
                continue

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(folder, f"{db}_{timestamp}.{self.get_file_extension(backup_format)}")
            
            # Construindo comando
            cmd = [
                pg_dump_path,
                "-U", user,
                "-h", host,
                "-p", port,
                "-F", backup_format[0],  # Usando a inicial do formato para pg_dump ('p', 'c', 'd')
                "-f", backup_file,
                db
            ]
            
            feedback.pushInfo(f'Executando comando: {" ".join(cmd)}')
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                feedback.pushInfo(self.tr(f"Backup do banco de dados '{db}' concluído com sucesso.",
                                          f"Backup of database '{db}' completed successfully."))
            else:
                feedback.reportError(self.tr(f"Erro ao fazer backup do banco de dados '{db}': {result.stderr}",
                                             f"Error backing up database '{db}': {result.stderr}"))

        del os.environ['PGPASSWORD']
        return {}

    def find_pg_dump(self, version):
        # Define os possíveis caminhos para pg_dump
        version = self.versions[version]
        possible_paths = [
            f'C:/Program Files/PostgreSQL/{version}/bin/pg_dump.exe',
            f'D:/Program Files/PostgreSQL/{version}/bin/pg_dump.exe',
            f'C:/Program Files (x86)/PostgreSQL/{version}/bin/pg_dump.exe',
            f'D:/Program Files (x86)/PostgreSQL/{version}/bin/pg_dump.exe',
            f'/Library/PostgreSQL/{version}/bin/pg_dump'
        ]
        
        for path in possible_paths:
            if os.path.isfile(path):
                return path
        return None

    def list_databases(self, host, port, user, password, feedback):
        try:
            connection = psycopg2.connect(dbname='postgres', user=user, password=password, host=host, port=port)
            cursor = connection.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            connection.close()
            return databases
        except Exception as e:
            feedback.reportError(self.tr(f"Erro ao listar bancos de dados: {str(e)}", f"Error listing databases: {str(e)}"))
            return []

    def get_file_extension(self, backup_format):
        if backup_format == 'plain':
            return 'sql'
        elif backup_format == 'custom':
            return 'backup'
        elif backup_format == 'directory':
            return 'dir'
        return 'backup'

    def name(self):
        return 'multi_backup'

    def displayName(self):
        return self.tr('Backup Multiple Databases', 'Backup de Múltiplos Bancos de Dados')

    def group(self):
        return self.tr('PostGIS Tools', 'Ferramentas PostGIS')

    def groupId(self):
        return 'postgis_tools'

    def createInstance(self):
        return MultiBackup()

    def tr(self, string, string_pt=None):
        return QCoreApplication.translate('Processing', string) if string_pt is None else QCoreApplication.translate('Processing', string_pt)
