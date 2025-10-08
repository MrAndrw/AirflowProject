import configparser
import os
import platform

config = configparser.ConfigParser()

base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, '../dags/parameters.ini')
current_os = platform.system()
# if current_os == 'Linux':
#     replace_from = 'D:'
#     replace_to = '/opt/airflow'
# else:
#     config.read('/opt/airflow/dags/parameters.ini', encoding='utf-8')
#
#     replace_from = None  # не меняем пути
config.read(config_path, encoding='utf-8')


def fix_path(path):
    path = path.replace('\'', '')  # Убираем кавычки
    if current_os == 'Linux':
        # Заменяем Windows путь D:/something на Linux путь /opt/airflow/something
        if path.startswith('D:/'):
            path = path.replace('D:/', '/opt/airflow/')
    return path


api_id = config['telegram']['api_id']
api_hash = config['telegram']['api_hash']
repo_id = config['mobiledep']['repo_id']
repo_id_ltmdb = config['mobiledep']['need_to_load_repo_id']
server_url = config['mobiledep']['server_url']
login_name = config['mobiledep']['login_name']
password = config['mobiledep']['password']
parent_dir = config['mobiledep']['parent_dir']

files_path = fix_path(config['paths']['files_path'])
images_path = fix_path(config['paths']['images_path'])
archives_with_duplicates = fix_path(config['paths']['archives_with_duplicates'])
directory = fix_path(config['paths']['directory'])
collection_files_path = fix_path(config['paths']['collection_files_path'])
session_telegram = fix_path(config['paths']['session_telegram'])
chrome_path = fix_path(config['paths']['chrome_path'])

ip_addr = config['mongo']['ip_addr']
mongo_user_name = config['mongo']['user_name']
mongo_password = config['mongo']['key']
my_mongo_url = config['mongo']['my_mongo_url']
my_database_name = config['mongo']['my_database_name']
my_collection_name = config['mongo']['my_collection_name']
work_database_name = config['mongo']['work_database_name']
work_collection_name = config['mongo']['work_collection_name']

server_id = config['server']['server_id']
server_port = config['server']['server_port']
server_username = config['server']['server_username']
server_password = config['server']['server_password']
server_archive_path = fix_path(config['server']['server_archive_path'])
server_files_path = fix_path(config['server']['server_files_path'])

