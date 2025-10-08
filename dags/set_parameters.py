import configparser
import os
import platform

config = configparser.ConfigParser()
current_os = platform.system()
config.read('/opt/airflow/dags/parameters.ini', encoding='utf-8')


if current_os == 'Linux':
    replace_from = 'D:'
    replace_to = '/opt/airflow'
else:
    replace_from = None  # не меняем пути



# config.read('/opt/airflow/dags/parameters.ini', encoding='utf-8') # для работы без сервера
# config.read('/root/Airflow/dags/parameters.ini', encoding='utf-8') # для работы с сервером
# files_read = config.read('../dags/parameters.ini', encoding='utf-8')

api_id = config['telegram']['api_id']
api_hash = config['telegram']['api_hash']
repo_id = config['mobiledep']['repo_id']
repo_id_ltmdb = config['mobiledep']['need_to_load_repo_id']
server_url = config['mobiledep']['server_url']
login_name = config['mobiledep']['login_name']
password = config['mobiledep']['password']
parent_dir = config['mobiledep']['parent_dir']

files_path = config['paths']['files_path']
images_path = config['paths']['images_path']
archives_with_duplicates = config['paths']['archives_with_duplicates']
directory = config['paths']['directory']
collection_files_path = config['paths']['collection_files_path']
session_telegram = config['paths']['session_telegram']
# os.environ['FILES_PATH'] = files_path
# os.environ['INFOFILES_PATH'] = images_path
# os.environ['ARCHIVE_PATH'] = directory
# os.environ['ARCHIVE_DUP_PATH'] = archives_with_duplicates
# os.environ['COLLECTION_PATH'] = collection_files_path
# os.environ['SESSION_PATH'] = session_telegram
# os.environ['ANALITICS_PATH'] = work_folder

ip_addr = config['mongo']['ip_addr']

mongo_user_name = config['mongo']['user_name']
mongo_password = config['mongo']['key']
my_mongo_url = config['mongo']['my_mongo_url']
my_database_name = config['mongo']['my_database_name']
my_collection_name = config['mongo']['my_collection_name']

work_database_name = config['mongo']['work_database_name']
work_collection_name = config['mongo']['work_collection_name']

# os.environ['MONGO_USERNAME'] = mongo_user_name
# os.environ['MONGO_PASSWORD'] = mongo_password

folders_Air = {"files_path": '/mnt/telegram', "images_path": '/mnt/infofiles',
               "directory": '/mnt/archives',
               "archives_with_duplicates": '/mnt/archives_dup',
               "collection_files_path": '/mnt/collection_files',
               "session_telegram": r'/mnt/session_telegram'}

ipv4_addr = config['mypc']['ipv4_addr']

# Функция для преобразования путей, если надо
def convert_path(path):
    if replace_from and replace_from in path:
        return path.replace(replace_from, replace_to)
    return path


# Формируем переменные окружения с учётом ОС
env_vars = {
    "AIRFLOW_UID": "50000",
    "FILES_PATH": convert_path(files_path),
    "INFOFILES_PATH": convert_path(images_path),
    "ARCHIVE_PATH": convert_path(directory),
    "ARCHIVE_DUP_PATH": convert_path(archives_with_duplicates),
    "COLLECTION_PATH": convert_path(collection_files_path),
    "SESSION_PATH": convert_path(session_telegram),
    "MONGO_USERNAME": mongo_user_name,
    "MONGO_PASSWORD": mongo_password,
    "DEFAULT_UI_TIMEZONE": "Europe/Moscow"
}
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
env_path = os.path.join(base_dir, '.env')
# Записываем .env
with open(env_path, 'w') as env_file:
    for k, v in env_vars.items():
        env_file.write(f"{k}={v}\n")

print(f".env файл .env создан для ОС: {current_os}")
# ## для работы с windows через docker
# with open('..\.env', 'w') as env_file:
#     env_file.write("AIRFLOW_UID=50000\n")
#     env_file.write(f"FILES_PATH={files_path}\n")
#     env_file.write(f"INFOFILES_PATH={images_path}\n")
#     env_file.write(f"ARCHIVE_PATH={directory}\n")
#     env_file.write(f"ARCHIVE_DUP_PATH={archives_with_duplicates}\n")
#     env_file.write(f"COLLECTION_PATH={collection_files_path}\n")
#     env_file.write(f"SESSION_PATH={session_telegram}\n")
#     env_file.write(f"MONGO_USERNAME={mongo_user_name}\n")
#     env_file.write(f"MONGO_PASSWORD={mongo_password}\n")
#     env_file.write("DEFAULT_UI_TIMEZONE=Europe/Moscow\n")
#
# ## для работы с виртуальным сервером
# with open('..\.env', 'w') as env_file:
#     env_file.write("AIRFLOW_UID=50000\n")
#     env_file.write(f"FILES_PATH={files_path.replace('D:', '/root')}\n")
#     env_file.write(f"INFOFILES_PATH={images_path.replace('D:', '/root')}\n")
#     env_file.write(f"ARCHIVE_PATH={directory.replace('D:', '/root')}\n")
#     env_file.write(f"ARCHIVE_DUP_PATH={archives_with_duplicates.replace('D:', '/root')}\n")
#     env_file.write(f"COLLECTION_PATH={collection_files_path.replace('D:', '/root')}\n")
#     env_file.write(f"SESSION_PATH={session_telegram.replace('D:', '/root')}\n")
#     env_file.write(f"MONGO_USERNAME={mongo_user_name}\n")
#     env_file.write(f"MONGO_PASSWORD={mongo_password}\n")
#     env_file.write("DEFAULT_UI_TIMEZONE=Europe/Moscow\n")
