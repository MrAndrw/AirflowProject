import configparser
import os

config = configparser.ConfigParser()
config.read('/opt/airflow/dags/parameters.ini', encoding='utf-8')
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

with open('..\.env', 'w') as env_file:
    env_file.write("AIRFLOW_UID=50000\n")
    env_file.write(f"FILES_PATH={files_path}\n")
    env_file.write(f"INFOFILES_PATH={images_path}\n")
    env_file.write(f"ARCHIVE_PATH={directory}\n")
    env_file.write(f"ARCHIVE_DUP_PATH={archives_with_duplicates}\n")
    env_file.write(f"COLLECTION_PATH={collection_files_path}\n")
    env_file.write(f"SESSION_PATH={session_telegram}\n")
    env_file.write(f"MONGO_USERNAME={mongo_user_name}\n")
    env_file.write(f"MONGO_PASSWORD={mongo_password}\n")
    env_file.write("DEFAULT_UI_TIMEZONE=Europe/Moscow\n")
