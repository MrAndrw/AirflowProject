import configparser
import os
config = configparser.ConfigParser()

base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, '../dags/parameters.ini')

config.read(config_path, encoding='utf-8')
api_id = config['telegram']['api_id']
api_hash = config['telegram']['api_hash']
repo_id = config['mobiledep']['repo_id']
repo_id_ltmdb = config['mobiledep']['need_to_load_repo_id']
server_url = config['mobiledep']['server_url']
login_name = config['mobiledep']['login_name']
password = config['mobiledep']['password']
parent_dir = config['mobiledep']['parent_dir']

files_path = config['paths']['files_path'].replace('\'', "")
images_path = config['paths']['images_path'].replace('\'', "")
archives_with_duplicates = config['paths']['archives_with_duplicates'].replace('\'', "")
directory = config['paths']['directory'].replace('\'', "")
collection_files_path = config['paths']['collection_files_path'].replace('\'', "")
session_telegram = config['paths']['session_telegram'].replace('\'', "")
chrome_path = config['paths']['chrome_path'].replace('\'', "")

ip_addr = config['mongo']['ip_addr']
mongo_user_name = config['mongo']['user_name']
mongo_password = config['mongo']['key']
my_mongo_url = config['mongo']['my_mongo_url']
my_database_name = config['mongo']['my_database_name']
my_collection_name = config['mongo']['my_collection_name']
work_database_name = config['mongo']['work_database_name']
work_collection_name = config['mongo']['work_collection_name']


