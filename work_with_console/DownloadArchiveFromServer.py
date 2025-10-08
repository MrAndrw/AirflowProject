import time

import paramiko
from scp import SCPClient
import os
from set_parameters import *
import asyncio
from seafileapi import SeafileAPI


# Удалённая и локальная директории
# remote_dir = '/opt/airflow/test_archive/'


async def ConnectionToSeafile(login_name, password):
    try:
        import requests
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # Патчим requests, чтобы не проверять SSL
        _old_send = requests.Session.send

        def _patched_send(self, request, **kwargs):
            kwargs['verify'] = False
            return _old_send(self, request, **kwargs)

        requests.Session.send = _patched_send

        seafile_api = SeafileAPI(login_name, password, server_url)
        seafile_api.auth()
        repo = seafile_api.get_repo(f"{repo_id}")
        need_to_load_repo_id = seafile_api.get_repo(f"{repo_id_ltmdb}")
        return repo, repo.token, need_to_load_repo_id, need_to_load_repo_id.token
    except Exception as e:
        print(f"Произошла ошибка: {e}. Переподключение...")
        await asyncio.sleep(5)
        return await ConnectionToSeafile(login_name, password)


def create_ssh_client(server, port, user, pwd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server, port, user, pwd)
    return ssh


def list_remote_files(ssh):
    stdin, stdout, stderr = ssh.exec_command(f'find "{server_archive_path}" -type f -printf "%f|%s\n"')
    file_info = stdout.read().decode().splitlines()
    archives = []
    for line in file_info:
        if '7z' in line:
            name, size = line.split('|')
            archives.append((name.strip(), int(size)))
    return archives


def download_file(ssh, filename):
    full_remote_path = f"{server_archive_path}/{filename}".replace("//", "/")
    local_path = os.path.join(directory, filename)
    with SCPClient(ssh.get_transport()) as scp:
        scp.get(full_remote_path, local_path)
        print(f"✅ Скачан: {filename}")


def FindArchiveInRepo(repo, archive_name, local_size):
    """
    Ищет файл в корне репозитория Seafile и возвращает True, если найден архив с совпадающим именем и размером.
    """
    try:
        entries = repo.list_dir(parent_dir)
        if not entries:
            print("Репозиторий пуст или не удалось получить содержимое.")
            return None

        for entry in entries:
            if entry.get("type") == "file" and entry.get("name") == archive_name:
                remote_size = int(entry.get("size", 0))
                if remote_size == local_size:
                    return entry
                else:
                    print(f"Файл найден, но размеры не совпадают: локальный={local_size}, удалённый={remote_size}")
        return None
    except Exception as e:
        print(f"Ошибка при поиске архива в репозитории: {e}")
        return None


def Size(size):
    if size < 1024:
        return f"{size} Б"
    elif 1024 <= size < 1024 ** 2:
        return f"{round(size / 1024, 3)} Кб"
    elif 1024 ** 2 <= size < 1024 ** 3:
        return f"{round(size / 1024 ** 2, 3)} Мб"
    else:
        return f"{round(size / 1024 ** 2, 3)} Мб ({round(size / 1024 ** 3, 3)} Гб)"


async def DownloadArchiveFromWorkServer():
    print("🔌 Подключаемся к OMP Seafile...")
    parameters = await ConnectionToSeafile(login_name, password)
    repo = parameters[0]
    # repo, repo_token, _, _ = await ConnectionToSeafile(login_name, password)

    print("🔐 Подключаемся к серверу по SSH...")
    ssh = create_ssh_client(server_id.replace('\'', ''), server_port.replace('\'', ''),
                            server_username.replace('\'', ''), server_password.replace('\'', ''))

    print("📂 Получаем список архивов на сервере...")
    archives = list_remote_files(ssh)
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)

    total = len(archives)
    skipped = 0
    downloaded = 0
    for idx, (archive_name, archive_size) in enumerate(archives, 1):
        print(f"\n🔎 [{idx}/{total}] Проверка архива: {archive_name}\t\t{Size(archive_size)}")
        found = FindArchiveInRepo(repo, archive_name, archive_size)

        if found:
            print(f"⏩ Архив уже есть в OMP: {archive_name}")
            skipped += 1
        else:
            print(f"⬇️ Архив не найден в OMP. Начинается загрузка с сервера... ")
            try:
                download_file(ssh, archive_name)
                downloaded += 1
            except Exception as e:
                print(f"⚠️ Ошибка при скачивании {archive_name}: {e}")

    print(f"\n🏁 Готово! Загрузили: {downloaded}, Пропущено: {skipped}, Всего: {total}")
    ssh.close()
    return


async def UploadFilesOnWorkServer():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("🔐 Подключаемся к серверу по SSH...")
    ssh = create_ssh_client(server_id.replace('\'', ''), server_port.replace('\'', ''),
                            server_username.replace('\'', ''), server_password.replace('\'', ''))
    with SCPClient(ssh.get_transport()) as scp:
        for file_name in os.listdir(files_path):
            local_path = os.path.join(files_path, file_name)
            if os.path.isfile(local_path):  # Только файлы
                try:
                    print(f"⬆️ Загружаем {file_name}\t\t{Size(os.path.getsize(local_path))}...")
                    scp.put(local_path, f"{server_files_path}/{file_name}")
                    print(f"✅ Успешно загружен: {file_name}")
                except Exception as e:
                    print(f"⚠️ Ошибка при загрузке {file_name}: {e}")
    ssh.close()


from pymongo import MongoClient


def TmpMongoCollection():
    mongo_url = (
        f"mongodb://{mongo_user_name}:{mongo_password}@{ip_addr}:27017/?authMechanism=DEFAULT"
    )
    database_name = work_database_name
    collection_name = work_collection_name
    return mongo_url, database_name, collection_name


def ConnectToMongo(collection_parameters):
    client = MongoClient(collection_parameters[0])
    database = client[collection_parameters[1]]
    collection = database[collection_parameters[2]]
    return collection


def FindFileInMongo():
    mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    notfound = []
    found = []
    for file_name in os.listdir(files_path):
        if mongo_collection.find_one({"files.name": file_name}):
            found.append(file_name)
        else:
            notfound.append(file_name)
    print('\n'.join(notfound))
    time.sleep(5)
    print("\nFound! :\n")
    print('\n'.join(found))


# if __name__ == "__main__":
#     asyncio.run(UploadFilesOnWorkServer())
