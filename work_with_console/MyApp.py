import asyncio
import os
import glob
import zipfile
from os.path import basename
import time
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from seafileapi import SeafileAPI
import urllib.request as ur
import shutil
import io
from work_with_telegram import *
from test_interface import *
from FileToHashWindow import *
import hashlib
from urllib.parse import quote
from datetime import datetime
from interface_for_files import *
from collections import Counter
from set_parameters import session_telegram, repo_id, server_url, login_name, password, parent_dir, repo_id_ltmdb

start_time = time.time()


# session_telegram = r"D:\session_telegram"
# repo_id = "d2edeb85-daea-4ed7-8546-b644c3f411c6"
# server_url = "https://files.mobiledep.ru/"
# login_name = "amalkov@mobiledep.ru"
# password = "GazSqLBczd"
# parent_dir = "/"


def FilesMetainfoCollectionPaths():
    file_path = folders.get("files_path")
    image_path = folders.get("images_path")
    collection_file_path = folders.get("collection_files_path")

    files = list(filter(os.path.isfile, glob.glob(f"{file_path}/*") + glob.glob(f"{file_path}/.*")))
    files.sort(key=lambda x: os.path.getctime(x))

    collections = list(filter(os.path.isdir, glob.glob(f"{collection_file_path}/*")))
    collections.sort(key=lambda x: os.path.getctime(x))

    images = list(filter(os.path.isfile, glob.glob(f"{image_path}/*") + glob.glob(f"{image_path}/.*")))
    images.sort(key=lambda x: os.path.getctime(x))

    return files, images, collections


def Archives_Path():
    archive_path = folders.get("directory")
    archives_with_duplicates_path = folders.get("archives_with_duplicates")
    archives = list(filter(os.path.isfile, glob.glob(f"{archive_path}/*") + glob.glob(f"{archive_path}/.*")))
    archives_dup = list(filter(os.path.isfile, glob.glob(f"{archives_with_duplicates_path}/*") + glob.glob(
        f"{archives_with_duplicates_path}/.*")))
    return archives, archives_dup


async def ConnectionToSeafile(login_name, password):
    try:
        # seafile_api = SeafileApiClient(server_url, login_name, password)
        # repo = seafile_api.get(f"{repo_id}")
        seafile_api = SeafileAPI(login_name, password, server_url)
        seafile_api.auth()
        repo = seafile_api.get_repo(f"{repo_id}")
        need_to_load_repo_id = seafile_api.get_repo(f"{repo_id_ltmdb}")
        return repo, repo.token, need_to_load_repo_id, need_to_load_repo_id.token
        # parent = repo.list_dir(parent_dir)
        # print(parent)
        # for folder in parent:
        #     print(folder)
        #     time.sleep(2)
        # if folder['type'] == 'dir' and folder['id'] == 'aeb516e84ad79f939d74ad405fbe2849a5034599':
        #     print(folder)
        #         #file = folder.getfile('AMD.7z')
        #         #print(file)
        #         print(folder['id'], folder['name'])
        # return repo, repo.token
    except Exception as e:
        print(f"Произошла ошибка: {e}. Переподключение...")
        await asyncio.sleep(5)
        return await ConnectionToSeafile(login_name, password)


# def ConnectionToSeafile(login_name, password):
#     seafile_api = SeafileAPI(login_name, password, server_url)
#     seafile_api.auth()
#     repo = seafile_api.get_repo(f"{repo_id}")
#     parent = repo.list_dir(parent_dir)
#     # for folder in parent:
#     #     if folder['type'] == 'dir' and folder['id'] == 'aeb516e84ad79f939d74ad405fbe2849a5034599':
#     #         print(folder)
#     #         #file = folder.getfile('AMD.7z')
#     #         #print(file)
#     #         print(folder['id'], folder['name'])
#     return repo, repo.token


def HashSumCheck(file1, file2):
    hash_func = hashlib.sha256
    with open(file1, "rb") as f:
        hash1 = hash_func(f.read()).hexdigest()
    with open(file2, "rb") as f:
        hash2 = hash_func(f.read()).hexdigest()
    return hash1 == hash2


def TestArchiveForDuplicates(repo, archives, names_of_duplicate):
    count = 0
    mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    for archive in archives:
        archive_size = os.path.getsize(archive)
        archive_name = os.path.basename(archive)
        if CheckUploadTimeInMongo(mongo_collection, archive_name) is not False:
            count += 1
        else:
            try:
                duplicate = repo.get_file(f"/{archive_name}")
                if (archive_size <= 1.05 * duplicate["size"]) and (
                        archive_size >= 0.95 * duplicate["size"]
                ):
                    count += 1
            except Exception:
                continue

    if count == 0:
        print("Повторяющихся архивов нет!" "\n")
        return False
    else:
        print("Повторяющихся архивов: ", count, "\n")
        for archive in archives:
            archive_size = os.path.getsize(archive)
            archive_name = os.path.basename(archive)
            if CheckUploadTimeInMongo(mongo_collection, archive_name) is not False:
                names_of_duplicate.append(archive_name)
                print(f"Архив {archive_name} уже был загружен на сервер. Он мог быть перемещен из папки For Sort в "
                      f"другую папку.")
            try:

                duplicate = repo.get_file(f"/{archive_name}")
                if (archive_size <= 1.05 * duplicate["size"]) and (
                        archive_size >= 0.95 * duplicate["size"]
                ):
                    names_of_duplicate.append(duplicate["name"])
                    print("Загружаемый архив: ", archive_name, Size(archive_size))
                    print(
                        "На сервере: ", duplicate["name"], Size(duplicate["size"]), "\n"
                    )
            except Exception:
                continue
        return True


def CheckForArchives(archives):
    if len(archives) == 0:
        return False
    else:
        print(f"\nДоступных архивов: {len(archives)}")
        print("\nНазвания архивов:")
        for archive in archives:
            size = os.path.getsize(archive)
            print(
                os.path.basename(archive), "\t", Size(size)
            )
        return True


def ArchiveRenamerToMove(archive, archive_name, tmp):
    count = 1
    new_archive_name = archive_name.replace(".7z", f" ({count}).7z")
    print(f"Переименовывание архива {archive_name} в {new_archive_name}")
    new_archive_path = os.path.join(folders.get("directory"), new_archive_name)
    os.rename(archive, new_archive_path)
    target_path = folders.get("archives_with_duplicates")
    while True:
        try:
            shutil.move(new_archive_path, target_path)
            print(f"Архив {new_archive_name} успешно перемещён в {target_path}")
            break
        except FileExistsError:
            if count == 101:
                return False
            count += 1
            print(f"Файл с именем {new_archive_name} уже существует. Попытка {count}...")
            new_archive_name = new_archive_name.replace(f" ({count - 1}).7z",
                                                        f" ({count}).7z")
            new_archive_path = os.path.join(folders.get("directory"), new_archive_name)
            os.rename(archive, new_archive_path)
    return True


# def ArchiveRenamerToMove(archive, archive_name, count):
#     global new_archive_path, new_archive_name
#     if count == 1:
#         new_archive_name = archive_name.replace(".7z", f" ({count}).7z")
#     else:
#         new_archive_name = archive_name.replace(
#             f" ({count - 1}).7z", f" ({count}).7z"
#         )
#     print(f"Переименовывание архива {archive_name} в {new_archive_name}")
#     os.rename(archive, folders.get("directory") + "\\" + new_archive_name)
#     new_archive_path = list(
#         filter(
#             os.path.isfile,
#             glob.glob(folders.get("directory") + f"\\{new_archive_name}"),
#         )
#     )
#     try:
#         shutil.move(new_archive_path[0], folders.get("archives_with_duplicates"))
#     except Exception:
#         print("Такой файл уже существует!")
#         # if count == 100:
#         #     print("100я попытка безуспешна, возникли проблемы!")
#         #     return False
#         count += 1
#         ArchiveRenamerToMove(new_archive_path[0], new_archive_name, count)
#     return True


def MoveArchives(archives, names_of_duplicate):
    files = FilesMetainfoCollectionPaths()[0]
    images = FilesMetainfoCollectionPaths()[1]
    collections = FilesMetainfoCollectionPaths()[2]
    print(
        "Архивы с дупликатами будут загружены в папку: ",
        folders.get("archives_with_duplicates"),
        "\n",
    )
    for archive in archives:
        archive_name = os.path.basename(archive)
        if archive_name in names_of_duplicate:
            count = 1
            try:
                shutil.move(archive, folders.get("archives_with_duplicates"))
            except FileExistsError:
                result = ArchiveRenamerToMove(archive, archive_name, count)
                if not result:
                    print("Проблемы с переносом архива!")
                    MenuArchiveWork(files, images, collections)
                else:
                    continue
            except shutil.Error as e:
                print(f"Ошибка при перемещении архива {archive_name}: {e}")
                MenuArchiveWork(files, images, collections)


# def get_upload_link(server_url, token, repo_id):
#   url = f'{server_url}/api2/repos/{repo_id}/upload-link/'
#   headers = {'Authorization': f'Token {token}'}
#   data = {'parent_dir': '/', 'filename': 'Traverse_City_Area_Public_Schools.7z'}
#   response = requests.post(url, headers=headers, data=data)
#   response.raise_for_status()
#   return response.json()['upload_link']


def TakeNames(filepath):
    file_names = []
    excluded_extensions = ('.xlsx', '.xlsm', '.docx', '.pptx', '.odt', '.ods', '.odp')
    if not filepath.lower().endswith(excluded_extensions):
        if py7zr.is_7zfile(filepath):
            try:
                with py7zr.SevenZipFile(filepath, mode="r") as archive:
                    for file_info in archive.list():
                        file_name = file_info.filename
                        if not file_name == 'info_file.txt':
                            file_names.append(file_name)

                file_names.append(os.path.basename(filepath))
                file_names = list(set(file_names))
                return file_names
            except Exception as e:
                logging.error("An error occurred: %s", e)
                file_names.append(os.path.basename(filepath))
                return file_names

        elif zipfile.is_zipfile(filepath):
            try:
                with zipfile.ZipFile(filepath, mode="r") as archive:
                    file_list = archive.namelist()
                    for file_name in file_list:
                        if not file_name == 'info_file.txt':
                            file_names.append(file_name)
                file_names.append(os.path.basename(filepath))
                file_names = list(set(file_names))
                return file_names
            except Exception as e:
                logging.error("An error occurred: %s", e)
                file_names.append(os.path.basename(filepath))
                return file_names


def Update(collection, file_name, upload_time):
    document_archive = collection.find_one({"archive_name": file_name})
    if document_archive is not None:
        if file_name.startswith('mail_pass_country'):
            update_result = collection.update_one(
                {"archive_name": file_name},
                {"$set": {"upload_to_server_time": upload_time,
                          "admin_name": "ЗАН",
                          "check_status": "#Need_to_Load",
                          "loader_name": "МАД",
                          "operator_name": "robot",
                          "processing_status": "В работе",
                          "status_changed_time": upload_time
                          }},
                upsert=False
            )
        else:
            update_result = collection.update_one(
                {"archive_name": file_name},
                {"$set": {"upload_to_server_time": upload_time}},
                upsert=False
            )
        print(f"{file_name} обновлен в базе MongoDB!")
        return update_result
    else:
        return None


def UpdateMongoDocument(archive_name, collection):
    upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_result = Update(collection, archive_name, upload_time)
    if update_result:
        print(f"\nИнформация о времени загрузки была успешно добавлена в базу данных!\n")
    else:
        print("\nОшибка при записи данных, скорее всего информации о файлах в архиве не существует в базе данных "
              "MongoDB!")
    return


# def UpdateMongoDocument(archive, collection):
#     updated_count = 0
#     upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     names_in_archive = TakeNames(archive)
#     for file_name in names_in_archive:
#         update_result = Update(collection, file_name, upload_time)
#         if update_result:
#             if update_result.modified_count > 0:
#                 updated_count += 1
#     if updated_count > 0:
#         print(f"\nИнформация о времени загрузки была успешно добавлена в базу данных!\n"
#               f"Обновлено записей: {updated_count}")
#     else:
#         print("\nОшибка при записи данных, скорее всего информации о файлах в архиве не существует в базе данных "
#               "MongoDB!")
#     return


def TryMongoConnect(files, images, collections):
    try:
        mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
        return mongo_collection
    except Exception as e:
        logging.error("An error occurred: %s", e)
        s = input("\nНе удается подключиться к базе данных MongoDB!\n"
                  "Если хотите загрузить архив без добавления даты загрузки в базу данных, нажмите 1\n"
                  "Для отмены выгрузки архивов, нажмите 0\n"
                  "Выбор: ")
        if s == '1':
            return None
        elif s == '0':
            return MenuArchiveWork(files, images, collections)
        else:
            print("Введите 1 или 0 по условию!")
            return TryMongoConnect(files, images, collections)


def TakeRowCountFromFile():
    collection_path = os.path.join(folders.get("collection_files_path"), "mail_pass_country")
    row_count_path = os.path.join(collection_path, "sum_all_rows.txt")
    if os.path.exists(row_count_path):
        with open(row_count_path, 'r') as file:
            entity = file.read()
            try:
                row_count = int(entity)
                return row_count
            except Exception as e:
                print(e)
    return 0


def UploadToSeafile(token, archives, server_url, repo_id, loaded_to_mongo_repo_id, loaded_to_mongo_token):
    files = FilesMetainfoCollectionPaths()[0]
    images = FilesMetainfoCollectionPaths()[1]
    collections = FilesMetainfoCollectionPaths()[2]
    not_loaded_archives = {}
    if len(archives) == 0:
        print("\nВыгрузка на сервер не требуется, Архивов нет!")
        return MenuArchiveWork(files, images, collections)
    else:
        print("\nВыгрузка архивов на сервер...")

    api_url = f"{server_url}/api2/repos/{repo_id}/upload-link/"
    headers = {"Authorization": f"Token {token}"}
    payload = {"parent_dir": f"{parent_dir}"}

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    try:
        response = session.get(api_url, headers=headers)
        upload_link = f"{response.json()}?ret-json=1"

        if not upload_link:
            print("Ошибка получения upload_link.")
            return

        count = 1
        mongo_collection = TryMongoConnect(files, images, collections)
        previous_uploads = []
        row_count = TakeRowCountFromFile()
        for archive in archives:
            if os.path.basename(archive).startswith('mail_pass_country'):
                archive_name = os.path.basename(archive)
                api_url = f"{server_url}/api2/repos/{loaded_to_mongo_repo_id}/upload-link/"
                headers = {"Authorization": f"Token {loaded_to_mongo_token}"}
                payload = {"parent_dir": f"{parent_dir}"}
                print(f"\nАрхив {archive_name} будет загружен в новый репозиторий с ID {loaded_to_mongo_repo_id}")
                try:
                    response = session.get(api_url, headers=headers)
                    if response.status_code != 200:
                        print(f"Ошибка при получении upload_link для архива {archive_name} в новом репозитории.")
                        continue
                    upload_link = f"{response.json()}?ret-json=1"
                except Exception as e:
                    print(e)
                    continue

            archive_name = os.path.basename(archive)
            size = os.path.getsize(archive_name)
            if size / 1024 ** 3 >= 20:
                print(f"\nАрхив {archive_name} слишком много весит для загрузки на сервер!\n"
                      f"Загрузите его вручную и обновите информацию о загрузке в базе MongoDB.\n")
                not_loaded_archives[archive_name] = Size(size)
            else:
                archive_size = Size(size)
                print(f"\nЗагрузка архива {archive_name}... \t Размер: {archive_size}")
                if previous_uploads:
                    avg_speed = sum(size / time_taken for size, time_taken in previous_uploads) / len(
                        previous_uploads)  # Средняя скорость загрузки
                    estimated_time = size / avg_speed if avg_speed > 0 else 0  # Ожидаемое время загрузки
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if estimated_time > 600:
                        estimated_time = estimated_time / 60
                        print(f"Начало загрузки: {current_time}\nОжидаемая продолжительность загрузки:"
                              f" {estimated_time:.2f} минут")
                    else:
                        print(f"Начало загрузки: {current_time}\nОжидаемая продолжительность загрузки:"
                              f" {estimated_time:.2f} секунд")

                start_load_time = time.time()  # Запоминаем время начала загрузки

                with open(archive, "rb") as file_stream:
                    response = session.post(upload_link, headers=headers, data=payload, files={"file": file_stream},
                                            stream=True)

                end_load_time = time.time()  # Запоминаем время окончания загрузки
                upload_time = end_load_time - start_load_time  # Вычисляем время загрузки
                previous_uploads.append((size, upload_time))

                if response.status_code == 200:
                    name = response.json()[0]["name"]
                    print(f"Архив {name} загружен на сервер", '\t', count, "из", len(archives))
                    if mongo_collection is not None:
                        UpdateMongoDocument(archive_name, mongo_collection)

                    count += 1

                    upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    with open(r'C:\Users\and23\OneDrive\Desktop\Статистика все.txt', 'a', encoding='utf-8') as file:
                        file.write(f'{upload_time} - {count - 1}\t\t {archive_name} \t\t{archive_size}\n')

                else:
                    print(f"\nПроизошла ошибка при загрузке архива {archive_name}\n")
        print(f"\nЗагруженные на сервер: ", '\t', count - 1, "из", len(archives), "архивов")
        if not_loaded_archives:
            print(f"\nНе загруженные архивы: ")
            for name, size in not_loaded_archives.items():
                print(name, '\t\t', size)
        upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(r'C:\Users\and23\OneDrive\Desktop\Статистика по количеству.txt', 'a', encoding='utf-8') as file:
            file.write(f'{upload_time} - {count - 1}\n')

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке: {e}")


def DownloadFromSeafile(token, archives, server_url, repo_id):
    dup_directory = folders.get("archives_with_duplicates")
    os.chdir(dup_directory)

    if not archives:
        print("\nЗагрузка архивов с сервера не требуется, повторяющихся архивов нет")
    else:
        print("\nЗагрузка дублирующих архивов с сервера...")
    headers = {"Authorization": f"Token {token}"}
    for archive in archives:
        archive_name = archive.replace(
            dup_directory + "\\", ""
        )
        full_archive_name = quote(archive_name)
        url = f"{server_url}/api2/repos/{repo_id}/file/?p={full_archive_name}&reuse=1"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            download_link = response.json()
            if download_link:
                try:
                    print(f"Загрузка архива: {archive_name}...")
                    with requests.get(download_link, stream=True) as r:
                        r.raise_for_status()  # Проверка на успешный ответ
                        with open(f"from_server_{archive_name}", 'wb') as f:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                    print(f"Архив from_server_{archive_name} скачан в {dup_directory}\n")
                except requests.exceptions.RequestException as e:
                    print(f"Ошибка при скачивании архива {archive_name}: {e}")
                    exit(0)
            else:
                print(f"Не удалось получить ссылку для скачивания архива {archive_name}")
                exit(0)
        else:
            print(f"Ошибка при получении ссылки для архива {archive_name}: {response.status_code}")
            exit(0)
    return dup_directory
    # download_link = response.json()
    # try:
    #     ur.urlretrieve(download_link, f"from_server_{archive_name}")
    #     print(
    #         f"Архив from_server_{archive_name} скачан в",
    #         folders_for_work.get("archives_with_duplicates"),
    #         "\n",
    #     )
    # except Exception:
    #     print(f"Невозможно скачать архив {archive_name} по данной ссылке!")


def Size(size):
    if size < 1024:
        return f"{size} Б"
    elif 1024 <= size < 1024 ** 2:
        return f"{round(size / 1024, 3)} Кб"
    elif 1024 ** 2 <= size < 1024 ** 3:
        return f"{round(size / 1024 ** 2, 3)} Мб"
    else:
        return f"{round(size / 1024 ** 2, 3)} Мб ({round(size / 1024 ** 3, 3)} Гб)"


def ArchiveSize(name):
    archives = Archives_Path()[0]
    for archive in archives:
        archive_name = os.path.basename(archive)
        if name == archive_name:
            size = os.path.getsize(archive_name)
            return size


def CheckForWork():
    files = FilesMetainfoCollectionPaths()[0]
    images = FilesMetainfoCollectionPaths()[1]
    collections = FilesMetainfoCollectionPaths()[2]

    archives = Archives_Path()[0]
    counter_files = len(files)
    counter_images = len(images)
    counter_archives = len(archives)
    counter_collections = len(collections)

    if (counter_files == 0) and (counter_images == 0) and (counter_archives != 0) and (counter_collections == 0):
        print(
            "Для архивации нет ни файлов и их описаний, ни коллекций, возможна только работа с архивами!"
        )
        return ViewNames(files, images, collections)
    elif (counter_files == 0) and (counter_images == 0) and (counter_archives == 0) and (counter_collections == 0):
        print("Архивов, файлов и коллекций нет, добавьте их для продолжения работы!\n")
        return MainMenu()
    elif (counter_files != 0 or counter_images != 0) and (counter_archives == 0) and (counter_collections == 0):
        print("Архивов и коллекций нет, возможна работа только с файлами!\n")
        return ViewNames(files, images, collections)

    elif (counter_files == 0 or counter_images == 0) and (counter_archives == 0) and (counter_collections != 0):
        print("Архивов и файлов нет, возможна работа только с коллекциями!\n")
        return ViewNames(files, images, collections)
    else:
        return ViewNames(files, images, collections)


def ViewNames(files, images, collections):
    counter_files = len(files)
    counter_images = len(images)
    counter_collections = len(collections)
    archives = Archives_Path()[0]
    CheckForArchives(archives)
    print(f"\nДоступных файлов: {counter_files}")
    print(f"Доступных файлов-описаний: {counter_images}")
    print(f"Доступных коллекций: {counter_collections}")

    if counter_files != 0:
        print("\nНазвания файлов:")
        for file in files:
            size = os.path.getsize(file)
            # size = os.path.getsize(os.path.join(files_path, file))
            print(os.path.basename(file), " ", Size(size))
    if counter_images != 0:
        print("\nНазвания файлов-описаний:")
        for img in images:
            print(os.path.basename(img))

    if counter_collections != 0:
        for collection in collections:
            print("\nКоллекция:", os.path.basename(collection), '\t Файлы:')
            collection_files = list(filter(os.path.isfile, glob.glob(f"{collection}/*")))
            for file in collection_files:
                size = os.path.getsize(file)
                collection_filepath = os.path.join(
                    folders.get("collection_files_path"), collection
                )
                print("\t" * 6, file.replace(collection_filepath + "\\", ""), " ", Size(size))

    return MenuArchiveWork(files, images, collections)


def Create_Archive_Name(filepath):
    name = []
    ar_name = []
    for letter in filepath:
        name.append(letter)
    curr = len(name) - 1
    while True:
        while name[curr] != "." and name[curr] != "\\":
            curr -= 1
            break
        else:
            if name[curr] == "\\":
                curr = len(name)
            for j in range(len(name)):
                curr -= 1
                if name[curr] == "\\":
                    rev_ar_name = list(reversed(ar_name))
                    need = "".join(rev_ar_name)
                    return need
                else:
                    ar_name.append(name[curr])


def Input_Archive_Name():
    s = input("\nВведите имя будущего архива 7z: ")
    test = re.fullmatch(".*\.7z", s)
    empty = re.fullmatch("\s*", s)
    empty1 = re.fullmatch("\.", s)
    empty2 = re.fullmatch("\s*\.7z", s)
    empty3 = re.fullmatch("\.7z", s)
    if len(s) < 200:
        if test:
            if empty2 or empty3:
                s = "empty_name"
                return s + ".7z"
            else:
                if ' ' in s:
                    s.replace(' ', "_")
                return s
        else:
            if empty or empty1:
                s = "empty_name"
                return s + ".7z"
            else:
                if ' ' in s:
                    s.replace(' ', "_")
                return s + ".7z"
    else:
        print("Имя слишком длинное!\n")
        return Input_Archive_Name()


def CheckExistArchiveNameInFolder(archive_name, j):
    base_name, ext = os.path.splitext(archive_name)
    while os.path.exists(archive_name):
        print(f"\nАрхив с именем {archive_name} уже есть в базе!")
        if re.search(r'\s\(\d+\)$', base_name):
            base_name = base_name.rsplit(" ", 1)[0]
        archive_name = f"{base_name} ({j}){ext}"
        print("Новое имя архива: ", archive_name)
        j += 1
    return archive_name


def CheckArchivenameInMongo(collection, filename):
    split = filename.rsplit(".", 1)[0]
    new_name = re.compile(f"^{re.escape(split)}\.[0-9a-zA-Z]*$", re.IGNORECASE)
    archive = collection.find_one({"archive_name": new_name})
    return archive if archive is not None else False


def CheckExistArchiveNameInMongo(work_mongo_collection, archive_name, j):
    base_name, ext = os.path.splitext(archive_name)
    while CheckArchivenameInMongo(work_mongo_collection, archive_name) is not False:
        print(f"\nАрхив с именем {archive_name} уже есть в базе!")
        if re.search(r'\s\(\d+\)$', base_name):
            base_name = base_name.rsplit(" ", 1)[0]
        archive_name = f"{base_name} ({j}){ext}"
        print("Новое имя архива: ", archive_name)
        j += 1
    return archive_name
    # if CheckArchivenameInMongo(work_mongo_collection, archive_name) is not False:
    #     print(f"\nАрхив с именем {archive_name} уже есть в базе!")
    #     base_name, ext = os.path.splitext(archive_name)
    #     if re.search(r'\s\(\d+\)$', base_name):
    #         base_name = base_name.rsplit(" ", 1)[0]
    #     archive_name = f"{base_name} ({j}){ext}"
    #     print("Новое имя архива: ", archive_name)
    #     while CheckArchivenameInMongo(work_mongo_collection, archive_name) is not False:
    #         print(f"\nАрхив с именем {archive_name} уже есть в базе!")
    #         base_name, ext = os.path.splitext(archive_name)
    #         base_name = base_name.rsplit(" ", 1)[0]
    #         j += 1
    #         archive_name = f"{base_name} ({j}){ext}"
    #         print("Новое имя архива: ", archive_name)
    # return archive_name


def TakeInfoFile(file_name, info_files):
    for info in info_files:
        info_file_name = os.path.basename(info)
        if (info_file_name == file_name.replace(".", "_") + ".txt") or (
                info_file_name.rsplit(".", 1)[0] == file_name.rsplit(".", 1)[0]):
            info_file = info.replace("\\", "\\\\")
            return info_file
    return None


def TakeSizeFromInfofile(file):
    try:
        with open(file, 'r', encoding='utf-8') as file_info:
            lines = file_info.readlines()
            for line in lines:
                if line.startswith('Размер файла:'):
                    current_file_size = line.replace('Размер файла: ', '').replace('\n', '')
                    return current_file_size
    except Exception as e:
        print(f"Ошибка при открытии файла {file}")
        print(e)
        return None


def Create_Many_Archives(files, info_files, work_mongo_collection, my_mongo_collection):
    folder_file_path = folders.get("files_path")
    count = 0
    for i in range(len(files)):

        count += 1
        file = files[i].replace("\\", "\\\\")
        file_name = files[i].replace(folder_file_path + "\\", "")
        # info_file = info_files[i].replace("\\", "\\\\")
        info_file = TakeInfoFile(file_name, info_files)
        if not info_file:
            print(f"Ошибка! Проверьте наличие файла с метаинформацией для файла {file_name}")
            continue
            # exit(0)
        else:
            current_file_size = TakeSizeFromInfofile(info_file)
            archive_name = f"{Create_Archive_Name(files[i])}.7z"
            archive_name = CheckExistArchiveNameInMongo(work_mongo_collection, archive_name, j=1)
            archive_name = CheckExistArchiveNameInFolder(archive_name, j=1)
            print(f"\nВыполняется создание архива {archive_name}...")
            print(f"Выполняется сбор ХЭШа для файла {file_name}...")
            file_hash = [TakeHash(file)]
            file_size = Size(os.path.getsize(file))
            if file_size != current_file_size:
                if current_file_size:
                    print("\nОдноименный файл с другим размером не может быть добавлен в текущий архив!\n"
                          "Проверьте этот файл вручную.")
                    continue

            if CheckFilehashInMongo(work_mongo_collection, file_hash[0]) is not False:
                print(f"Файл {file_name} уже есть в базе данных, он не будет добавлен в архив!")
            else:
                document = my_mongo_collection.find_one({"hash": file_hash[0]})
                archive_info = document['info']
                try:
                    archive_channel = document['channel']
                except Exception as e:
                    archive_channel = 'site'

                with zipfile.ZipFile(
                        f"{archive_name}", mode="w"
                ) as archive:
                    archive.write(file, basename(file))
                    archive.write(info_file, basename(info_file))
                    time.sleep(2)

                archive_size = Size(ArchiveSize(archive.filename))
                print(
                    f"Выполнено {count} из {len(files)}, размер архива: ",
                    archive_size,
                )
                merge = MergeMongoDocuments(archive_name, archive_size, archive_info, archive_channel, file_hash,
                                            my_mongo_collection, work_mongo_collection)
                if not merge:
                    print("Архив может быть удален, так как в MongoDB отсутствует информация о файлах!")
                    if DeleteOneArchive():
                        DeleteEmptyArchive(archive_name)

    print("\nАрхивы в папке ", folders.get("directory"), "\n")
    return


def MergeMongoDocuments(archive_name, archive_size, archive_info, archive_channel, file_hashes, my_mongo_collection,
                        work_mongo_collection):
    print("Выполняется создание общего документа MongoDB...")
    merged_document = {'archive_name': archive_name, 'archive_channel': archive_channel, 'archive_size': archive_size,
                       'archive_info': archive_info, 'files': []}  # Создаем новый документ
    for file_hash in file_hashes:
        existing_doc = my_mongo_collection.find_one({'hash': file_hash})
        if existing_doc:
            merged_document['files'].append(existing_doc)
            # my_mongo_collection.delete_one({'_id': existing_doc['_id']})  # Удаляем документ из коллекции
        else:
            print(f"Не найден документ по указанным данным: \n"
                  f"\t\t\t\t ХЭШ файла: {file_hash}\n"
                  f"\t\t\t\t Имя архива: {archive_name}\n"
                  f"\t\t\t\t Канал: {archive_channel}\n"
                  f"\t\t\t\t Информация об архиве: {archive_info}\n")
    if not merged_document['files']:
        return False
    work_mongo_collection.insert_one(
        merged_document)  # Вставляем новый объединенный документ в коллекцию tmp.file_hashes
    my_mongo_collection.insert_one(
        merged_document)  # Вставляем новый объединенный документ в локальную коллекцию NewCol.file_hashes
    print("Данные сохранены!")
    return True


def DeleteEmptyArchive(name):
    archives = Archives_Path()[0]
    for archive in archives:
        archive_name = os.path.basename(archive)
        if name == archive_name:
            try:
                os.remove(name)
            except Exception as e:
                print("Не удается удалить архив!")
                print(e)


def CheckIsInfofile(file):
    if ((os.path.basename(file) == 'info_file.txt') or (os.path.basename(file) == 'file_info.txt')
            or (os.path.basename(file) == 'Files_List.txt') or (os.path.basename(file) == 'sum_all_rows.txt')):
        return True
    elif round(os.path.getsize(file) / 1024 ** 2, 3) > 2:
        return False
    elif os.path.basename(file).endswith(".txt"):
        with open(file, 'r', encoding='utf-8', errors='ignore') as info_file:
            content = info_file.read()
            if "Имя файла: " and "Размер файла: " and "Название канала: " in content:
                return True
        return False
    else:
        return False


def CreateArchiveOfCollections(collection_files, work_mongo_collection, my_mongo_collection):
    count = 0
    archive_info = ''
    file_names = [os.path.basename(file) for file in collection_files]
    file_hashes = []
    collection_name = Input_Archive_Name()
    collection_name = CheckExistArchiveNameInMongo(work_mongo_collection, collection_name, j=1)
    collection_name = CheckExistArchiveNameInFolder(collection_name, j=1)

    with zipfile.ZipFile(f"{collection_name}", mode="w") as archive:
        for i in range(len(collection_files)):
            file = collection_files[i].replace("\\", "\\\\")
            if not CheckIsInfofile(collection_files[i]):
                print(f"\nВыполняется сбор ХЭШа для файла {file_names[i]}...")
                try:
                    file_hash = TakeHash(file)
                except Exception as e:
                    print(e)
                    continue
                if CheckFilehashInMongo(work_mongo_collection, file_hash) is not False:
                    print(f"Файл {file_names[i]} уже есть в базе данных, он не будет добавлен в архив!")
                    continue
                else:
                    file_hashes.append(file_hash)
                    document = my_mongo_collection.find_one({"hash": file_hash})
                    archive_info += f"{document['info']}\n"
                    try:
                        archive_channel = document['channel']
                    except Exception as e:
                        archive_channel = 'site'
            # elif file_names[i] == 'info_file.txt':
            #     count += 1
            count += 1
            print(f"Добавление файла {file_names[i]} в архив...")
            archive.write(file, basename(file))
            print("Файлов в архиве: ", count)
            time.sleep(2)
    if count == 0:
        print(f"\nАрхив {collection_name} будет удалён, так как он пуст.")
        DeleteEmptyArchive(collection_name)
    else:
        archive_size = Size(ArchiveSize(archive.filename))
        print("\nРазмер архива: ", archive_size)
        print("Архив в папке ", folders.get("directory"), "\n")
        merge = MergeMongoDocuments(collection_name, archive_size, archive_info, archive_channel, file_hashes,
                                    my_mongo_collection, work_mongo_collection)
        if not merge:
            print("Архив может быть удален, так как в MongoDB отсутствует информация о файлах!")
            if DeleteOneArchive():
                DeleteEmptyArchive(collection_name)
    return


def MailPassCountryArchiveCreation(collection_files, work_mongo_collection, my_mongo_collection):
    count = 0
    archive_info = ''
    archive_channel = ''
    file_names = [os.path.basename(file) for file in collection_files]
    file_hashes = []
    collection_name = 'mail_pass_country.7z'
    print("Имя коллекции: ", collection_name)
    collection_name = CheckExistArchiveNameInMongo(work_mongo_collection, collection_name, j=1)
    collection_name = CheckExistArchiveNameInFolder(collection_name, j=1)
    with zipfile.ZipFile(f"{collection_name}", mode="w") as archive:
        for i in range(len(collection_files)):
            file = collection_files[i]
            if not CheckIsInfofile(file):
                print(f"Выполняется сбор ХЭШа для файла {file_names[i]}...")
                try:
                    file_hash = TakeHash(file)
                except Exception as e:
                    print(e)
                    continue
                if CheckFilehashInMongo(work_mongo_collection, file_hash) is not False:
                    print(f"Файл {file_names[i]} уже есть в базе данных, он не будет добавлен в архив!")
                    continue
                else:
                    file_hashes.append(file_hash)
                    document = my_mongo_collection.find_one({"hash": file_hash})
                    if document:
                        archive_info += f"{document['info']}\n"
                        try:
                            archive_channel = document['channel']
                        except Exception as e:
                            print(e)
                            archive_channel = 'site'
            count += 1
            print(f"Добавление файла {file_names[i]} в архив...")
            archive.write(file, basename(file))
            print("Файлов в архиве: ", count)
            time.sleep(2)
    if count == 0:
        print(f"\nАрхив {collection_name} будет удалён, так как он пуст.")
        DeleteEmptyArchive(collection_name)
        return None
    else:
        archive_size = Size(ArchiveSize(archive.filename))
        print("\nРазмер архива: ", archive_size)
        print("Архив в папке ", folders.get("directory"), "\n")
        merge = MergeMongoDocuments(collection_name, archive_size, archive_info, archive_channel, file_hashes,
                                    my_mongo_collection, work_mongo_collection)
        if not merge:
            print("Архив будет удален, так как в MongoDB отсутствует информация о файлах!")
            DeleteEmptyArchive(collection_name)
            return None
    return


def DeleteCollections(files, descriptions, collections, all_count):
    s = input("Для удаления всех коллекций нажмите 1. Для отмены операции нажмите 0: ")
    if s == '1':
        print("\nВыполняется...\n")
        for collection in collections:
            name = collection.replace(folders.get("collection_files_path") + "\\", "")
            if name == 'mail_pass_country':
                print("Количество записей в mail_pass_country: ", all_count)
                continue
            if os.path.exists(collection):
                shutil.rmtree(collection)
                print(f"Коллекция ", name, " была успешно удалена.")

            else:
                print(f"Коллекция {collection} не существует.")
        return MenuArchiveWork(files, descriptions, collections)
    elif s == '0':
        return MenuArchiveWork(files, descriptions, collections)
    else:
        print("Введите номер пункта по условию!")
        return DeleteCollections(files, descriptions, collections)


def CheckCorrectExecution(files, images):
    if (len(files) == len(images)) and (len(files) != 0 and len(images) != 0):
        rez = True
        return rez
    else:
        rez = False
        print(
            "Количество скриншотов и файлов не совпадают, операция не может быть выполнима!\n"
        )
        return rez


def Delete_Memory(files, images, collections):
    s = input("Уверены? нажмите 1. Для отмены операции нажмите 0: ")
    print("\nВыполняется...\n")
    if s == "1":
        for j in range(len(files)):
            filename = files[j].replace(folders.get("files_path") + "\\", "")
            file = files[j].replace("\\", "\\\\")
            os.remove(file)
            print(f"Файл {filename} удален")
        print("\n")
        for j in range(len(images)):
            imagename = images[j].replace(folders.get("images_path") + "\\", "")
            image = images[j].replace("\\", "\\\\")
            os.remove(image)
            print(f"Файл метаинформации {imagename} удален")
        return MainMenu()
    elif s == "0":
        return MenuArchiveWork(files, images, collections)
    else:
        print("Введите 1 или 0 по условию!")
        Delete_Memory(files, images, collections)


def Delete_Archives(files, images, archives, collections):
    for j in range(len(archives)):
        archive_name = archives[j].replace(folders.get("directory") + "\\", "")
        archive = archives[j].replace("\\", "\\\\")
        os.remove(archive)
        print(f"Архив {archive_name} удален")
    return AskForDelete(files, images, collections)


def AskForDelete(files, images, collections):
    archives = Archives_Path()[0]
    ch = input(
        "\nВыберите вариант очистки памяти\n"
        "1. Для удаления всех файлов и описаний нажмите 1\n"
        f"2. Для удаления всех архивов, нажмите 2 (повторяющиеся архивы останутся в папке {folders.get('archives_with_duplicates')})\n"
        "3. Для выхода в главное меню, нажмите 0\n"
        "Выбор: "
    )
    if ch == "1":
        if len(files) == 0 and len(images) == 0:
            print("Очистка не требуется, файлов и описаний нет!")
            return AskForDelete(files, images, collections)
        else:
            Delete_Memory(files, images, collections)
    elif ch == "0":
        print("\n")
        return MainMenu()
    elif ch == "2":
        if len(archives) == 0:
            print("Очистка не требуется,  архивов нет!")
            return AskForDelete(files, images, collections)
        else:
            Delete_Archives(files, images, archives, collections)
    else:
        print("Введите номер пункта!")
        return AskForDelete(files, images, collections)


def AutoCheckFilesForHash(archives_dup, dup_directory):
    recurring_names = []
    for i in range(len(archives_dup)):
        archive1_name = archives_dup[i].replace(dup_directory + "\\", "")
        if 'from_server_' in archive1_name:
            archive1_name = archive1_name.replace('from_server_', "")
        for j in range(i + 1, len(archives_dup)):
            archive2_name = archives_dup[j].replace(dup_directory + "\\", "")
            if 'from_server_' in archive2_name:
                archive2_name = archive2_name.replace('from_server_', "")
            if archive1_name == archive2_name:
                if HashSumCheck(archives_dup[i], archives_dup[j]):
                    print(f"Файл {archive1_name} идентичен файлу {archive2_name}")
                    recurring_names.append(archive1_name)
                    time.sleep(3)
    if not recurring_names:
        print("\nВсе повторяющиеся по названию архивы уникальны и отличаются от архивов на сервере.\n"
              "Архивы будут перемещены в исходную папку для загрузки на сервер.")
    else:
        print("\nВсе повторяющиеся по названию архивы останутся в текущей папке.\n"
              "Уникальные архивы будут перемещены в исходную папку для загрузки на сервер.")
        time.sleep(1)
        directory = folders.get("directory")
        for archive in archives_dup:
            archive_name = archive.replace(dup_directory + "\\", "")
            if 'from_server_' in archive_name:
                archive_name = archive_name.replace('from_server_', "")
            if archive_name not in recurring_names:
                shutil.move(archive, directory)
        print(f"\nАрхивы перенесены в {directory}")
        time.sleep(1)
    return


def CheckFilesForHash():
    app1 = App1()
    app1.mainloop()
    file1 = files_to_hash.get("filepath1")
    file2 = files_to_hash.get("filepath2")
    if HashSumCheck(file1, file2):
        print(f"Файл {file1} идентичен файлу {file2}")
    else:
        print(f"\nФайл {file1} отличается от файла {file2}")
    tmp = input(
        "\nЧтобы проверить другие файлы, нажмите 1, для пропуска этапа - 0:\n" "Выбор: "
    )
    if tmp == "1":
        CheckFilesForHash()
    elif tmp == "0":
        return
    else:
        print("Некорректный ввод! Этап будет пропущен!")
        return


def DownloadFromServerMenu(token, archives_dup):
    ch = input(
        "Для загрузки повторяющихся архивов с сервера нажмите 1, для продолжения без скачивания, нажмите 0:\n"
        "Выбор: "
    )
    if ch == "1":
        dup_directory = DownloadFromSeafile(token, archives_dup, server_url, repo_id)
        ch1 = input(
            "Для автоматической проверки файлов на соответствие нажмите 1\n"
            "Для проверки файлов вручную, нажмите 2\n"
            "Для пропуска этапа, нажмите 0\n"
            "Выбор: "
        )
        if ch1 == "1":
            AutoCheckFilesForHash(archives_dup, dup_directory)
        elif ch == "2":
            CheckFilesForHash()
        elif ch1 == "0":
            return
        else:
            print("Некорректный ввод! Этап будет пропущен!")
            return
    elif ch == "0":
        return
    else:
        print("Введите 1 или 0 по условию!\n")
        DownloadFromServerMenu(token, archives_dup)


def DeleteOneArchive():
    s = input("\nДля удаления архива, нажмите 1, для продолжения без удаления - 0:\n" "Выбор: ")
    if s == "1":
        return True
    elif s == "0":
        return False
    else:
        print("Введите 1 или 0!")
        return Agree()


def Agree():
    s = input("\nДля продолжения, нажмите 1, для выхода - 0:\n" "Выбор: ")
    if s == "1":
        return True
    elif s == "0":
        return False
    else:
        print("Введите 1 или 0!")
        return Agree()


async def GetFileEntityInArchive(file):
    return await TakeEntityInArchiveFile(file)


def CreateMongoDocFromSiteFiles(my_mongo_collection, work_mongo_collection):
    description_folder = folders.get("images_path")
    files_and_folders = ChooseFiles()
    files = files_and_folders[0]
    chosen_folders = files_and_folders[1]
    if chosen_folders:
        for folder in chosen_folders:
            dop_files = list(filter(os.path.isfile, glob.glob(f"{folder}/*") + glob.glob(f"{folder}/.*")))
            files += dop_files
    for file in files:
        found = False
        file_name = os.path.basename(file)
        file_type = 'folder' if os.path.isdir(file) else os.path.splitext(file_name)[1][1:]
        file_size = Size(os.path.getsize(file))
        download_from_site_time = datetime.fromtimestamp(os.path.getctime(file)).strftime('%Y-%m-%d %H:%M:%S')
        file_hash = TakeHash(file)
        if CheckFilehashInMongo(work_mongo_collection, file_hash) is not False:
            print(f"Файл {file_name} уже есть в базе данных, он будет пропущен!")
            continue
        for file_info_name in os.listdir(description_folder):
            if file_name.rsplit(".", 1)[0] == file_info_name.rsplit(".", 1)[0]:
                found = True
                file_name = CheckExistArchiveNameInMongo(work_mongo_collection, file_name, j=1)
                file_name = CheckExistArchiveNameInMongo(my_mongo_collection, file_name, j=1)
                description_file_path = os.path.join(description_folder, file_info_name)
                with open(description_file_path, 'r', encoding='utf-8') as file_info:
                    info = file_info.read().strip()
        if not found:
            print(f"Файл-описание для файла {file_name} не найден!\n"
                  f"Документ MongoDB не будет создан для этого файла.")
            info = ''
            continue
        loop = asyncio.get_event_loop()
        file_entity = loop.run_until_complete(GetFileEntityInArchive(file))
        document = {
            "name": file_name,
            "hash": file_hash,
            "size": file_size,
            "type": file_type,
            "download_from_site_time": download_from_site_time,
            "info": info
        }
        if file_entity is not None:
            document["in_archive"] = file_entity

        print(f"\nДобавление данных файла {file_name} в MongoDB...")
        my_mongo_collection.insert_one(document)
        time.sleep(1)
        print("Данные добавлены!")
        continue
    return


def MenuArchiveWork(files, images, collections):
    os.chdir(folders.get("directory"))
    try:
        work_mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
        my_mongo_collection = ConnectToMongo(collection_parameters=MyMongoCollection())
    except Exception as e:
        print("Не удается подключиться к коллекции MongoDB")
        print(e)
        return MainMenu()
    s = input(
        "\nДействия\n"
        "1. Если хотите создать отдельный архив для каждого файла, нажмите 1\n"
        "2. Если хотите создать архивы из коллекций файлов, нажмите 2\n"
        "3. Чтобы создать документы с метоинформацией в MongoDB для загруженных файлов не с телеграмм каналов, "
        "нажмите 3\n"
        "4. Для очистки памяти, нажмите 4\n"
        "5. Для переноса архивов в For sort нажмите 5\n"
        "6. Для выхода в главное меню нажмите 0\n"
        "Выбор: "
    )
    if s == "1":
        print("\nСоздание архивов...")
        Create_Many_Archives(files, images, work_mongo_collection, my_mongo_collection)
        AskForDelete(files, images, collections)
        return MenuArchiveWork(files, images, collections)
        # if CheckCorrectExecution(files, descriptions):
        #     print("\nСоздание архивов...")
        #     Create_Many_Archives(files, descriptions)
        #     AskForDelete(files, descriptions, collections)
        # else:
        #     MenuArchiveWork(files, descriptions, collections)
    elif s == "2":
        if not collections:
            print("\nДобавьте коллекции для работы с ними!")
            return MenuArchiveWork(files, images, collections)
        try:
            all_count = 0
            for collection in collections:
                if collection.replace(folders.get("collection_files_path") + "\\", "") == 'mail_pass_country':
                    collection_files = list(filter(os.path.isfile, glob.glob(f"{collection}/*")))
                    if not collection_files:
                        print(f"Нет файлов в коллекции {os.path.basename(collection)}")
                        continue
                    print("Проверка объема записей для mail_pass_country...")
                    for file in collection_files:
                        if os.path.basename(file) == 'info_file.txt':
                            continue
                        try:
                            with open(file, 'r', encoding='utf-8', errors='replace') as data_file:
                                for line in data_file:
                                    all_count += 1
                        except Exception as e:
                            print(e)
                            exit(0)
                    if all_count > 100000000:
                        len_mail_pass_country_path = collection.join('sum_all_rows.txt')
                        with open(len_mail_pass_country_path, 'w', encoding='utf-8') as file:
                            file.write(str(all_count))
                        print(
                            "Количество записей в файлах больше 100 млн, архивация файлов будет выполнена, "
                            " коллекция будет загружена в tmp.\n"
                            f"Количество записей {all_count} ")
                        print("\nКоллекция:", os.path.basename(collection), '\t Файлы: ')
                        for file in collection_files:
                            size = os.path.getsize(file)
                            print("\t", os.path.basename(file), " ", Size(size))
                        MailPassCountryArchiveCreation(collection_files, work_mongo_collection, my_mongo_collection)
                        # LoadMailPassCountryToMongo(all_dataframe)
                    else:
                        print("Количество записей еще не превысило 100 млн!\n"
                              f"Текущее количество записей: {all_count}\n"
                              "Коллекция mail_pass_country будет пропущена")
                else:
                    print(
                        f"\nВыполняется архивация коллекции... \t\t {collections.index(collection) + 1} из {len(collections)}")
                    print("\nКоллекция:", os.path.basename(collection),
                          '\t Файлы:')
                    collection_files = list(filter(os.path.isfile, glob.glob(f"{collection}/*")))
                    if not collection_files:
                        print(f"Нет файлов в коллекции {os.path.basename(collection)}")
                        continue
                    for file in collection_files:
                        size = os.path.getsize(file)
                        collection_filepath = os.path.join(
                            folders.get("collection_files_path"), collection
                        )
                        print("\t" * 6, file.replace(collection_filepath + "\\", ""), " ", Size(size))
                    CreateArchiveOfCollections(collection_files, work_mongo_collection,
                                               my_mongo_collection)
            return DeleteCollections(files, images, collections, all_count)
        except Exception as e:
            print("Не удается подключиться к MongoDB!")
            logging.error("An error occurred: %s", e)
            return MenuArchiveWork(files, images, collections)

    elif s == "3":
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        CreateMongoDocFromSiteFiles(my_mongo_collection, work_mongo_collection)
        return MenuArchiveWork(files, images, collections)
    elif s == "4":
        AskForDelete(files, images, collections)
        return MenuArchiveWork(files, images, collections)
    elif s == "5":
        # loop = asyncio.get_event_loop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        parameters = loop.run_until_complete(ConnectionToSeafile(login_name, password))
        repo = parameters[0]
        token = parameters[1]
        loaded_to_mongo_repo_id = parameters[2]
        loaded_to_mongo_token = parameters[3]
        # repo = ConnectionToSeafile(login_name, password)[0]
        # token = ConnectionToSeafile(login_name, password)[1]
        print("\nУспешное подключение к библиотеке Seafile!\n")
        time.sleep(2)
        archives = Archives_Path()[0]
        if CheckForArchives(archives):
            names_of_duplicate = []
            print("\nПроверка архивов на наличие уже существующих в папке For sort...")
            if TestArchiveForDuplicates(repo, archives, names_of_duplicate):
                if Agree():
                    MoveArchives(archives, names_of_duplicate)
                    archives_dup = Archives_Path()[1]
                    DownloadFromServerMenu(token, archives_dup)
                    archives = Archives_Path()[0]
                    archive_path = folders.get("directory")
                    os.chdir(archive_path)
                    UploadToSeafile(token, archives, server_url, repo_id, loaded_to_mongo_repo_id,
                                    loaded_to_mongo_token)
                    loop.close()
                    MenuArchiveWork(files, images, collections)
                else:
                    loop.close()
                    MenuArchiveWork(files, images, collections)
            else:
                if Agree():
                    UploadToSeafile(token, archives, server_url, repo_id, loaded_to_mongo_repo_id,
                                    loaded_to_mongo_token)
                    loop.close()
                    MenuArchiveWork(files, images, collections)
                else:
                    loop.close()
                    MenuArchiveWork(files, images, collections)
        else:
            print("Архивов нет!")
            loop.close()
            MenuArchiveWork(files, images, collections)
    elif s == "0":
        print("\n")
        return MainMenu()
    else:
        print("Введите номер пункта по условию!\n")
        MenuArchiveWork(files, images, collections)


async def Run():
    stop_running = False
    while not stop_running:
        client = await ConnectToTelegram()
        try:
            stop_running = await TelegramWorkMenu(client, channel=None)
        except Exception as e:
            print(e)
            print("Завершение работы клиента...")
            await client.disconnect()
            stop_running = True
        finally:
            await client.disconnect()
            await cancel_all_tasks()
    return stop_running


async def cancel_all_tasks():
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
    # await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)


def MainMenu():
    stop_running = False
    s = input(
        "\nГлавное меню\n"
        "1. Для работы с файлами и архивами, нажмите 1\n"
        "2. Для работы с Telegram, нажмите 2\n"
        "3. Для изменения рабочих папок, нажмите 3\n"
        "4. Для выхода из программы нажмите 0\n"
        "Выбор: "
    )
    if s == "1":
        CheckForWork()
    elif s == "2":
        os.chdir(session_telegram)
        while not stop_running:
            stop_running = asyncio.run(Run())
        return MainMenu()

        # if client is False:
        #     return MainMenu()
        # else:
        #     TelegramWorkMenu(client, channel=None)
        #     return MainMenu()
    elif s == "3":
        return ChooseFolders()
    elif s == "0":
        print("Завершение работы программы...")
        exit(0)
    else:
        print("Введите номер пункта по условию!\n")
        return MainMenu()


def ChooseFiles():
    app = App2()
    print("\nВыберите необходимые файлы или папки c файлами.\n")
    app.mainloop()
    return app.files, app.collection_folders


def ChooseFolders():
    app = App()
    print("Выберите или создайте необходимые папки для работы.")
    app.mainloop()
    if folders.get("files_path") == folders.get("images_path"):
        print("\nПапка с файлами не может совпадать с папкой с метаинформацией!")
        return ChooseFolders()
    elif folders.get("directory") == folders.get("archives_with_duplicates"):
        print(
            "\nПапка с архивами не может совпадать с папкой с повторяющимися архивами!"
        )
        return ChooseFolders()
    elif folders.get("files_path") == folders.get("collection_files_path"):
        print(
            "\nПапка с одиночными файлами не может совпадать с папкой для коллекции файлов!"
        )
        return ChooseFolders()
    else:
        return MainMenu()


if __name__ == "__main__":
    # CheckFilesForHash()
    # d = r'D:/Bases/finland_db'
    # work_mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    # files = list(filter(os.path.isfile, glob.glob(f"{d}/*") + glob.glob(f"{d}/.*")))
    # for file in files:
    #     hash = TakeHash(file)
    #     filename = os.path.basename(file)
    #     if CheckFilehashInMongo(work_mongo_collection, hash) is not False:
    #         print(f"Файл {filename} уже есть в базе данных, он будет пропущен!")
    #     else:
    #         print(f"Файла {filename} нет в базе данных")

    ChooseFolders()
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Программа выполнилась за {total_time} секунд.")
