import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from telethon import TelegramClient, errors
# from MyApp_Air import (cancel_all_tasks, CheckExistArchiveNameInFolder, CheckExistArchiveNameInMongo, CheckIsInfofile,
#                        ArchiveSize, DeleteEmptyArchive, MergeMongoDocuments, FilesMetainfoCollectionPaths,
#                        TakeSizeFromInfofile)
# from work_with_telegram_Air import (FilenameFromFiles, DisplayClusters, RenameDuplicates, Link_to_File, Size,
#                                     CheckConnect,
#                                     GetMetaInfoInInternet, CompareFilenames, AI_Analyse_For_Collection, StartDownload,
#                                     DeleteCollectionFolder, ConnectToMongo,
#                                     TmpMongoCollection, TakeHash, CheckFilehashInMongo, CheckUploadTimeInMongo)

from seafileapi import SeafileAPI
# from work_with_telegram_Air import *
from difflib import SequenceMatcher
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import types
from os.path import basename
from functools import wraps
import os
import glob
import re
# from MyApp_Air import *
from MyApp_Air import (cancel_all_tasks, CheckExistArchiveNameInMongo, CheckExistArchiveNameInFolder, CheckIsInfofile,
                       DeleteEmptyArchive, ArchiveSize, MergeMongoDocuments, TakeSizeFromInfofile,
                       FilesMetainfoCollectionPaths)
from work_with_telegram_Air import *

# from set_parameters import *

# api_id = "28424267"
# api_hash = "1f0defdcfcd12aaebeb3b60ad8258e35"
#
# folders_Air = {"files_path": '/mnt/telegram', "images_path": '/mnt/infofiles',
#            "directory": '/mnt/archives',
#            "archives_with_duplicates": '/mnt/archives_dup',
#            "collection_files_path": '/mnt/collection_files'}

# session_telegram = r"/mnt/session_telegram"
#
# work_folder = r"/mnt/analitics"
#
# repo_id = "d2edeb85-daea-4ed7-8546-b644c3f411c6"
# server_url = "https://files.mobiledep.ru/"
# login_name = "amalkov@mobiledep.ru"
# password = "GazSqLBczd"
# parent_dir = "/"

# folders_Air = {"files_path": 'C:/Users/and23/Downloads/Telegram Desktop', "images_path": 'D:/test_infofiles',
#                     "directory": 'D:/test_archive',
#                     "archives_with_duplicates": 'D:/test_archive_dup',
#                     "collection_files_path": 'D:/test_collection_files_paths'}
#
# session_telegram = r"D:\session_telegram"
#
# work_folder = r"D:\analitics"

# files_path = r""
# images_path = r""
# archives_with_duplicates = r""
# directory = r""
# collection_files_path = r""

# analitic_folders = {
#     "work_folder": work_folder,
# }


# folders_Air = {"files_path": files_path, "images_path": images_path, "directory": directory,
#            "archives_with_duplicates": archives_with_duplicates, "collection_files_path": collection_files_path}

# folders_Air.update({"files_path": folders_Air.get("files_path")})
# folders_Air.update({"images_path": folders_Air.get("images_path")})
# folders_Air.update({"directory": folders_Air.get("directory")})
# folders_Air.update({"archives_with_duplicates": folders_Air.get("archives_with_duplicates")})
# folders_Air.update({"collection_files_path": folders_Air.get("collection_files_path")})

session_telegram = folders_Air.get("session_telegram")


# Функция собирает все имена, размер (в байтах) и id сообщений из тг канала
def GetMessageNamesSizesId(messages_list):
    mass_names = []
    mass_id = []
    mass_sizes_for_check = []
    for current_index, message in enumerate(messages_list[:-1]):
        if message.media or message.file:
            try:
                if message.file.name is not None:
                    mass_names.append(message.file.name)
                    mass_id.append(message.id)
                    try:
                        mass_sizes_for_check.append(message.media.document.size)
                    except Exception as e:
                        print(e)
                        mass_names.pop(-1)
                        mass_id.pop(-1)
            except Exception as e:
                print(e)
                continue
    return mass_names, mass_sizes_for_check, mass_id


# функция находит id сообщений, которые соответствуют загруженным файлам
def FindMessageIdForFile(file_names, mass_sizes_for_check, mass_id):
    new_mass_id = []
    filenames_in_folder = []
    downloaded_files_paths = folders_Air.get("files_path")
    files = list(
        filter(os.path.isfile, glob.glob(f"{downloaded_files_paths}/*") + glob.glob(f"{downloaded_files_paths}/.*")))
    for file in files:
        filenames_in_folder.append(FilenameFromFiles(file)[0])
    print("\nВыполняется поиск id сообщений для загруженных ранее файлов...\n")

    count = 0
    for name in file_names:
        name_size = mass_sizes_for_check[count]

        name_parts = name.rsplit(".", 1)
        if len(name_parts) == 2:
            base_name, ext = name_parts
            num = re.search(" \(\d+\).*[\s\w0-9]*", base_name)
            if num:
                base_name = base_name.rsplit(num.group(), 1)[0]
        else:
            base_name, ext = name, ''
        for file in files:
            filename, file_base_name, ext_file = FilenameFromFiles(file)
            file_size = os.path.getsize(file)

            if file_base_name == base_name and ext_file == ext and file_size == name_size:
                new_mass_id.append(mass_id[count])
        count += 1
    return new_mass_id


# это для очистки имен файлов
def PreCleanFilename(filename):
    filename = re.sub(r'\s*\(\d+\)', '', filename)
    filename = re.sub(r'\s*-\s*\d+\.[a-z0-9]+$', '', filename, flags=re.IGNORECASE)
    filename = re.sub(r'\.part\d+|\.\d+$', '', filename, flags=re.IGNORECASE)
    filename = re.sub(r'\.\d+', '', filename, flags=re.IGNORECASE)
    filename = os.path.splitext(filename)[0]
    filename = re.sub(r'[^\wа-яА-ЯёЁ]+', ' ', filename)
    filename = re.sub(r'\s+', ' ', filename).strip().lower()
    filename = re.sub(r'\d+$', '', filename, flags=re.IGNORECASE)
    return filename


# сравнение имен
def AreSimilar(file1, file2, threshold=0.87):
    seq = SequenceMatcher(None, file1, file2)
    return seq.ratio() >= threshold


# переформировывание кластеров
def Restructure(clusters):
    new_clusters = defaultdict(list)
    new_cluster_id = 1
    for cluster_id, filenames in clusters.items():
        cleaned_files = [PreCleanFilename(f) for f in filenames]
        for filename, cleaned in zip(filenames, cleaned_files):
            added_to_cluster = False
            for new_id, cluster_files in new_clusters.items():
                if any(AreSimilar(cleaned, PreCleanFilename(f)) for f in cluster_files):
                    new_clusters[new_id].append(filename)
                    added_to_cluster = True
                    break
            if not added_to_cluster:
                new_clusters[new_cluster_id].append(filename)
                new_cluster_id += 1
    new_clusters = RemoveDupInClusters(new_clusters)
    DisplayClusters(new_clusters)
    return new_clusters


def CreateDirectoryForCollectionAir(path_to_files, collection_names, index):
    collection_name = collection_names[index]
    test_path_to_files = os.path.join(
        path_to_files,
        f"collection_{collection_name}",
    )
    if not os.path.exists(test_path_to_files):
        os.makedirs(test_path_to_files)
        path_to_info = test_path_to_files
        print(f"Коллекция с номером {collection_name}...")
        return test_path_to_files, path_to_info
    else:
        print(f"Папка коллекции с именем collection_{collection_name} уже существует!")
        collection_name += 1
        collection_names[index] = collection_name
        return CreateDirectoryForCollectionAir(path_to_files, collection_names, index)


limit_memory = False


def is_disk_limit_exceeded(limit_gb=50):
    global limit_memory
    total, used, free = shutil.disk_usage("/")
    used_gb = used // (1024 ** 3)
    print(f"Используется на диске: {used_gb} Гб из {total // (1024 ** 3)} Гб")
    limit_memory = True if used_gb >= limit_gb else False
    return limit_memory


def IsNeedFile(filename):
    image_extensions = (".jpeg", ".png", ".jpg", ".bmp", ".dib", ".rle", ".pdf", ".gif", ".tiff", ".tgs")
    scripts_extensions = (".html", ".css", ".php", ".py", ".asp", ".xml", ".htm", ".exe")
    audios_extensions = (".mp3", ".wav", ".ogg", ".flac", ".wma")
    video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".webp")
    all_ext = image_extensions + scripts_extensions + audios_extensions + video_extensions
    return True if not filename.lower().endswith(all_ext) else False


async def DownloadFilesInFlow(client, channel, num, work_collection, marker_of_download):
    if not client.is_connected():
        print("Потеря подключения к клиенту.")
    messages_list = []
    count = 0
    channel_id = channel[1]
    # формируем список всех сообщений
    async for message in client.iter_messages(channel_id, limit=num + 1):
        messages_list.append(message)
    mass_names = []
    mass_id = []
    mass_sizes = []
    mass_sizes_for_check = []
    print("Список всех файлов и их метаинформация: \n")
    for current_index, message in enumerate(messages_list[:-1]):
        time.sleep(5)
        if message.media or message.file:
            try:
                if message.file.name is not None:
                    mass_names.append(message.file.name)
                    mass_id.append(message.id)
                    try:
                        mass_sizes_for_check.append(message.media.document.size)
                        if Size(message.media.document.size) is not None:
                            mass_sizes.append(Size(message.media.document.size))
                        else:
                            mass_sizes.append("-")
                    except Exception:
                        mass_sizes.append("-")
                        mass_sizes_for_check.append(0)
                    print(
                        count + 1,
                        "\t" * 2,
                        message.file.name,
                        "\t" * 2,
                        Size(message.media.document.size),
                        "\nИнформация из текста сообщения: \t",
                        message.text,
                    )
                    count += 1
                    if count % 10 == 0:
                        await CheckConnect(client)
            except Exception as e:
                logging.error("An error occurred: %s", e)
                continue

    if not mass_names:
        print("Файлов для загрузки нет!")
        return
    mass_names = RenameDuplicates(mass_names)
    if not mass_names:
        print("Файлов для загрузки нет!")
        return
    check = list(set(mass_names))
    if len(mass_names) != len(check):
        print("Где-то функция создает дупликаты!")
        return

    await CheckConnect(client)
    print("Выполняется загрузка файлов...")
    path_to_files = folders_Air.get("files_path")
    count = 0
    new_mass_names = list(reversed(mass_names))
    new_mass_id = list(reversed(mass_id))
    new_mass_sizes = list(reversed(mass_sizes))
    print(new_mass_names)
    print(new_mass_id)
    print(new_mass_sizes)

    for i in range(len(mass_id)):
        if not IsNeedFile(new_mass_names[i]):
            continue
        try:
            count += 1
            count = await StartDownloadFiles(
                count,
                i,
                client,
                channel_id,
                num,
                work_collection,
                new_mass_names,
                new_mass_id,
                new_mass_sizes,
                path_to_files
            )
        except Exception as e:
            logging.error("An error occurred: %s", e)
            continue
        if is_disk_limit_exceeded():
            print("Достигнут лимит по дисковому пространству. Прерываем загрузку.")
            return True  # сигнал завершения загрузки


async def get_all_dialogs(client):
    dialogs = []
    async for dialog in client.iter_dialogs():
        dialogs.append(dialog)
    return dialogs


async def DownloadInfoInFlow(client, channel, num, my_collection, work_collection, marker_of_download):
    if not client.is_connected():
        print("Потеря подключения к клиенту.")
    meta_info = "-"
    messages_list = []
    count = 0
    channel_id = channel[1]
    channel_name = None

    dialogs = await get_all_dialogs(client)

    for dialog in dialogs:
        if channel_id == dialog.entity.id:
            channel_name = dialog.name

    if not channel_name or channel_name == "" or channel_name == " " or channel_name == "\t" or channel_id == 1938105511:
        channel_title = channel[0]
        if not channel_title:
            channel_name = channel_id
        else:
            channel_name = channel_title
    channel_title = channel[0]
    if not channel_title:
        channel_title = channel_id
    # формируем список всех сообщений
    async for message in client.iter_messages(channel_id, limit=num + 1):
        messages_list.append(message)
    compared_mass_id = []
    if marker_of_download is False:
        mass_names, mass_sizes, mass_id = GetMessageNamesSizesId(messages_list)
        mass_names = RenameDuplicates(mass_names)
        compared_mass_id = FindMessageIdForFile(mass_names, mass_sizes, mass_id)
        if not compared_mass_id:
            print(f"Нет сообщений в канале {channel_name} для загруженных файлов!")
            time.sleep(2)
            return

    mass_names = []
    mass_id = []
    mass_links = []
    mass_info = []
    mass_sizes = []
    mass_sizes_for_check = []
    mass_time = []
    print("Список всех файлов и их метаинформация: \n")
    for current_index, message in enumerate(messages_list[:-1]):
        prev_message = messages_list[current_index - 1] if current_index > 0 else None
        next_message = messages_list[current_index + 1] if current_index < len(messages_list) - 1 else None
        if message.media or message.file:
            try:
                if message.file.name is not None:
                    if marker_of_download is False:
                        if message.id not in compared_mass_id:
                            continue
            except Exception as e:
                print(e)
                continue
            try:
                if message.file.name:
                    mass_names.append(message.file.name)
                    mass_id.append(message.id)
                    mass_time.append(message.date)
                    try:
                        mass_links.append(
                            await Link_to_File(
                                client, message.id, channel_title, channel_id
                            )
                        )
                    except Exception as e:
                        logging.error("An error occurred: %s", e)
                        mass_links.append("Ссылка на сообщение не доступна!")

                    try:
                        mass_sizes_for_check.append(message.media.document.size)
                        if Size(message.media.document.size) is not None:
                            mass_sizes.append(Size(message.media.document.size))
                        else:
                            mass_sizes.append("-")
                    except Exception:
                        mass_sizes.append("-")
                        mass_sizes_for_check.append(0)
                    tmp_info = ''
                    try:
                        meta_info = await GetMetaInfoInInternet(message.file.name)
                    except Exception as e:
                        print(e)
                        meta_info = '-'
                    if meta_info != '-':
                        tmp_info += meta_info
                    try:
                        if message.text:
                            tmp_info = tmp_info + '\n' + message.text
                            mass_info.append(tmp_info)
                        else:
                            info_parts = []

                            if prev_message:
                                try:
                                    if prev_message.text:
                                        info_parts.append(f"Информация для предыдущего сообщения: {prev_message.text}")
                                except Exception as e:
                                    logging.error("An error occurred: %s", e)
                            if next_message:
                                try:
                                    if next_message.text:
                                        info_parts.append(
                                            f"\nИнформация для последующего сообщения: {next_message.text}")
                                except Exception as e:
                                    logging.error("An error occurred: %s", e)

                            if info_parts:
                                tmp_info_parts = "\n".join(info_parts)
                                tmp_info = tmp_info + '\n' + tmp_info_parts
                                mass_info.append(tmp_info)
                            else:
                                mass_info.append(tmp_info if tmp_info else '-')
                    except Exception as e:
                        logging.error("An error occurred: %s", e)
                        mass_info.append(tmp_info if tmp_info else '-')
                    if marker_of_download is False:
                        print("Сканировано файлов: ", count + 1)
                    else:
                        print(
                            count + 1,
                            "\t" * 2,
                            message.file.name,
                            "\t" * 2,
                            Size(message.media.document.size),
                            "\nИнформация из текста сообщения: \t",
                            message.text,
                            "\nИнформация из интернета: ",
                            meta_info,
                        )
                    count += 1
                    if count % 10 == 0:
                        await CheckConnect(client)
            except Exception as e:
                logging.error("An error occurred: %s", e)
                continue

    if not mass_names:
        print("Файлов для загрузки нет!")
        return
    mass_names = RenameDuplicates(mass_names)
    if marker_of_download is False:
        all_massives = CompareFilenames(mass_names, mass_sizes_for_check, mass_sizes, mass_id, mass_info, mass_links,
                                        mass_time)
        mass_names = all_massives[0]
        mass_sizes = all_massives[1]
        mass_id = all_massives[2]
        mass_info = all_massives[3]
        mass_links = all_massives[4]
        mass_time = all_massives[5]
    if not mass_names:
        print("Файлов для загрузки нет!")
        return
    check = list(set(mass_names))
    if len(mass_names) != len(check):
        print("Где-то функция создает дупликаты!")
        return

    print("\nАвтоматическое разделение по коллекциям...")
    clusters = AI_Analyse_For_Collection(mass_names, mass_sizes)
    clusters = Restructure(clusters)
    numbers_of_only_files = []
    numbers_of_files_for_collection = []
    mass_collections = []
    collection_names = []
    tmp_collection = []
    for cluster_id, file_names in clusters.items():
        if len(file_names) > 1:
            collection_names.append(cluster_id)
            for name in file_names:
                numbers_of_files_for_collection.append(
                    mass_names.index(name) + 1
                )
                tmp_collection.append(mass_names.index(name) + 1)
            mass_collections.append(tmp_collection.copy())
            tmp_collection.clear()
        else:
            numbers_of_only_files.append(mass_names.index(file_names[0]) + 1)

    await CheckConnect(client)
    if numbers_of_files_for_collection is not None:
        flag_of_collection = True
        for collection_of_files in mass_collections:
            path_to_files = folders_Air.get("collection_files_path")
            index = mass_collections.index(collection_of_files)
            print(
                f"\nВыполняется загрузка  {mass_collections.index(collection_of_files) + 1}й коллекции."
            )
            paths = CreateDirectoryForCollectionAir(path_to_files, collection_names, index)
            path_to_files = paths[0]
            path_to_info = paths[1]
            count = 0
            for number in collection_of_files:
                for i in range(len(mass_id)):
                    if number == i + 1:
                        try:
                            count += 1
                            count = await StartDownload(
                                marker_of_download,
                                flag_of_collection,
                                count,
                                i,
                                client,
                                channel_id,
                                num,
                                my_collection,
                                work_collection,
                                mass_names,
                                mass_id,
                                mass_info,
                                mass_sizes,
                                mass_time,
                                mass_links,
                                channel_name,
                                path_to_files,
                                path_to_info,
                            )
                        except Exception as e:
                            logging.error("An error occurred: %s", e)
                            continue
            if count == 0:
                DeleteCollectionFolder(path_to_files)
            else:
                print(
                    f"\nКоллекция {mass_collections.index(collection_of_files) + 1} успешно загружена!"
                )
    else:
        print("Нет коллекций для загрузки!\n")

    path_to_files = folders_Air.get("files_path")
    path_to_info = folders_Air.get("images_path")
    if numbers_of_only_files is None:
        print("Для загрузки не были выбраны файлы!")
        return

    elif numbers_of_only_files == [1]:
        flag_of_collection = False
        print("Выполняется...")
        count = 0
        new_mass_names = list(reversed(mass_names))
        new_mass_id = list(reversed(mass_id))
        new_mass_info = list(reversed(mass_info))
        new_mass_sizes = list(reversed(mass_sizes))
        new_mass_time = list(reversed(mass_time))
        new_mass_links = list(reversed(mass_links))

        for i in range(len(mass_id)):
            try:
                count += 1
                count = await StartDownload(
                    marker_of_download,
                    flag_of_collection,
                    count,
                    i,
                    client,
                    channel_id,
                    num,
                    my_collection,
                    work_collection,
                    new_mass_names,
                    new_mass_id,
                    new_mass_info,
                    new_mass_sizes,
                    new_mass_time,
                    new_mass_links,
                    channel_name,
                    path_to_files,
                    path_to_info,
                )
            except Exception as e:
                logging.error("An error occurred: %s", e)
                continue
    elif len(numbers_of_only_files) != 0:
        flag_of_collection = False
        print("Выполняется загрузка отдельных файлов...")
        count = 0
        for number in numbers_of_only_files:
            for i in range(len(mass_id)):
                if number == i + 1:
                    try:
                        count += 1
                        count = await StartDownload(
                            marker_of_download,
                            flag_of_collection,
                            count,
                            i,
                            client,
                            channel_id,
                            num,
                            my_collection,
                            work_collection,
                            mass_names,
                            mass_id,
                            mass_info,
                            mass_sizes,
                            mass_time,
                            mass_links,
                            channel_name,
                            path_to_files,
                            path_to_info,
                        )
                    except Exception as e:
                        logging.error("An error occurred: %s", e)
                        continue


def StartCollectInfo(**kwargs):
    marker_of_download = False
    ti = kwargs['ti']
    stop_running = False
    while not stop_running:
        stop_running = asyncio.run(Run(ti, marker_of_download))
    return


def StartDownloadInFlow(**kwargs):
    marker_of_download = True
    ti = kwargs['ti']
    stop_running = False
    while not stop_running:
        stop_running = asyncio.run(Run(ti, marker_of_download))
    return


async def connect_to_telegram_async(ti):
    client = TelegramClient(f"{session_telegram}/session_name", int(api_id), api_hash)
    try:
        # Подключение
        await client.connect()
        if await client.is_user_authorized():
            # Передаем информацию о подключении в XCom
            ti.xcom_push(key='client_state', value='connected')
            ti.xcom_push(key='session_id', value=session_telegram)
            print("Подключение успешно.")
            return True
        else:
            ti.xcom_push(key='client_state', value='not_connected')
            print("Необходимо авторизоваться.")
            return False
    except Exception as e:
        # В случае ошибки передаем информацию о статусе ошибки
        ti.xcom_push(key='client_state', value='error')
        print(f"Ошибка подключения: {e}")
        return False


# # Сохранение client в XCom
# kwargs['ti'].xcom_push(key='client', value=client)
# print("Клиент телеграмма записан в XCom")
# # Возвращаем значение для XCom
# return client
def connect_to_telegram(**kwargs):
    ti = kwargs['ti']
    asyncio.run(connect_to_telegram_async(ti))


# async def AirConnectToTelegram(client: TelegramClient) -> bool:
#     # session_path = r"C:\Users\and23\PycharmProjects\Airflow\dags\session_name.session"
#     # session_path = f"/opt/airflow/sessions/session_{uuid.uuid4()}"
#     print("\nПодключение к клиенту Telegram...")
#     # client = TelegramClient("session_name", int(api_id), api_hash)
#
#     # client = TelegramClient(f"{session_telegram}/session_name", int(api_id), api_hash)
#     try:
#         if not client.is_connected():
#             await client.connect()
#         if await client.is_user_authorized():
#             print("Подключение установлено успешно.")
#             return True
#         else:
#             print("Необходимо авторизоваться. Проверьте свои учетные данные.")
#             return False
#     except Exception as e:
#         print(f"Произошла ошибка: {e}. Переподключение через 5 секунд...")
#         await asyncio.sleep(5)
#         return False
#
#     # while True:  # Бесконечный цикл для поддержания подключения
#     #     try:
#     #         if not client.is_connected():
#     #             await client.connect()
#     #         if await client.is_user_authorized():
#     #             print("Подключение установлено успешно.")
#     #             # return client
#     #             return True
#     #         else:
#     #             print("Необходимо авторизоваться. Проверьте свои учетные данные.")
#     #             return None
#     #
#     #     except Exception as e:
#     #         print(f"Произошла ошибка: {e}. Переподключение через 5 секунд...")
#     #         await asyncio.sleep(5)

def MyMongoCollectionAir():
    # mongo_url = "mongodb://mongodb:27017/"
    mongo_url = "mongodb://10.169.44.69:27017/"
    # database_name = "NewCol"
    # collection_name = "files_hashes"
    return mongo_url, my_database_name, my_collection_name


# эта функция запускает процесс загрузки
async def RunDownloadInfo(client, marker_of_download):
    print("Проверка состояния клиента...")
    if not client.is_connected():
        print("Потеря подключения к клиенту.")
        return True
    # ch = input("Введите количество сообщений для проверки: ")
    # print(int(ch))
    dialogs = await get_all_dialogs(client)
    if marker_of_download is False:
        print("Начинается потоковая выгрузка всей информации для скачанных заранее файлов...")
    else:
        print("Проверка свободного пространства на диске...")
        if is_disk_limit_exceeded():
            print("Достигнут лимит по дисковому пространству. Прерываем загрузку.")
            return True
        print("Начинается потоковая загрузка всех непрочитанных файлов...")
    my_collection = ConnectToMongo(collection_parameters=MyMongoCollectionAir())
    work_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    if my_collection is not None and work_collection is not None:
        print("Успешное подключение к серверу MongoDB!")
    for dialog in dialogs:
        channel_id = dialog.entity.id
        channel_name = dialog.name
        tmp_channel = [channel_name, channel_id]
        if marker_of_download:
            if channel_id == 2117186516:
                # print("RuBase проверить вручную!")
                # continue
                num = 50
            else:
                num = dialog.unread_count
        else:
            num = 5000
        if marker_of_download is False:
            if not channel_name:
                continue
            print(f"\n\nПроцесс загрузки метаинформации для {channel_name}...")
            await DownloadInfoInFlow(client, tmp_channel, num, my_collection, work_collection, marker_of_download)
        else:
            print(f"\n\nПроцесс загрузки файлов для {channel_name}...")
            await DownloadFilesInFlow(client, tmp_channel, num, work_collection, marker_of_download)
            if limit_memory:
                return True
        print(f"\nПроцесс загрузки для {channel_name} завершен.")
    return True


async def Run(ti, marker_of_download):
    # блок для повторного подключения к клиенту
    client_state = ti.xcom_pull(task_ids='connect_to_client', key='client_state')
    if client_state == 'connected':
        session_id = ti.xcom_pull(task_ids='connect_to_client', key='session_id')
        client = TelegramClient(f"{session_id}/session_name", int(api_id), api_hash)
        try:
            await client.connect()
            if await client.is_user_authorized():
                print("Подключение установлено успешно.")
            else:
                print("Клиент не подключен")
                print("Статус клиента: ", client)
        except Exception as e:
            print(e)
            return
    else:
        print("Клиент не подключен")
        return
    stop_running = False
    while not stop_running:
        # client = await AirConnectToTelegram()
        # await AirConnectToTelegram()
        try:
            # stop_running = await RunDownloadInfo(client)
            stop_running = await RunDownloadInfo(client, marker_of_download)
        except Exception as e:
            print(e)
            print("Завершение работы клиента...")
            await client.disconnect()
            stop_running = True
        finally:
            if client.is_connected():
                await client.disconnect()
            await cancel_all_tasks()
    return stop_running


# это потом для создания архивов
def AuthoCollectionName(filenames):
    cleaned_filenames = [PreCleanFilename(name) for name in filenames]
    word_counts = Counter(cleaned_filenames)
    most_common = word_counts.most_common(1)[0][0].strip()
    if (most_common == '.' or most_common == 'info_file' or most_common == '' or most_common == ' '
            or most_common == 'info_file_txt'):
        most_common = (max(name for name in filenames)).replace(".", "_").strip()
    return most_common + '.7z'


def CheckEmptyArchive(archive):
    file_list = archive.namelist()
    if not file_list:
        return True
    with tempfile.TemporaryDirectory() as temp_dir:
        for file_name in file_list:
            extracted_path = archive.extract(file_name, path=temp_dir)
            if os.path.isdir(extracted_path):
                return False
            elif not CheckIsInfofile(extracted_path):
                return False
    return True


def AuthoCreationOfCollection(collection_files, work_mongo_collection, my_mongo_collection):
    count = 0
    archive_info = ''
    archive_channel = ''
    file_names = [os.path.basename(file) for file in collection_files]
    file_hashes = []
    collection_name = AuthoCollectionName(file_names)
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
        flag = CheckEmptyArchive(archive)
    if count == 0:
        print(f"\nАрхив {collection_name} будет удалён, так как он пуст.")
        DeleteEmptyArchive(collection_name)
        return None
    elif flag:
        print(f"\nАрхив {collection_name} будет удалён, так как в нем только файлы с информацией.")
        DeleteEmptyArchive(collection_name)
        return None
    else:
        archive_size = Size(ArchiveSize(archive.filename))
        print("\nРазмер архива: ", archive_size)
        print("Архив в папке ", folders_Air.get("directory"), "\n")
        merge = MergeMongoDocuments(collection_name, archive_size, archive_info, archive_channel, file_hashes,
                                    my_mongo_collection, work_mongo_collection)
        if not merge:
            print("Архив будет удален, так как в MongoDB отсутствует информация о файлах!")
            DeleteEmptyArchive(collection_name)
            return None
    return


def MailPassCountryArchiveCreation(collection_files, work_mongo_collection, my_mongo_collection):
    count = 0
    archive_info = ''
    archive_channel = []
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
                            # archive_channel = document['channel']
                            if document['channel'] not in archive_channel:
                                archive_channel.append(document['channel'])
                        except Exception as e:
                            print(e)
                            if 'site' not in archive_channel:
                                archive_channel.append('site')

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
        print("Архив в папке ", folders_Air.get("directory"), "\n")
        archive_channel = ', '.join(archive_channel)
        merge = MergeMongoDocuments(collection_name, archive_size, archive_info, archive_channel, file_hashes,
                                    my_mongo_collection, work_mongo_collection)
        if not merge:
            print("Архив будет удален, так как в MongoDB отсутствует информация о файлах!")
            DeleteEmptyArchive(collection_name)
            return None
    return


def TakeInfoFile_Air(file_name, info_files):
    for info in info_files:
        info_file_name = os.path.basename(info)
        if (info_file_name == file_name.replace(".", "_") + ".txt") or (
                info_file_name.rsplit(".", 1)[0] == file_name.rsplit(".", 1)[0]):
            info_file = info
            return info_file
    return None


def Create_Many_Archives_Air(files, info_files, work_mongo_collection, my_mongo_collection):
    count = 0
    for i in range(len(files)):
        count += 1
        file = files[i]
        file_name = files[i].replace("/mnt/telegram/", "")
        info_file = TakeInfoFile_Air(file_name, info_files)
        if not info_file:
            print(f"Ошибка! Проверьте наличие файла с метаинформацией для файла {file_name}")
            continue
        else:
            current_file_size = TakeSizeFromInfofile(info_file)
            archive_name = f"{file_name.rsplit('.', 1)[0].replace('.', '_')}.7z"
            archive_name = CheckExistArchiveNameInMongo(work_mongo_collection, archive_name, j=1)
            archive_name = CheckExistArchiveNameInFolder(archive_name, j=1)
            print(f"\nВыполняется создание архива {archive_name}...")
            print(f"Выполняется сбор ХЭШа для файла {file_name}...")
            file_hash = [TakeHash(file)]
            size = os.path.getsize(file)
            file_size = Size(size)
            if file_size != current_file_size:
                if current_file_size:
                    print("\nОдноименный файл с другим размером не может быть добавлен в текущий архив!\n"
                          "Проверьте этот файл вручную.")
                    continue

            if CheckFilehashInMongo(work_mongo_collection, file_hash[0]) is not False:
                print(f"Файл {file_name} уже есть в базе данных, он не будет добавлен в архив!")
            else:
                document = my_mongo_collection.find_one({"hash": file_hash[0]})
                try:
                    archive_info = document['info']
                except Exception as e:
                    print(e)
                    archive_info = '-'
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
                    print("Архив будет удален, так как в MongoDB отсутствует информация о файлах!")
                    DeleteEmptyArchive(archive_name)

    print("\nАрхивы в папке ", folders_Air.get("directory"), "\n")
    return


def CreateArchivesOfAll(**kwargs):
    len_data = kwargs['ti']
    os.chdir(folders_Air.get("directory"))
    files = FilesMetainfoCollectionPaths()[0]
    images = FilesMetainfoCollectionPaths()[1]
    collections = FilesMetainfoCollectionPaths()[2]
    work_mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    my_mongo_collection = ConnectToMongo(collection_parameters=MyMongoCollectionAir())
    print("\nСоздание архивов...")
    Create_Many_Archives_Air(files, images, work_mongo_collection, my_mongo_collection)
    for collection in collections:
        if collection.replace("/mnt/collection_files/", "") == 'mail_pass_country':
            collection_files = list(filter(os.path.isfile, glob.glob(f"{collection}/*")))
            if not collection_files:
                print(f"Нет файлов в коллекции {os.path.basename(collection)}")
                continue
            print("Проверка объема записей для mail_pass_country...")
            all_count = 0
            for file in collection_files:
                if os.path.basename(file) == 'info_file.txt' or os.path.basename(file) == 'sum_all_rows.txt':
                    continue
                try:
                    with open(file, 'r', encoding='utf-8', errors='replace') as data_file:
                        for line in data_file:
                            all_count += 1
                except Exception as e:
                    print(e)
                    exit(0)
            len_data.xcom_push(key='row_count', value=all_count)
            if all_count > 100000000:
                len_mail_pass_country_path = collection + '/sum_all_rows.txt'
                with open(len_mail_pass_country_path, 'w', encoding='utf-8') as file:
                    file.write(str(all_count))
                    print("Данные о количестве строк в mail_pass_country сохранены в файле sum_all_rows.txt")
                print("Количество записей в файлах больше 100 млн, архивация файлов будет выполнена, коллекция "
                      " будет загружена в tmp.\n"
                      f"Количество записей {all_count} ")
                print("\nКоллекция:", collection.replace("/mnt/collection_files/", ""), '\t Файлы:')
                for file in collection_files:
                    size = os.path.getsize(file)
                    print("\t", os.path.basename(file), " ", Size(size))
                MailPassCountryArchiveCreation(collection_files, work_mongo_collection, my_mongo_collection)
            else:
                len_mail_pass_country_path = collection + '/sum_all_rows.txt'
                print("Количество записей еще не превысило 100 млн!\n"
                      f"Текущее количество записей: {all_count}\n"
                      "Коллекция mail_pass_country будет пропущена")
                with open(len_mail_pass_country_path, 'w', encoding='utf-8') as file:
                    file.write(str(all_count))
                    print("Данные о количестве строк в mail_pass_country сохранены в файле sum_all_rows.txt")
        else:
            print(
                f"Выполняется архивация коллекции... \t\t {collections.index(collection) + 1} из {len(collections)}")
            print("Коллекция:", collection.replace("/mnt/collection_files/", ""), '\t Файлы:')
            collection_files = list(filter(os.path.isfile, glob.glob(f"{collection}/*")))
            if not collection_files:
                print(f"Нет файлов в коллекции {os.path.basename(collection)}")
                continue
            for file in collection_files:
                size = os.path.getsize(file)
                print("\t", os.path.basename(file), " ", Size(size))
            AuthoCreationOfCollection(collection_files, work_mongo_collection, my_mongo_collection)


def Air_DeleteCollections(collections, row_count):
    count = 0
    for collection in collections:
        count += 1
        name = os.path.basename(collection)
        if name == 'mail_pass_country':
            continue
        if os.path.exists(collection):
            shutil.rmtree(collection)
            print(f"Коллекция ", name, f" была успешно удалена. \t\t {count} из {len(collections)}")
        else:
            print(f"Коллекция {collection} не существует.")


def Air_Delete_Files(files):
    count = 0
    for i in range(len(files)):
        count += 1
        file_name = os.path.basename(files[i])
        file = files[i]
        try:
            if os.path.isfile(file) or os.path.islink(file):
                os.unlink(file)  # удаляем файл или символическую ссылку
            elif os.path.isdir(file):
                shutil.rmtree(file)  # удаляем папку рекурсивно
        except Exception as e:
            print(f'Не удалось удалить {file_name}. Причина: {e}')
        # os.remove(file)
        print(f"Файл {file_name} удален. \t\t {count} из {len(files)}")


def Autho_Dellete_All_Files_And_Info_And_Collections(**kwargs):
    len_data = kwargs['ti']
    row_count = len_data.xcom_pull(task_ids='creation_archives_all', key='row_count')
    files = FilesMetainfoCollectionPaths()[0]
    images = FilesMetainfoCollectionPaths()[1]
    collections = FilesMetainfoCollectionPaths()[2]
    print("Удаление файлов...")
    Air_Delete_Files(files)
    print("Удаление файлов метаинформации...")
    Air_Delete_Files(images)
    print("Удаление коллекций...")
    Air_DeleteCollections(collections, row_count)


def Clear_Test_Archive_Dup_Folder():
    # folder_path = '/opt/airflow/test_archive_dup'
    count = 0
    archives_dup = Archives_Path()[1]
    for i in range(len(archives_dup)):
        count += 1
        file_name = os.path.basename(archives_dup[i])
        file = archives_dup[i]
        try:
            if os.path.isfile(file) or os.path.islink(file):
                os.unlink(file)  # удаляем файл или символическую ссылку
            elif os.path.isdir(file):
                shutil.rmtree(file)  # удаляем папку рекурсивно
        except Exception as e:
            print(f'Не удалось удалить {file_name}. Причина: {e}')
        # os.remove(file)
        print(f"Файл {file_name} удален. \t\t {count} из {len(archives_dup)}")
    # if os.path.exists(folder_path):
    #     for filename in os.listdir(folder_path):
    #         file_path = os.path.join(folder_path, filename)
    #         try:
    #             if os.path.isfile(file_path) or os.path.islink(file_path):
    #                 os.unlink(file_path)  # удаляем файл или символическую ссылку
    #             elif os.path.isdir(file_path):
    #                 shutil.rmtree(file_path)  # удаляем папку рекурсивно
    #         except Exception as e:
    #             print(f'Не удалось удалить {file_path}. Причина: {e}')


def Archives_Path():
    archive_path = folders_Air.get("directory")
    archives_with_duplicates_path = folders_Air.get("archives_with_duplicates")
    archives = list(filter(os.path.isfile, glob.glob(f"{archive_path}/*") + glob.glob(f"{archive_path}/.*")))
    archives.sort(key=os.path.getsize)
    archives_dup = list(filter(os.path.isfile, glob.glob(f"{archives_with_duplicates_path}/*") + glob.glob(
        f"{archives_with_duplicates_path}/.*")))
    return archives, archives_dup


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


def patch_requests_ssl():
    original_get = requests.get
    original_post = requests.post

    @wraps(original_get)
    def patched_get(*args, **kwargs):
        kwargs.setdefault("verify", False)
        return original_get(*args, **kwargs)

    @wraps(original_post)
    def patched_post(*args, **kwargs):
        kwargs.setdefault("verify", False)
        return original_post(*args, **kwargs)

    requests.get = patched_get
    requests.post = patched_post


def patched_auth(self):
    data = {
        'username': self.login_name,
        'password': self.password,
    }
    url = "%s/%s" % (self.server_url.rstrip('/'), 'api2/auth-token/')
    res = requests.post(url, data=data, timeout=self.timeout)
    if res.status_code != 200:
        raise Exception(f"HTTP {res.status_code}: {res.content}")
    token = res.json()['token']
    assert len(token) == 40, 'The length of seahub api auth token should be 40'
    self.token = token
    self.headers = {"Authorization": f"Token {token}"}


async def ConnectionToSeafile(login_name, password):
    try:
        patch_requests_ssl()
        seafile_api = SeafileAPI(login_name, password, server_url)
        seafile_api.auth = types.MethodType(patched_auth, seafile_api)
        seafile_api.auth()
        repo = seafile_api.get_repo(f"{repo_id}")
        need_to_load_repo_id = seafile_api.get_repo(f"{repo_id_ltmdb}")
        return repo, repo.token, need_to_load_repo_id, need_to_load_repo_id.token
    except Exception as e:
        print(f"Произошла ошибка: {e}. Переподключение...")
        await asyncio.sleep(5)
        return await ConnectionToSeafile(login_name, password)


def AirArchiveRenamerToMove(archive, archive_name):
    count = 1
    new_archive_name = archive_name.replace(".7z", f" ({count}).7z")
    print(f"Переименовывание архива {archive_name} в {new_archive_name}")
    new_archive_path = os.path.join(folders_Air.get("directory"), new_archive_name)
    os.rename(archive, new_archive_path)
    target_path = folders_Air.get("archives_with_duplicates")
    while True:
        try:
            shutil.move(new_archive_path, target_path)
            print(f"Архив {new_archive_name} успешно перемещён в {target_path}")
            return True
        except Exception as e:
            print(e)
            if count == 101:
                return False
            count += 1
            print(f"Файл с именем {new_archive_name} уже существует. Попытка {count}...")
            new_archive_name = new_archive_name.replace(f" ({count - 1}).7z",
                                                        f" ({count}).7z")
            new_archive_path = os.path.join(folders_Air.get("directory"), new_archive_name)
            os.rename(archive, new_archive_path)


def AirMoveArchives(archives, names_of_duplicate):
    print(
        "Архивы с дупликатами будут загружены в папку: ",
        folders_Air.get("archives_with_duplicates"),
        "\n",
    )
    for archive in archives:
        archive_name = os.path.basename(archive)
        if archive_name in names_of_duplicate:
            try:
                shutil.move(archive, folders_Air.get("archives_with_duplicates"))
            except Exception as e:
                print(e)
                result = AirArchiveRenamerToMove(archive, archive_name)
                if not result:
                    print(f"Проблемы с переносом архива {archive_name}!")
                    exit(0)
                else:
                    continue


def TestArchiveForDuplicates(repo, archives, names_of_duplicate, tmp_mongo_collection):
    count = 0
    for archive in archives:
        archive_size = os.path.getsize(archive)
        archive_name = os.path.basename(archive)
        if CheckUploadTimeInMongo(tmp_mongo_collection, archive_name) is not False:
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
            if CheckUploadTimeInMongo(tmp_mongo_collection, archive_name) is not False:
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


def MountLocalFolder(smb_share, mount_point, credentials_file):
    try:
        subprocess.run([
            'mount', '-t', 'cifs', smb_share, mount_point,
            '-o', f'credentials={credentials_file},rw,file_mode=0777,dir_mode=0777'
        ], check=True)
    except Exception as e:
        print(e)


def UnmountLocalFolder(mount_point):
    subprocess.run(['umount', mount_point])


def DownloadArchiveToLocalFolder(local_archive_path):
    mount_point = '/mnt/test_archive'
    smb_share = fr'//{ipv4_addr}/test_archive'  # сетевой путь к папке
    credentials_file = '/root/.smbcredentials'
    if not os.path.exists(mount_point):
        os.makedirs(mount_point)
    try:
        MountLocalFolder(smb_share, mount_point, credentials_file)
        shutil.copy(local_archive_path, mount_point)

    except subprocess.CalledProcessError as e:
        print(f"Ошибка монтирования или копирования: {e}")
    finally:
        UnmountLocalFolder(mount_point)


# def DownloadArchiveToLocalFolder(file_path):
#     archive_dir = "/mnt/test_archive"
#     try:
#         if not os.path.exists(file_path):
#             logging.warning(f"Файл для архивации не найден: {file_path}")
#             return
#
#         if not os.path.isdir(archive_dir):
#             logging.error(f"Папка {archive_dir} недоступна")
#             return
#         file_name = os.path.basename(file_path)
#         archived_path = os.path.join(archive_dir, file_name)
#         shutil.move(file_path, archived_path)
#         logging.info(f"Файл перемещён в архив: {archived_path}")
#
#     except Exception as e:
#         logging.exception(f"Ошибка при перемещении файла в архив: {e}")


def AirUploadToSeafile(token, archives, server_url, repo_id, work_mongo_collection, row_count, loaded_to_mongo_repo_id,
                       loaded_to_mongo_token):
    not_loaded_archives = {}
    if len(archives) == 0:
        print("\nВыгрузка на сервер не требуется, Архивов нет!")
        return
    else:
        print("\nВыгрузка архивов на сервер...")

    api_url = f"{server_url}/api2/repos/{repo_id}/upload-link/"
    headers = {"Authorization": "Token {}".format(token)}
    payload = {"parent_dir": f"{parent_dir}"}

    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    try:
        response = session.get(api_url, headers=headers, verify=False)
        upload_link = f"{response.json()}?ret-json=1"
        # upload_link = requests.get(api_url, headers=headers).json() + "?ret-json=1"
        if not upload_link:
            print("Ошибка получения upload_link.")
            return

        count = 1
        previous_uploads = []
        for archive in archives:
            if os.path.basename(archive).startswith('mail_pass_country'):
                if row_count is None:
                    print("Отсутствует информация по количеству строк в архиве mail_pass_country! Проверьте его или "
                          "загрузите вручную!")
                elif row_count > 100000000:
                    archive_name = os.path.basename(archive)
                    api_url = f"{server_url}/api2/repos/{loaded_to_mongo_repo_id}/upload-link/"
                    headers = {"Authorization": f"Token {loaded_to_mongo_token}"}
                    payload = {"parent_dir": f"{parent_dir}"}
                    print(f"\nАрхив {archive_name} будет загружен в новый репозиторий с ID {loaded_to_mongo_repo_id}")
                    try:
                        # upload_link = requests.get(api_url, headers=headers).json() + "?ret-json=1"
                        # if not upload_link:
                        #     print(f"Ошибка при получении upload_link для архива {archive_name} в новом репозитории.")
                        #     continue
                        response = session.get(api_url, headers=headers, verify=False)
                        if response.status_code != 200:
                            print(f"Ошибка при получении upload_link для архива {archive_name} в новом репозитории.")
                            continue
                        upload_link = f"{response.json()}?ret-json=1"
                    except Exception as e:
                        print(e)
                        continue
            archive_name = os.path.basename(archive)
            size = os.path.getsize(archive_name)
            if size / 1024 ** 3 >= 15:
                print(f"\nАрхив {archive_name} слишком много весит для загрузки на сервер!\n"
                      f"Загрузите его вручную и обновите информацию о загрузке в базе MongoDB.\n")
                not_loaded_archives[archive_name] = Size(size)
            else:
                archive_size = Size(size)
                print(f"\nЗагрузка архива {archive_name}... \t Размер: {archive_size}")
                if previous_uploads:
                    avg_speed = sum(size / time_taken for size, time_taken in previous_uploads) / len(
                        previous_uploads)
                    estimated_time = size / avg_speed if avg_speed > 0 else 0
                    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if estimated_time > 600:
                        estimated_time = estimated_time / 60
                        print(f"Начало загрузки: {current_time}\nОжидаемая продолжительность загрузки:"
                              f" {estimated_time:.2f} минут")
                    else:
                        print(f"Начало загрузки: {current_time}\nОжидаемая продолжительность загрузки:"
                              f" {estimated_time:.2f} секунд")

                start_load_time = time.time()
                try:
                    with open(archive, "rb") as file_stream:
                        response = session.post(upload_link, headers=headers, data=payload, files={"file": file_stream},
                                                stream=True, verify=False)
                        # response = requests.post(upload_link, headers=headers, files={"file": file_stream,
                        #                                                               "parent_dir": f"{parent_dir}"},
                        #                          verify=False)

                    end_load_time = time.time()
                    upload_time = end_load_time - start_load_time
                    previous_uploads.append((size, upload_time))

                    if response.status_code == 200:
                        name = response.json()[0]["name"]
                        print(f"Архив {name} загружен на сервер", '\t', count, "из", len(archives))
                        UpdateMongoDocument(archive_name, work_mongo_collection)
                        count += 1
                    else:
                        print(f"\nПроизошла ошибка при загрузке архива {archive_name}\n")
                except Exception as e:
                    print(e)
                    # DownloadArchiveToLocalFolder(archive)
        print(f"\nЗагруженные на сервер: ", '\t', count - 1, "из", len(archives), "архивов")
        if not_loaded_archives:
            print(f"\nНе загруженные архивы: ")
            for name, size in not_loaded_archives.items():
                print(name, '\t\t', size)
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке: {e}")


def Autho_Upload_Archives_On_Server(**kwargs):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    len_data = kwargs['ti']
    row_count = len_data.xcom_pull(task_ids='creation_archives_all', key='row_count')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        parameters = loop.run_until_complete(ConnectionToSeafile(login_name, password))
    except Exception as e:
        print("Не удается подключиться к серверу OMP Seafiles")
        print(e)
        return
    print("\nУспешное подключение к библиотеке Seafile!\n")
    try:
        work_mongo_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
    except Exception as e:
        print("Не удается подключиться к коллекции MongoDB")
        print(e)
        return
    print("\nУспешное подключение к MongoDB!\n")
    all_archives = Archives_Path()
    archives = all_archives[0]
    CheckForArchives(archives)
    repo = parameters[0]
    token = parameters[1]
    loaded_to_mongo_repo_id = parameters[2]
    loaded_to_mongo_token = parameters[3]
    names_of_duplicate = []
    if TestArchiveForDuplicates(repo, archives, names_of_duplicate, work_mongo_collection):
        AirMoveArchives(archives, names_of_duplicate)
    all_archives = Archives_Path()
    archives = all_archives[0]
    os.chdir(folders_Air.get("directory"))
    AirUploadToSeafile(token, archives, server_url, repo_id, work_mongo_collection, row_count, loaded_to_mongo_repo_id,
                       loaded_to_mongo_token)
    loop.close()
    return


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 27, 10, 39),
    'provide_context': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}
'''
* * * * *
| | | | |
| | | | +---- день недели (0 - 6) (воскресенье = 0)
| | | +------ месяц (1 - 12)
| | +-------- день месяца (1 - 31)
| +---------- час (0 - 23)
+------------ минута (0 - 59)
'''

# schedule_interval='0 21 * * *'
with DAG('ETL_Процесс_Загрузки_Метаинформации', description='Запуск программы', schedule_interval=None,
         catchup=False,
         default_args=args) as dug:
    t1 = PythonOperator(
        task_id='connect_to_client',
        python_callable=connect_to_telegram
    )
    t2 = PythonOperator(
        task_id='connect_and_download_info',
        python_callable=StartCollectInfo
    )
    t3 = TriggerDagRunOperator(
        task_id='trigger_create_archive_dug',
        trigger_dag_id='ETL_Процесс_Создания_и_Выгрузки_Архивов',  # ID второго DAG
        conf={},  # можно передать дополнительные параметры, если нужно
    )
    t1 >> t2 >> t3

with DAG('ETL_Процесс_Создания_и_Выгрузки_Архивов', description='Запуск программы', schedule_interval=None,
         catchup=False,
         default_args=args) as dug1:
    t1 = PythonOperator(
        task_id='creation_archives_all',
        python_callable=CreateArchivesOfAll
    )
    t2 = PythonOperator(
        task_id='clean_memory',
        python_callable=Autho_Dellete_All_Files_And_Info_And_Collections
    )
    t3 = BashOperator(
        task_id='end_ETL',
        bash_command='echo "Загрузка информации завершена! Архивы собраны, используемые файлы удалены. Архивы будут '
                     ' перенесены на сервер."'
    )
    t4 = PythonOperator(
        task_id='upload_archives_on_server',
        python_callable=Autho_Upload_Archives_On_Server
    )
    t1 >> t2 >> t3 >> t4

with DAG('ETL_Процесс_Загрузки_Файлов', description='Запуск загрузки файлов из телеграмм-канала',
         schedule_interval='0 21 * * *',
         catchup=False,
         default_args=args) as dug2:
    t1 = PythonOperator(
        task_id='connect_to_client',
        python_callable=connect_to_telegram
    )
    t2 = PythonOperator(
        task_id='connect_and_download_info',
        python_callable=StartDownloadInFlow
    )
    t3 = TriggerDagRunOperator(
        task_id='trigger_collect_info_dug',
        trigger_dag_id='ETL_Процесс_Загрузки_Метаинформации',  # ID второго DAG
        conf={},  # можно передать дополнительные параметры, если нужно
    )

    t1 >> t2 >> t3

with DAG('ArchivesDupFolder_Clean',
         description='Удаление содержимого папки /opt/airflow/test_archive_dup с архивами, уже загруженными на сервер',
         schedule_interval='@daily',
         catchup=False,
         default_args=args) as dag3:
    clear_task = PythonOperator(
        task_id='clear_archives_dup_folder',
        python_callable=Clear_Test_Archive_Dup_Folder
    )
    clear_task
