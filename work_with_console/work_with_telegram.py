import asyncio
import glob
import hashlib
import logging
import os
import re
import shutil
import time
import warnings
from collections import defaultdict
import py7zr
from typing import Dict, Any
import tempfile
import zipfile
import rarfile
from pymongo import MongoClient
from sklearn.cluster import KMeans
from sklearn.cluster import MiniBatchKMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import silhouette_score
from telethon import TelegramClient, errors
from datetime import datetime
import pytz
from analitics_telegram import AnaliticWork
from analitics_telegram import *
from test_interface import *
from telethon.tl.types import Channel
from pathlib import Path
from collections import defaultdict
from difflib import SequenceMatcher
from dags.BaseAnalyse import RunFileAnalyse, NormalizeAndCreateMailPassFile
from set_parameters import (api_id, api_hash,
                            mongo_user_name, mongo_password, my_mongo_url, my_collection_name,
                            my_database_name, work_collection_name, work_database_name, ip_addr)
from telethon.errors import SessionPasswordNeededError
from urllib.parse import urlparse
from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl


def MyMongoCollection():
    return my_mongo_url, my_database_name, my_collection_name


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


def WriteMongoDocument(
        filename,
        file_hash,
        file_time,
        file_size,
        file_info,
        channel_title,
        file_link,
        collection,
        download_time,
        file_type,
        file_entity,
        file_report,
):
    if '.' in filename:
        file_type = filename.split('.')[-1]

    if isinstance(file_info, list):
        string_info = "\n".join(file_info)
    else:
        string_info = file_info

    if file_report:
        all_info = file_report + "\n" + string_info
    else:
        all_info = string_info
    record = {
        "name": filename,
        "hash": file_hash,
        "link": file_link,
        "upload_to_tg_time": file_time,
        "download_from_tg_time": download_time,
        "size": file_size,
        "type": file_type,
        "channel": channel_title,
        "info": all_info,
    }
    if file_entity is not None:
        record["in_archive"] = file_entity

    print(f"\nДобавление сохраненных данных файла {filename} в MongoDB...")
    collection.insert_one(record)
    time.sleep(1)
    print("Данные добавлены!")


def WriteHashInMongo(
        file_hash,
        collection,
):
    record = {
        "hash": file_hash,
    }
    print(f"\nДобавление исходного хэша отдельным документом MongoDB для файла mail+pass...")
    collection.insert_one(record)
    time.sleep(1)
    print("ХЭШ добавлен!")


def CheckUploadTimeInMongo(collection, filename):
    split = filename.rsplit(".", 1)[0]
    new_name = re.compile(f"^{re.escape(split)}\.[0-9a-zA-Z]*$", re.IGNORECASE)
    archive = collection.find_one(
        {"archive_name": new_name, "upload_to_server_time": {"$exists": True}}
    )
    return archive if archive is not None else False


def CheckFilenameInMongo(collection, filename):
    split = filename.rsplit(".", 1)[0]
    new_name = re.compile(f"^{re.escape(split)}\.[0-9a-zA-Z]*$", re.IGNORECASE)
    document = collection.find_one({"name": new_name})
    document_in_archive = collection.find_one({"files.name": new_name})
    archive = collection.find_one({"archive_name": new_name})
    return next((x for x in [document, document_in_archive, archive] if x is not None), False)


def CheckFilehashInMongo(collection, file_hash):
    document = collection.find_one({"hash": file_hash})
    document_in_archive = collection.find_one({"files.hash": file_hash})
    return next((x for x in [document, document_in_archive] if x is not None), False)


def TakeHash(filepath):
    hash_func = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
    file_hash = hash_func.hexdigest()
    return file_hash


# переработанные методы для ссылок на сообщения и канал
async def GetChannelLink(channel_name, channel_id):
    return f"https://t.me/{channel_name}" if channel_name else f"https://t.me/c/{channel_id}"


async def ChannelLinkByChannelId(client, channel_id):
    entity = await client.get_entity(int(channel_id))
    if entity.username:
        return f"https://t.me/{entity.username}"
    return f"https://t.me/c/{channel_id}"


async def Link_to_File(client, message_id, channel_name, channel_id):
    channel_link = await ChannelLinkByChannelId(client, channel_id)

    if channel_name:
        return f"{channel_link}/{message_id}"
    return f"https://t.me/c/{channel_id}/{message_id}"


def Size(size):
    if size < 1024:
        return f"{size} Б"
    elif 1024 <= size < 1024 ** 2:
        return f"{round(size / 1024, 3)} Кб"
    elif 1024 ** 2 <= size < 1024 ** 3:
        return f"{round(size / 1024 ** 2, 3)} Мб"
    else:
        return f"{round(size / 1024 ** 2, 3)} Мб ({round(size / 1024 ** 3, 3)} Гб)"


def KByteExtractFromSize(size):
    size_str = size.split(" ")[0]
    if "Мб" in size:
        return float(size_str) * 1024
    elif "Гб" in size:
        return float(size_str) * 1024 * 1024
    elif "Кб" in size:
        return float(size_str)
    elif "Б" in size:
        return float(size_str) / 1024
    elif "-":
        return 100


async def ConnectToTelegram():
    print("\nПодключение к клиенту Telegram...")
    session_path = os.path.join(session_telegram, "session_name")
    client = TelegramClient(session_path, int(api_id), api_hash)
    while True:  # Бесконечный цикл для поддержания подключения
        try:
            if not client.is_connected():
                await client.connect()
            if await client.is_user_authorized():
                print("Подключение установлено успешно.")
                return client
            else:
                print("Необходимо авторизоваться. Проверьте свои учетные данные.")
                return client

        except Exception as e:
            print(f"Произошла ошибка: {e}. Переподключение через 5 секунд...")
            await asyncio.sleep(5)


async def AuthorizeTelegram(client):
    phone = input("Введите номер телефона (в формате +71234567890): ").strip()
    await client.send_code_request(phone)
    code = input("Введите код из Telegram: ").strip()

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        password = input("Введите пароль от аккаунта Telegram (2FA): ").strip()
        await client.sign_in(password=password)

    if await client.is_user_authorized():
        print("✅ Авторизация прошла успешно.")
        return True
    else:
        print("❌ Авторизация не удалась.")
        return False


async def Write_Channel_name(client, obj):
    cur = 0
    dialogs = await get_all_dialogs(client)
    print("\nДоступные каналы:")
    for dialog in dialogs:
        print(dialog.entity.username)
    created_channel_name = input("\nВведите имя вручную: \n" "Ввод: ")
    if created_channel_name == "None":
        print("Невозможно получить доступ к этому каналу!")
        return await Write_Channel_name(client, obj)
    for dialog in dialogs:
        if dialog.entity.username == created_channel_name:
            obj.channel_name = created_channel_name
            created_link = obj.ChannelLink()
            print("Ссылка на канал: ", created_link, "\n")
            return created_channel_name, created_link
    print("Такого канала нет в списке, повторите ввод!")
    return await Write_Channel_name(client, obj)


async def CheckConnectToChanel(client, channel_id):
    try:
        dialogs = await get_all_dialogs(client)
        for dialog in dialogs:
            if dialog.entity.id == channel_id:
                unread = dialog.unread_count
                channel_name = dialog.name
                channel_title = dialog.entity.username
                if channel_id == 2117186516:
                    from folders_for_analitics import analitic_folders

                    f = open(rf"{analitic_folders.get('work_folder')}\take.txt", "r")
                    cur = f.readline()
                    f.close()
                    unread = unread - int(cur)
                print(f"Выполнено подключение к каналу {channel_name}")
                return channel_title, channel_id, unread
    except Exception:
        print("Не удается подключиться к указанному каналу!")
        return await ConnectionToChannel(client)


async def ConnectionToChannel(client):
    massive_id = []
    massive_unread = []
    massive_names = []
    dialogs = await get_all_dialogs(client)
    for dialog in dialogs:
        massive_names.append(dialog.name)
    max_len_name_size = len(max(massive_names, key=len))
    print("\nДоступные каналы", " " * (max_len_name_size - 16), "\t" * 3, "id", "\t" * 3, "(непрочитанные сообщения)",
          "\t" * 3, "ссылка\n")
    for dialog in dialogs:
        name = dialog.name
        channel_id = dialog.entity.id
        try:
            channel = await client.get_entity(channel_id)
            if hasattr(channel, "invite_link") and channel.invite_link:
                channel_link = channel.invite_link
            else:
                channel_link = f"https://t.me/{channel.username}"
        except Exception:
            channel_link = f"https://t.me/{name}"
        unread = dialog.unread_count
        if channel_id == 2117186516:
            from folders_for_analitics import analitic_folders
            f = open(rf"{analitic_folders.get('work_folder')}\take.txt", "r")
            cur = f.readline()
            f.close()
            unread = unread - int(cur)
        massive_id.append(channel_id)
        massive_unread.append(unread)
        print(name, "\t-" * round((max_len_name_size - len(name)) * 0.31), "\t-" * 2, channel_id, "\t\t", f"({unread})",
              "\t", channel_link)
    s = input("\nВведите id канала для подключения: ")
    need = re.fullmatch("[0-9]+", s)
    if need:
        print("Подключение к каналу...")
        if int(s) in massive_id:
            return await CheckConnectToChanel(client, int(s))
        else:
            print("Неверно указан id канала!")
            return await ConnectionToChannel(client)
    else:
        print("Проверьте корректность ввода!")
        return await ConnectionToChannel(client)


async def Count_Unread_Messages(client, channel_name):
    dialogs = await get_all_dialogs(client)
    for dialog in dialogs:
        if dialog.entity.username == channel_name:
            return dialog.unread_count


async def CheckMessage(client, channel, unread):
    count = 0
    message_names = []
    message_info = []
    sizes = []
    mass_links = []
    channel_id = channel[1]
    channel_title = channel[0]
    if channel_title is None:
        channel_title = channel_id
    async for message in client.iter_messages(channel_id, limit=unread):
        # for message in client.iter_messages(link, limit=unread):
        if message.media or message.file:
            try:
                if message.file.name is not None:
                    message_names.append(message.file.name)
                    message_info.append(message.text)
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
                        sizes.append(message.media.document.size)
                    except Exception:
                        sizes.append(0)
                    count += 1
            except Exception:
                continue
    if count == 0:
        print("\nВ этих сообщениях файлов нет!")
        return await TelegramWorkMenu(client, channel)
    else:
        print(
            f"\nИз {unread} сообщений количество файлов, доступных для скачивания: {count}\n"
        )
        print("Файлы:")
        for i in range(len(message_names)):
            print(
                f"{i + 1})\t",
                message_names[i],
                " ",
                Size(sizes[i]),
                "\t" * 2,
                mass_links[i],
                '\t' * 2,
                message_info[i],
                "\n",
            )
        return await TelegramWorkMenu(client, channel)


def CountMessages(num):
    test = re.fullmatch("[0-9]+", num)
    if len(num) <= 5:
        if test:
            if num == "0" or num == "00" or num == "000" or num == "0000":
                print("Количество сообщений должно быть больше нуля!")
                s = input("Введите количество сообщений: ")
                return CountMessages(s)
            else:
                return int(num)
        else:
            s = input("Введите количество сообщений: ")
            return CountMessages(s)
    else:
        print("Слишком большое количество сообщений!\n")
        s = input("Введите количество сообщений: ")
        return CountMessages(s)


def ControlInput(ch, mass_names):
    numbers_of_files = []
    val = re.fullmatch(
        r'^(0|[1-9]\d*|0 [1-9]\d*|[1-9]\d*(?:-[1-9]\d*)?)(?: (0|[1-9]\d*|0 [1-9]\d*|[1-9]\d*(?:-[1-9]\d*)?))*$', ch)
    if val:
        try:
            input_list = ch.split(" ")
            for item in input_list:
                if "-" in item:
                    start, end = map(int, item.split("-"))
                    if end < start:
                        tmp = start
                        start = end
                        end = tmp
                    while end - start >= 0:
                        numbers_of_files.append(start)
                        start += 1
                elif item != "" and item != "-":
                    numbers_of_files.append(int(item))

            numbers_of_files = list(set(numbers_of_files))
            return numbers_of_files
        except Exception:
            print("Некорректный ввод!")
            return ChooseTypeOfDownload(mass_names)
    else:
        print("Некорректный ввод!")
        return ChooseTypeOfDownload(mass_names)


def ControlInputSingle(s):
    val = re.fullmatch("[1-9][0-9]*", s)
    if val:
        return int(s)
    else:
        print("Некорректный ввод!")
        return ControlInputSingle(s)


def NumberOfCollections():
    s = input("\nВведите количество коллекций: ")
    number_of_collection = ControlInputSingle(s)
    return number_of_collection


def CheckDuplicateNames(collection_names):
    collection_name = InputName()
    if collection_name in collection_names:
        print("Это имя уже было использовано для коллекции!")
        return CheckDuplicateNames(collection_names)
    else:
        return collection_name


def RemoveDupInClusters(clusters):
    for cluster_id, files in clusters.items():
        seen = set()
        unique_files = []
        for f in files:
            if f not in seen:
                unique_files.append(f)
                seen.add(f)
        clusters[cluster_id] = unique_files
    return clusters


def AI_Analyse_For_Collection(mass_names, mass_sizes):
    print("\nОпределяется оптимальное число коллекций...")
    optimal_num_clusters = find_optimal_num_clusters(mass_names, mass_sizes)
    print("\nОпределяются файлы для коллекции...")
    clusters = cluster_file_names(mass_names, optimal_num_clusters, mass_sizes)
    clusters = RemoveDupInClusters(clusters)
    DisplayClusters(clusters)
    return clusters


def DopAIAnalyse(other_files):
    print("\nОпределяется оптимальное число коллекций...")
    optimal_num_clusters = DopFindOptimalNumClusters(other_files)
    print("\nОпределяются файлы для коллекции...")
    clusters = DopClusterFilenames(other_files, optimal_num_clusters)
    DisplayClusters(clusters)
    return clusters


def preprocess_filenames(filenames):
    print("\nОбработка имен файлов для более точной группировки...")

    def clean_filename(filename):
        return re.sub(r'[^\w\s]', '', filename).strip().lower()

    def get_base_name(filename):
        base_name = re.sub(r'\(.*\)', '', filename)
        return re.sub(r'\.\w+$', '', base_name)

    return [clean_filename(get_base_name(filename)) for filename in filenames]


def DopFindOptimalNumClusters(other_files):
    other_files_cleaned = preprocess_filenames(other_files)
    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    X = vectorizer.fit_transform(other_files_cleaned)
    scores = []
    unique_vector_count = X.shape[0]
    if unique_vector_count <= 2:
        return unique_vector_count
    max_clusters = min(unique_vector_count - 1, len(other_files))
    warnings.filterwarnings("ignore")

    for i in range(2, max_clusters + 1):
        if len(other_files) < 100:
            kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42, n_init=15)
        else:
            kmeans = MiniBatchKMeans(n_clusters=i, init='k-means++', random_state=42, n_init=15, batch_size=50)

        kmeans.fit(X)
        try:
            score = silhouette_score(X, kmeans.labels_)
        except Exception as e:
            print(e)
            score = 0
        scores.append(score)

    silhouette_threshold = 0.1
    if max(scores) < silhouette_threshold:
        optimal_num_clusters = 1
    else:
        optimal_num_clusters = scores.index(max(scores)) + 2
    return optimal_num_clusters


def DopClusterFilenames(other_files, num_clusters):
    clusters = {}
    if num_clusters == 1:
        clusters = {0: other_files}
        return clusters
    other_files_cleaned = preprocess_filenames(other_files)
    filename_map = defaultdict(list)
    for cleaned, original in zip(other_files_cleaned, other_files):
        filename_map[cleaned].append(original)
    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    X = vectorizer.fit_transform(other_files_cleaned)
    unique_vector_count = X.shape[0]
    if unique_vector_count < 2:
        clusters[1] = other_files
        return clusters
    num_clusters = max(1, min(num_clusters, unique_vector_count))

    if unique_vector_count < 100:
        kmeans = KMeans(n_clusters=num_clusters, init='k-means++', random_state=42, n_init=15)
    else:
        kmeans = MiniBatchKMeans(n_clusters=num_clusters, init='k-means++', random_state=42, n_init=15, batch_size=50)

    kmeans.fit(X)
    clusters = defaultdict(set)
    for index, label in enumerate(kmeans.labels_):
        cleaned_name = other_files_cleaned[index]
        clusters[label].update(filename_map[cleaned_name])
        # clusters[label].append(filename_map[other_files_cleaned[label]])
    clusters = {label: list(filenames) for label, filenames in clusters.items()}
    return clusters


def find_optimal_num_clusters(file_names, mass_sizes):
    counter = 0
    image_extensions = (".jpeg", ".png", ".jpg", ".bmp", ".dib", ".rle", ".pdf", ".gif", ".tiff", ".tgs")
    scripts = (".html", ".css", ".php", ".py", ".asp", ".xml", ".htm", ".exe")
    audios = (".mp3", ".wav", ".ogg", ".flac", ".wma")
    video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".webp")
    small_files = [file for size, file in zip(mass_sizes, file_names) if KByteExtractFromSize(size) <= 50]
    has_images = any(file_name.lower().endswith(image_extensions) for file_name in file_names)
    has_videos = any(file_name.lower().endswith(video_extensions) for file_name in file_names)
    has_scripts = any(file_name.lower().endswith(scripts) for file_name in file_names)
    has_audios = any(file_name.lower().endswith(audios) for file_name in file_names)
    has_small_files = bool(small_files)

    if has_images:
        counter += 1
    if has_videos:
        counter += 1
    if has_scripts:
        counter += 1
    if has_audios:
        counter += 1
    if has_small_files:
        counter += 1

    other_files = [file_name for file_name in file_names if not (
            file_name.lower().endswith(
                image_extensions + video_extensions + scripts + audios) or file_name in small_files)]

    if not other_files:
        return counter

    scores = []
    other_files_cleaned = preprocess_filenames(other_files)
    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    X = vectorizer.fit_transform(other_files_cleaned)
    unique_vector_count = X.shape[0]
    if unique_vector_count <= 2:
        return unique_vector_count + counter

    max_clusters = min(unique_vector_count - 1, len(other_files))
    warnings.filterwarnings("ignore")
    if max_clusters == 1 or len(set(other_files_cleaned)) == 1:
        optimal_num_clusters = 1
    else:
        for i in range(2, max_clusters + 1):
            if len(other_files) < 100:
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42, n_init=15)
            else:
                kmeans = MiniBatchKMeans(n_clusters=i, init='k-means++', random_state=42, n_init=15, batch_size=50)
            kmeans.fit(X)
            score = silhouette_score(X, kmeans.labels_)
            scores.append(score)

        silhouette_threshold = 0.1
        if max(scores) < silhouette_threshold:
            optimal_num_clusters = 1
        else:
            optimal_num_clusters = scores.index(max(scores)) + 2
    return optimal_num_clusters + counter


def cluster_file_names(file_names, num_clusters, mass_sizes):
    if num_clusters == 1:
        clusters = {0: file_names}
        return clusters
    image_extensions = (".jpeg", ".png", ".jpg", ".bmp", ".dib", ".rle", ".pdf", ".gif", ".tiff", ".tgs")
    scripts_extensions = (".html", ".css", ".php", ".py", ".asp", ".xml", ".htm", ".exe")
    audios_extensions = (".mp3", ".wav", ".ogg", ".flac", ".wma")
    video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".webp")
    small_files = [
        file
        for size, file in zip(mass_sizes, file_names)
        if KByteExtractFromSize(size) <= 50
    ]
    clusters = {"info_files": [], "videos": [], "audios": [], "scripts": [], "small_files": []}
    other_files = []
    for file_name in file_names:
        if file_name.lower().endswith(image_extensions):
            clusters["info_files"].append(file_name)
        elif file_name.lower().endswith(video_extensions):
            clusters["videos"].append(file_name)
        elif file_name.lower().endswith(audios_extensions):
            clusters["audios"].append(file_name)
        elif file_name.lower().endswith(scripts_extensions):
            clusters["scripts"].append(file_name)
        elif file_name in small_files:
            clusters["small_files"].append(file_name)
        else:
            other_files.append(file_name)

    numbered_clusters = {}
    current_cluster_id = 0
    for key, file_list in clusters.items():
        if file_list:  # Если в кластере есть файлы
            numbered_clusters[current_cluster_id] = file_list
            current_cluster_id += 1

    if not other_files:
        return numbered_clusters

    other_files_cleaned = preprocess_filenames(other_files)
    filename_map = defaultdict(list)
    for cleaned, original in zip(other_files_cleaned, other_files):
        filename_map[cleaned].append(original)

    unique_cleaned_files = list(set(other_files_cleaned))

    vectorizer = TfidfVectorizer(ngram_range=(1, 2))
    X = vectorizer.fit_transform(unique_cleaned_files)
    unique_vector_count = X.shape[0]
    if unique_vector_count < 2:
        clusters[1] = file_names
        return clusters
    num_clusters = max(1, min(num_clusters, unique_vector_count))

    if unique_vector_count < 100:
        kmeans = KMeans(n_clusters=num_clusters, init='k-means++', random_state=42, n_init=15)
    else:
        kmeans = MiniBatchKMeans(n_clusters=num_clusters, init='k-means++', random_state=42, n_init=15, batch_size=50)

    kmeans.fit(X)
    other_clusters = defaultdict(set)
    for index, label in enumerate(kmeans.labels_):
        cleaned_name = other_files_cleaned[index]
        other_clusters[label].update(filename_map[cleaned_name])

    for label, filenames in other_clusters.items():
        numbered_clusters[current_cluster_id] = list(filenames)
        current_cluster_id += 1

    clusters = {label: list(filenames) for label, filenames in numbered_clusters.items()}
    return clusters


def DisbandCluster(clusters):
    ch = input(
        "\nВведите номера коллекций через пробел: "
    )
    selected_ids = []
    selected_ids = ControlInput(ch, selected_ids)
    new_clusters = {}
    existing_ids = set(clusters.keys())
    # Пересобираем кластеры
    for cluster_id in selected_ids:
        if cluster_id in clusters:
            # Получаем файлы из выбранного кластера
            file_names = clusters.pop(cluster_id)
            for file_name in file_names:
                # Находим новый уникальный ID
                new_cluster_id = max(existing_ids.union(new_clusters.keys()), default=0) + 1

                # Убедимся, что новый ID не совпадает с уже существующими
                while new_cluster_id in existing_ids:
                    new_cluster_id += 1  # Ищем следующий доступный ID
                new_clusters[new_cluster_id] = [file_name]

    # Добавляем неизменённые кластеры обратно
    new_clusters.update(clusters)
    return new_clusters


def MergeClusters(clusters):
    ch = input(
        "\nВведите номера объединяемых коллекций или одиночных файлов через пробел: "
    )
    selected_ids = []
    selected_ids = ControlInput(ch, selected_ids)
    existing_ids = [cl_id for cl_id in selected_ids if cl_id in clusters]
    if len(existing_ids) < 2:
        print("Выберите 2 и более номера!")
        return MergeClusters(clusters)
    # Объединяем файлы из всех указанных кластеров
    merged_files = []
    for cl_id in existing_ids:
        merged_files.extend(clusters.pop(cl_id))  # Удаляем и получаем файлы
    # Создаём новый кластер с первым идентификатором
    new_cluster_id = existing_ids[0]
    clusters[new_cluster_id] = merged_files
    return clusters


def DeleteClusters(clusters):
    ch = input(
        "\nВведите номера коллекций или одиночных файлов через пробел: "
    )
    selected_ids = []
    selected_ids = ControlInput(ch, selected_ids)
    existing_ids = [cl_id for cl_id in selected_ids if cl_id in clusters]
    if len(existing_ids) == 0:
        print("Выберите существующие номера!")
        return MergeClusters(clusters)
    for cl_id in existing_ids:
        clusters.pop(cl_id)  # Удаляем файлы
    return clusters


def DisplayClusters(clusters):
    clusters_list = [(cluster_id, file_names) for cluster_id, file_names in clusters.items() if file_names]
    clusters_list_sorted = sorted(clusters_list, key=lambda x: x[1][0])
    sorted_clusters = {cluster_id: file_names for cluster_id, file_names in clusters_list_sorted}
    print("\nРезультаты группировки файлов: ")
    print("Номер группы:", "\t" * 5, "Названия файлов:")
    for cluster_id, file_names in sorted_clusters.items():
        print(cluster_id, "\t" * 3, file_names[0] if len(file_names) == 1 else file_names)


def AskForRemakeClusters(clusters):
    user_choice = input("\nДействия:\n"
                        "1) Если хотите пересобрать определенные коллекции с участием одиночных файлов, нажмите 1\n"
                        "2) Если хотите пересобрать определенные коллекции без участия отдельных одиночных файлов, "
                        "нажмите 2\n"
                        "3) Если хотите расформировать определенную коллекцию, нажмите 3\n"
                        "4) Для объединения коллекции вручную, нажмите 4\n"
                        "5) Для удаления определенной позиции, нажмите 5\n"
                        "6) Если вас устраивает автоматическое разбиение файлов на коллекции, нажмите 0\n"
                        "Выбор: ")
    if user_choice == '0':
        return clusters  # Возвращаем неизменный набор кластеров

    elif user_choice == '1':
        all_unique_files = []
        keys_to_remove = []
        new_clusters = dict(sorted(DisbandCluster(clusters).items()))  # расформировываем кластер
        for cluster_id, file_names in new_clusters.items():
            if len(file_names) == 1:
                all_unique_files.append(file_names[0])
                keys_to_remove.append(cluster_id)
        for key in keys_to_remove:
            new_clusters.pop(key)
        remaked_clusters = DopAIAnalyse(all_unique_files)  # еще раз кластеризуем новый набор файлов
        for key, value in remaked_clusters.items():
            if key not in new_clusters:
                new_clusters[key] = value
            else:
                new_key = key
                while new_key in new_clusters:
                    new_key += 1
                new_clusters[new_key] = value

        DisplayClusters(new_clusters)
        return AskForRemakeClusters(new_clusters)

    elif user_choice == '2':
        all_file_names = []
        ch = input(
            "\nВведите номера позиций через пробел: "
        )
        selected_ids = []
        selected_ids = ControlInput(ch, selected_ids)
        new_clusters = {}
        for cluster_id in selected_ids:
            if cluster_id in clusters:
                file_names = clusters.pop(cluster_id)  # Получаем файлы из выбранного кластера
                for file_name in file_names:
                    all_file_names.append(file_name)
        new_clusters.update(clusters)  # Добавляем неизменённые кластеры обратно
        remaked_clusters = DopAIAnalyse(all_file_names)
        for key, value in remaked_clusters.items():
            if key not in new_clusters:
                new_clusters[key] = value
            else:
                new_key = key
                while new_key in new_clusters:
                    new_key += 1
                new_clusters[new_key] = value

        DisplayClusters(new_clusters)
        return AskForRemakeClusters(new_clusters)

    elif user_choice == '3':
        new_clusters = dict(sorted(DisbandCluster(clusters).items()))
        DisplayClusters(new_clusters)
        return AskForRemakeClusters(new_clusters)

    elif user_choice == '4':
        new_clusters = MergeClusters(clusters)
        DisplayClusters(new_clusters)
        return AskForRemakeClusters(new_clusters)

    elif user_choice == '5':
        new_clusters = DeleteClusters(clusters)
        DisplayClusters(new_clusters)
        return AskForRemakeClusters(new_clusters)

    else:
        print("Введите 1 или 0 по условию!")
        return AskForRemakeClusters(clusters)  # Повторяем запрос


def TakeCollection(mass_names):
    collection_names = []
    ch = input(
        "\nЕсли хотите указать файлы для коллекции, нажмите 1, для продолжения без коллекций, нажмите 0: "
    )
    if ch == "1":
        number_of_collections = NumberOfCollections()
        mass_collections = []
        all_numbers_for_collections = []
        for i in range(number_of_collections):
            collection_name = CheckDuplicateNames(collection_names)

            s = input(
                f"\nДля Коллекции {i + 1} ({collection_name}):\n"
                f"Введите номера файлов через пробел или промежуток c A до B (например: 1 2 "
                f"5-9): "
            )
            numbers_of_files_for_collection = ControlInput(s, mass_names)

            for collection in mass_collections:
                for number in numbers_of_files_for_collection:
                    if number in collection:
                        print(
                            f"\nНомер файла {number} уже был указан для коллекции {mass_collections.index(collection) + 1}!"
                        )
                        print("Этот файл не будет добавлен в коллекцию\n")
                        numbers_of_files_for_collection.remove(number)

            if len(numbers_of_files_for_collection) == 0:
                number_of_collections -= 1
                print("Указанные номера файлов создают повторяющуюся коллекцию!")
                print(
                    f"Имя для этой коллекции будет удалено. Теперь количество коллекций: {number_of_collections}"
                )
            else:
                mass_collections.append(numbers_of_files_for_collection)
                collection_names.append(collection_name)

        for collection in mass_collections:
            for number in collection:
                all_numbers_for_collections.append(number)

        if len(all_numbers_for_collections) == 0:
            return None, None, None
        else:
            return all_numbers_for_collections, mass_collections, collection_names

    elif ch == "0":
        return None, None, None
    else:
        print("Введите номер пункта по условию!\n")
        return TakeCollection(mass_names)


def CheckUnionNumbers(
        numbers_of_files,
        numbers_of_files_for_collection,
        mass_collections,
        collection_names,
):
    if numbers_of_files_for_collection is not None:
        for number in numbers_of_files:
            if number in numbers_of_files_for_collection:
                choise = input(
                    f"Номер {number} уже был выбран для коллекции файлов!\nЧтобы оставить файл для коллекции, нажмите 1\n"
                    f"Чтобы удалить файл из коллекции и загрузить отдельно, нажмите 0\n"
                    f"Выбор: "
                )
                if choise == "1":
                    numbers_of_files.remove(number)
                    print("Этот номер удален из списка отдельных файлов")
                    return (
                        numbers_of_files,
                        numbers_of_files_for_collection,
                        mass_collections,
                    )
                elif choise == "0":
                    numbers_of_files_for_collection.remove(number)
                    if len(numbers_of_files_for_collection) == 0:
                        numbers_of_files_for_collection = None
                        print(
                            "Номеров файлов для коллекции нет, коллекция не будет загружена!"
                        )
                    for collection in mass_collections:
                        if number in collection:
                            collection.remove(number)
                    print("Этот номер удален из коллекции")

                    for collection in mass_collections:
                        if len(collection) == 0:
                            del collection_names[mass_collections.index(collection)]
                            mass_collections.remove(collection)
                            print(
                                f"Коллекция {mass_collections.index(collection) + 1} удалена, так как в ней не осталось "
                                f"элементов!"
                            )
                    if len(mass_collections) == 0:
                        mass_collections = None
                        print(
                            "Коллекций больше нет, все файлы будут загружаться по отдельности!"
                        )
                    return (
                        numbers_of_files,
                        numbers_of_files_for_collection,
                        mass_collections,
                    )

                else:
                    print("Введите номер пункта по условию!\n")
                    return CheckUnionNumbers(
                        numbers_of_files,
                        numbers_of_files_for_collection,
                        mass_collections,
                        collection_names,
                    )

    return (
        numbers_of_files,
        numbers_of_files_for_collection,
        mass_collections,
        collection_names,
    )


def SecondChoose():
    s = input(
        "\nДействия\n"
        "1. Для загрузки всех файлов, сортированных по дате добавления (от ранних к поздним), нажмите 1\n"
        "2. Для выборочной загрузки, нажмите 2\n"
        "3. Для продолжения без загрузки одиночных файлов, нажмите 0\n"
        "Выбор: "
    )
    if s == "1" or "2" or "0":
        return s
    else:
        print("Введите номер пункта по условию!\n")
        return SecondChoose()


def ChooseTypeOfDownload(mass_names):
    collection_parameters = TakeCollection(
        mass_names
    )  # все элементы коллекций,  массив коллекций
    numbers_of_files_for_collection = collection_parameters[0]
    mass_collections = collection_parameters[1]
    collection_names = collection_parameters[2]
    s = SecondChoose()
    if s == "1":
        numbers_of_files = [1]
        return (
            numbers_of_files,
            numbers_of_files_for_collection,
            mass_collections,
            collection_names,
        )

    elif s == "2":
        ch = input(
            "\nВведите номера файлов через пробел или промежуток c A до B (например: 1 2 5-9): "
        )
        numbers_of_files = ControlInput(ch, mass_names)
        all_numbers_of_files = CheckUnionNumbers(
            numbers_of_files,
            numbers_of_files_for_collection,
            mass_collections,
            collection_names,
        )
        return (
            all_numbers_of_files[0],
            all_numbers_of_files[1],
            all_numbers_of_files[2],
            all_numbers_of_files[3],
        )
    elif s == "0":
        return None, numbers_of_files_for_collection, mass_collections, collection_names


def ExecutionTime(current_speed, next_message_size):
    return next_message_size / current_speed


def WriteStructure(structure, file, level=0):
    indent = '  ' * level  # Отступы для иерархии
    if isinstance(structure, dict):
        for key, value in structure.items():
            if isinstance(value, (dict, list)):
                file.write(f"{indent}{key}:\n")
                WriteStructure(value, file, level + 1)
            else:
                file.write(f"{indent}{key}: {value}\n")
    elif isinstance(structure, list):
        for item in structure:
            WriteStructure(item, file, level)


def WriteInfoFile(
        i,
        massive_info,
        infofilepath,
        file_link,
        filename,
        file_time,
        file_size,
        channel_title,
        download_time,
        file_entity,
        file_report
):
    print(f"\nДобавление сохраненных данных файла {filename} в файл с описанием...")
    infofilepath_txt = infofilepath.replace(".", "_") + ".txt"
    with open(infofilepath_txt, "a", encoding="utf-8") as infofile:
        infofile.write(f"Название канала: {channel_title}\n")
        infofile.write(f"\nИмя файла: {filename}\n")
        infofile.write(f"\nРазмер файла: {file_size}\n")
        infofile.write(f"\nВремя добавления файла в ТГ канал: {file_time}\n")
        infofile.write(f"\nВремя загрузки файла из ТГ канала: {download_time}\n")
        infofile.write(f"\nСсылка на сообщение: {file_link}\n")
        if file_entity is not None:
            infofile.write(f"\nВ архиве:\n")
            WriteStructure(file_entity, infofile)
        if file_report:
            infofile.write(file_report)
            infofile.write("\n")
        if massive_info[i] != "-":
            infofile.write(f"\n{str(massive_info[i])}\n")
            infofile.write("\n")
        else:
            infofile.write("\nМетаинформация по файлу отсутсвует!\n")
            infofile.write("\n")
    time.sleep(1)
    print("Данные добавлены!")


async def download_file(client, message, filepath, progress_callback):
    while True:
        try:
            await client.download_media(message, filepath, progress_callback=progress_callback)
            break  # Выход из цикла, если загрузка прошла успешно
        except errors.FloodWaitError as e:
            print(f"Ожидание {e.seconds} секунд из-за флуда...")
            await asyncio.sleep(e.seconds)  # Ожидание, указанное в ошибке
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            break  # Выход из цикла, если произошла другая ошибка


def CreateDirectoryForMailPassFilesAir():
    path_to_files = folders.get("collection_files_path")
    test_path_to_files = os.path.join(
        path_to_files,
        f"mail_pass_country",
    )
    if not os.path.exists(test_path_to_files):
        os.makedirs(test_path_to_files, exist_ok=True)
        print(f"Создана папка {test_path_to_files}.")
        return test_path_to_files
    else:
        print(f"Используется папка {test_path_to_files}")
        return test_path_to_files


last = 0


async def StartDownload(
        marker_of_download,  # показывает, нужно ли загружать файлы из ТГ
        flag_of_collection,  # проверяет, будет ли загружаться коллекция или отдельный файл
        count,  # счетчик для сообщений
        i,  # итерация для сообщений
        client,  # клиент телеграмм
        channel_id,  # id канала
        num,  # количество сообщений
        my_collection,  # моя коллекция в mongodb для промежуточных файлов
        work_collection,  # рабочая коллекция files_hashes в mongodb
        massive_names,  # список имен файлов
        massive_id,  # список id файлов
        massive_info,  # список информаций по сообщениям для файлов
        massive_sizes,  # список размера файлов
        massive_time,  # список времени добавления файла
        massive_links,  # список ссылок на сообщения
        channel_title,  # название канала
        path_to_file,  # путь к загружаемым файлам
        path_to_info,  # путь к файлам с информацией о сообщении
):
    await CheckConnect(client)
    filename = massive_names[i]
    file_size = massive_sizes[i]
    file_time = massive_time[i]
    file_info = massive_info[i]
    file_link = massive_links[i]
    filepath = os.path.join(path_to_file, filename)
    if not os.path.splitext(filename)[1]:
        filename += '.txt'  # Добавляем расширение .txt
        filepath = os.path.join(path_to_file, filename)
    if flag_of_collection:
        infofilepath = os.path.join(path_to_info, "info_file")
        if marker_of_download is False or os.path.exists(filepath):
            try:
                downloaded_files_paths = folders.get("files_path")
                files = list(filter(os.path.isfile, glob.glob(f"{downloaded_files_paths}/*") + glob.glob(
                    f"{downloaded_files_paths}/.*")))
                for file in files:
                    if os.path.basename(file) == filename:
                        shutil.move(file, path_to_file)
            except Exception as e:
                print("Не удается переместить загруженный файл в папку с коллекцией!")
                exit(0)
    else:
        infofilepath = os.path.join(path_to_info, filename)

    if marker_of_download is True:
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            print(f"\nФайл {filename} уже существует, пропускаю загрузку из Telegram.")
        else:
            global last
            last = 0

            def progress_callback(current, total):
                global last
                total_mb = total / (1024 * 1024)
                current_mb = current / (1024 * 1024)
                if 10 < total_mb <= 100:
                    if round(current_mb, 2) - last >= 10:
                        print(f"Загружено {current_mb:.2f} Мб из {total_mb:.2f} Мб")
                        last = round(current_mb, 2)
                elif 100 < total_mb <= 500:
                    if round(current_mb, 2) - last >= 50:
                        print(f"Загружено {current_mb:.2f} Мб из {total_mb:.2f} Мб")
                        last = round(current_mb, 2)
                elif 500 < total_mb:
                    if round(current_mb, 2) - last >= 100:
                        print(f"Загружено {current_mb:.2f} Мб из {total_mb:.2f} Мб")
                        last = round(current_mb, 2)
                else:
                    print(f"Загружено {current_mb:.2f} Мб из {total_mb:.2f} Мб")

            async for message in client.iter_messages(channel_id, limit=num):
                if message.id == massive_id[i]:
                    # добавить время ожидания загрузки
                    print(f"\nЗагрузка файла {filename} \t {file_size}...")
                    await download_file(client, message, filepath, progress_callback=progress_callback)
                    # await client.download_media(message, filepath, progress_callback=progress_callback)
    else:
        print(f"\nЗагрузка метаинформации для файла {filename} \t {file_size}...")
        await asyncio.sleep(1)

    download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        file_hash = TakeHash(filepath)
    except Exception as e:
        print(f"Файл {filename} пропущен.")
        # print(f"Ошибка! Проверьте загруженный файл {filename}")
        return count - 1

    async def Process(
            work_collection,
            my_collection,
            filename,
            file_hash,
            file_time,
            file_size,
            file_info,
            channel_title,
            file_link,
            filepath,
            massive_info,
            infofilepath,
            count,
            i,
            download_time,
    ):
        file_entity = await TakeEntityInArchiveFile(filepath)
        file_type = 'folder' if os.path.isdir(filepath) else 1
        report = RunFileAnalyse(filepath)
        file_report = report[0]
        is_mail_pass_file = report[1]
        if is_mail_pass_file is True:
            normalize_folder = CreateDirectoryForMailPassFilesAir()
            path_to_files = folders.get("files_path")
            normalize_filepath = os.path.join(normalize_folder, os.path.basename(filepath))
            base_name, ext = os.path.splitext(os.path.basename(filepath))
            new_filename = f"{base_name}{ext}"
            i = 1
            while os.path.exists(normalize_filepath):
                new_filename = f"{base_name} ({i}){ext}"
                normalize_filepath = os.path.join(normalize_folder, new_filename)
                i += 1
            temp_renamed_path = os.path.join(path_to_files, new_filename)

            if os.path.basename(filepath) != new_filename:
                os.rename(filepath, temp_renamed_path)
                filepath = temp_renamed_path

            shutil.move(filepath, normalize_folder)
            if os.path.exists(normalize_filepath):
                infofilepath = os.path.join(normalize_folder, "info_file")
                filepath = normalize_filepath
            else:
                print("Ошибка при переносе файла с Mail+Pass. Он будет загружен как обычный файл без "
                      " автоматической обработки строк.")

        def WriterInfo():
            WriteMongoDocument(
                filename,
                file_hash,
                file_time,
                file_size,
                file_info,
                channel_title,
                file_link,
                my_collection,
                download_time,
                file_type,
                file_entity,
                file_report
            )
            WriteInfoFile(
                i,
                massive_info,
                infofilepath,
                file_link,
                filename,
                file_time,
                file_size,
                channel_title,
                download_time,
                file_entity,
                file_report
            )

        if marker_of_download is False:
            try:
                if CheckFilehashInMongo(my_collection, file_hash) is not False:
                    if CheckFilehashInMongo(work_collection, file_hash) is not False:
                        print(f"Файл {filename} уже есть в базе данных!")
                        os.remove(filepath)
                        print("Файл удален!")
                        return count - 1
                    else:
                        print(f"Информация для файла {filename} уже была загружена в базу данных!")
                        return count - 1
                else:
                    WriterInfo()
                    print("Загружено файлов: ", count)
            except Exception as e:
                print(e)
                return count - 1
        else:
            if CheckFilehashInMongo(work_collection, file_hash) is not False:
                print(f"Файл {filename} уже есть в базе данных!")
                os.remove(filepath)
                count -= 1
                print("Файл удален!")
            else:
                WriterInfo()
                print("Загружено файлов: ", count)

    await Process(
        work_collection,
        my_collection,
        filename,
        file_hash,
        file_time,
        file_size,
        file_info,
        channel_title,
        file_link,
        filepath,
        massive_info,
        infofilepath,
        count,
        i,
        download_time,
    )
    return count


def ValidName(filename):
    forbidden_characters = r'[<>:"/\\|?*\x00-\x1F]'
    if re.search(forbidden_characters, filename) or not filename.strip():
        return False
    return True


def InputName():
    s = input("\nВведите название или номер коллекции: ")
    if len(s) > 200:
        print("Имя слишком длинное!\n")
        return InputName()
    if not ValidName(s):
        print("Имя содержит недопустимые символы!\n")
        return InputName()
    return s


def CreateDirectoryForCollection(path_to_files, collection_names, index):
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
        new_collection_name = InputName()
        collection_names[index] = new_collection_name
        return CreateDirectoryForCollection(path_to_files, collection_names, index)


async def CheckConnect(client):
    if not client.is_connected():
        print("\nКлиент был отключен, повторное подключение...")
        await client.connect()
        print("Подключение восстановлено.\n")
    else:
        print("\nТекущее подключение к клиенту в норме.\n")


logging.basicConfig(level=logging.INFO)


def FilenameFromFiles(file):
    filename = os.path.basename(file)
    split = filename.rsplit(".", 1)[0]
    ext = filename.rsplit(".", 1)[-1]
    num = re.search(" \(\d+\).*[\s\w0-9]*", split)
    file_base_name = split.rsplit(num.group(), 1)[0] if num else split
    return filename, file_base_name, ext


def CompareFilenames(file_names, mass_sizes_for_check, mass_sizes, mass_id, mass_info, mass_links, mass_time):
    used = {}
    renamed = []
    new_mass_sizes = []
    new_mass_id = []
    new_mass_info = []
    new_mass_links = []
    new_mass_time = []
    filenames_in_folder = []
    downloaded_files_paths = folders.get("files_path")
    files = list(
        filter(os.path.isfile, glob.glob(f"{downloaded_files_paths}/*") + glob.glob(f"{downloaded_files_paths}/.*")))
    for file in files:
        filenames_in_folder.append(FilenameFromFiles(file)[0])
    print("\nВыполняется отбор имен для загруженных ранее файлов...\n")
    max_len_name = len(max(file_names, key=len))
    max_len_file = len(max(filenames_in_folder, key=len))
    max_len_name_size = len(max(mass_sizes, key=len))
    print("Имя", " " * (max_len_name - 3), "\t\tРазмер", " " * (max_len_name_size - 6), "\t-" * 5, "\t\tФайл",
          " " * (max_len_file - 4), "\t\t", "Размер\n")
    count = 0
    for name in file_names:
        flag = False
        name_size = mass_sizes_for_check[count]

        def DopAppend():
            new_mass_sizes.append(mass_sizes[count])
            new_mass_id.append(mass_id[count])
            new_mass_info.append(mass_info[count])
            new_mass_links.append(mass_links[count])
            new_mass_time.append(mass_time[count])

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
                if filename not in used:
                    used[filename] = 1
                    renamed.append(filename)
                    DopAppend()
                    flag = True
                    print(f"{count + 1}) ",
                          name, " " * (max_len_name - len(name)), "\t\t", Size(name_size),
                          " " * (max_len_name_size - len(Size(name_size))), "\t-" * 5, "\t\t", filename,
                          " " * (max_len_file - len(filename)), "\t\t", Size(os.path.getsize(file)))
                    break
                else:
                    print(
                        f"Имя {name} с размером {Size(name_size)} еще не было использовано, а файл {filename} с "
                        f"размером {Size(os.path.getsize(file))} уже был использован. Повтор файла в тг канале.\n"
                        f"Объединение информации из сообщения для файла {name}")
                    new_mass_info[renamed.index(filename)] += f"\n{mass_info[count]}"
                    continue
        count += 1
        # if not flag:
        #     print(f"Файл с именем {name} и размером {Size(name_size)} отсутствует в папке с файлами")
    return renamed, new_mass_sizes, new_mass_id, new_mass_info, new_mass_links, new_mass_time


def RenameDuplicates(file_names):
    seen = {}
    renamed = []
    print("\nОпределение и изменение повторяющихся имен файлов...")
    for name in file_names:
        name_parts = name.rsplit(".", 1)
        if len(name_parts) == 2:
            base_name, ext = name_parts
            num = re.search(" \(\d+\).*[\s\w0-9]*", base_name)
            if num:
                base_name = base_name.rsplit(num.group(), 1)[0]
        else:
            base_name, ext = name, ''
        if name not in seen:
            seen[name] = 1
            renamed.append(name)
        else:
            count = seen[name]
            new_name = f"{base_name} ({count}){('' if ext == '' else '.' + ext)}"
            while new_name in seen:
                count += 1
                new_name = f"{base_name} ({count}){('' if ext == '' else '.' + ext)}"
            renamed.append(new_name)
            seen[name] += 1
            seen[new_name] = 1
    return renamed


def ChooseAIProcess():
    ch = input(
        "\nВыбор процесса:\n"
        "1) Для автоматической обработки всех сообщений при помощи ИИ, нажмите 1\n"
        "2) Для загрузки файлов вручную, нажмите 0\n"
        "Выбор: "
    )
    if ch == "1":
        return 1
    if ch == "0":
        return 0
    else:
        print("Введите 1 или 0!")
        return ChooseAIProcess()


def Confirm():
    ch = input("\nДля подтверждения операции, нажмите 1, для отмены, нажмите 0: ")
    if ch == "1":
        return 1
    if ch == "0":
        return 0
    else:
        print("Введите 1 или 0!")
        return Confirm()


async def GetEntityFromInnerFile(directory, archive_path: str, parent_path: str = '', depth: int = 0,
                                 max_depth: int = 10) -> \
        Dict[str, Any]:
    extension = os.path.splitext(archive_path)[1].lower()
    if extension in ['.7z', '.rar', '.zip']:
        type_name = 'in_archive'
    elif extension:
        type_name = 'in_folder'
    else:
        type_name = None
    if depth > max_depth:
        print(f"Превышена максимальная глубина вложенности для архива {archive_path}")
        return {"name": os.path.basename(archive_path), "type": extension.replace(".", ""), type_name: []}
    structure = {"name": os.path.basename(archive_path), "type": extension.replace(".", ""), type_name: []}
    if extension == '.zip':
        try:
            with zipfile.ZipFile(archive_path, 'r', metadata_encoding='cp866') as archive:
                for file_info in archive.infolist():
                    relative_path = file_info.filename
                    if relative_path.endswith('/'):
                        relative_path = relative_path[:-1]
                    file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
                    if file_info.is_dir():
                        await AddToStructure(structure, relative_path, "folder")
                    else:
                        if relative_path.endswith(('.zip', '.rar', '.7z')):
                            nested_structure = await ProccessInnerArchiveZIP(directory, archive, archive_path,
                                                                             relative_path,
                                                                             file_extension, depth, max_depth)
                            await AddToStructure(structure, relative_path, file_extension, nested_structure)
                        else:
                            await AddToStructure(structure, relative_path, file_extension)
        except rarfile.Error as e:
            print(f"Ошибка при работе с архивом {archive_path}: {e}")
            raise
    elif extension == '.rar':
        try:
            with rarfile.RarFile(archive_path, 'r') as archive:
                all_folders = []
                for file_info in archive.infolist():
                    if file_info.filename.endswith('/'):
                        all_folders.append(file_info.filename)
                if not all_folders:
                    for file_info in archive.infolist():
                        if file_info.filename.count('/') > 0:
                            main_folder = file_info.filename.split('/')[0]
                            all_folders.append(main_folder)
                all_folders = sorted(all_folders)
                for folder in all_folders:
                    await AddToStructure(structure, folder[:-1] if folder[-1] == '/' else folder, "folder")
                for file_info in archive.infolist():
                    relative_path = file_info.filename
                    if relative_path.endswith('/'):
                        continue
                    file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
                    if file_info.is_dir():
                        await AddToStructure(structure, relative_path, "folder")
                    else:
                        if relative_path.endswith(('.zip', '.rar', '.7z')):
                            nested_structure = await ProccessInnerArchiveRAR(directory, archive, archive_path,
                                                                             relative_path,
                                                                             file_extension, depth, max_depth)
                            await AddToStructure(structure, relative_path, file_extension, nested_structure)
                        else:
                            await AddToStructure(structure, relative_path, file_extension)
        except rarfile.Error as e:
            print(f"Ошибка при работе с архивом {archive_path}: {e}")
            raise
    elif extension == '.7z':
        try:
            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                for file_info in archive.list():
                    relative_path = file_info.filename
                    file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
                    if file_info.is_directory:
                        await AddToStructure(structure, relative_path, "folder")
                    else:
                        if relative_path.endswith(('.zip', '.rar', '.7z')):
                            nested_structure = await ProccessInnerArchive7z(directory, archive, archive_path,
                                                                            relative_path,
                                                                            file_extension,
                                                                            depth, max_depth)
                            await AddToStructure(structure, relative_path, file_extension, nested_structure)
                        else:
                            await AddToStructure(structure, relative_path, file_extension)
        except rarfile.Error as e:
            print(f"Ошибка при работе с архивом {archive_path}: {e}")
            raise
    return structure


async def AddToStructure(structure: Dict[str, Any], relative_path: str, type: str,
                         nested_structure: Dict[str, Any] = None):
    parts = relative_path.split('/')
    current = structure
    for part in parts[:-1]:
        found = False
        for item in current.get("in_folder", []):
            if item["name"] == part and item["type"] == "folder":
                current = item
                found = True
                break
        if not found:
            for item in current.get("in_archive", []):
                if item["name"] == part and item["type"] == "folder":
                    current = item
                    found = True
                    break
        if not found and len(parts) == 1 and type == 'folder':
            new_dir = {"name": part, "type": "folder", "in_folder": []}
            if "in_folder" not in current:
                current["in_folder"] = []  # Инициализируем ключ, если его нет
            current["in_folder"].append(new_dir)
            current = new_dir
    if nested_structure:
        if nested_structure["type"] in ['7z', 'rar', 'zip']:
            if "in_archive" not in current:
                current["in_folder"].append(
                    {"name": parts[-1], "type": type, "in_archive": nested_structure["in_archive"]})
            else:
                current["in_archive"].append(
                    {"name": parts[-1], "type": type, "in_archive": nested_structure["in_archive"]})

        elif nested_structure["type"] == 'folder':
            if "in_archive" not in current:
                current["in_folder"].append(
                    {"name": parts[-1], "type": type, "in_folder": nested_structure["in_archive"]})
            else:
                current["in_archive"].append(
                    {"name": parts[-1], "type": type, "in_folder": nested_structure["in_archive"]})
    else:
        if len(parts) == 1 and type == 'folder':
            new_dir = {"name": parts[0], "type": "folder", "in_folder": []}
            current["in_archive"].append(new_dir)
        else:
            if "in_archive" not in current:
                if type == 'folder':
                    current["in_folder"].append({"name": parts[-1], "type": type, "in_folder": []})
                elif type in ['7z', 'rar', 'zip']:
                    current["in_folder"].append({"name": parts[-1], "type": type, "in_archive": []})
                else:
                    current["in_folder"].append({"name": parts[-1], "type": type})
            else:
                if type == 'folder':
                    current["in_archive"].append({"name": parts[-1], "type": type, "in_folder": []})
                elif type in ['7z', 'rar', 'zip']:
                    current["in_archive"].append({"name": parts[-1], "type": type, "in_archive": []})
                else:
                    current["in_archive"].append({"name": parts[-1], "type": type})


async def ProccessInnerArchiveZIP(directory, archive, parent_path, relative_path, file_extension, depth, max_depth):
    temp_dir = tempfile.mkdtemp(dir=directory)
    try:
        file_list = archive.namelist()
        for file_name in file_list:
            if file_name == relative_path:
                archive.extract(file_name, path=temp_dir)
                nested_archive_path = os.path.join(temp_dir, file_name)
                nested_structure = await GetEntityFromInnerFile(directory, nested_archive_path, parent_path, depth + 1,
                                                                max_depth)
                return nested_structure
    finally:
        shutil.rmtree(temp_dir)


async def ProccessInnerArchiveRAR(directory, archive, parent_path, relative_path, file_extension, depth, max_depth):
    temp_dir = tempfile.mkdtemp(dir=directory)
    try:
        file_list = archive.namelist()
        for file_name in file_list:
            if file_name == relative_path:
                archive.extract(file_name, path=temp_dir)
                nested_archive_path = os.path.join(temp_dir, file_name)
                nested_structure = await GetEntityFromInnerFile(directory, nested_archive_path, parent_path, depth + 1,
                                                                max_depth)
                return nested_structure
    finally:
        shutil.rmtree(temp_dir)


async def ProccessInnerArchive7z(directory, archive, parent_path, relative_path, file_extension, depth, max_depth):
    temp_dir = tempfile.mkdtemp(dir=directory)
    try:
        for file_info in archive.list():
            file_name = file_info.filename
            if file_name == relative_path:
                archive.extract(targets=[file_name], path=temp_dir)
                nested_archive_path = os.path.join(temp_dir, file_name)
                nested_structure = await GetEntityFromInnerFile(directory, nested_archive_path, parent_path, depth + 1,
                                                                max_depth)
                return nested_structure
    finally:
        shutil.rmtree(temp_dir)


async def TakeEntityInArchiveFile(filepath):
    file_list = []
    if not (zipfile.is_zipfile(filepath) or py7zr.is_7zfile(filepath) or rarfile.is_rarfile(filepath)):
        return None
    excluded_extensions = ('.xlsx', '.xlsm', '.docx', '.pptx', '.odt', '.ods', '.odp')
    if filepath.lower().endswith(excluded_extensions):
        return None
    try:
        directory = filepath.replace(os.path.basename(filepath), "")
        print("\nВыполняется сбор информации о составе архива...")
        structure = await GetEntityFromInnerFile(directory, filepath)
        return structure if structure else None
    except Exception as e:
        print(e)
        file_list.append({"name": "-", "type": "-"})
    return file_list


def DeleteCollectionFolder(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f'Коллекция "{folder_path}" удалена, так как в неё не были добавлены файлы.')
    except OSError as e:
        print(f'Ошибка при удалении папки "{folder_path}": {e}')


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


def AreSimilar(file1, file2, threshold=0.87):
    seq = SequenceMatcher(None, file1, file2)
    return seq.ratio() >= threshold


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


def FindMessageIdForFile(file_names, mass_sizes_for_check, mass_id):
    new_mass_id = []
    filenames_in_folder = []
    downloaded_files_paths = folders.get("files_path")
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


async def get_all_dialogs(client):
    dialogs = []
    async for dialog in client.iter_dialogs():
        dialogs.append(dialog)
    return dialogs


async def is_valid_link(url: str) -> bool:
    DB_EXTENSIONS = (
        # Архивы
        ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".zst",
        # Текстовые форматы
        ".txt", ".csv", ".tsv", ".json", ".ndjson", ".xml", ".yaml", ".yml",
        # SQL / дампы
        ".sql", ".dump", ".bak", ".backup",
        # Таблицы
        ".xlsx", ".xls", ".xlsb", ".ods",
        # Файлы БД
        ".db", ".sqlite", ".sqlite3", ".mdb", ".accdb", ".parquet", ".feather"
    )
    try:
        async with aiohttp.ClientSession() as session:
            # Сначала пробуем HEAD
            try:
                resp = await session.head(url, allow_redirects=True, timeout=10)
            except Exception:
                # Если HEAD не поддерживается – пробуем GET только с заголовками
                resp = await session.get(url, allow_redirects=True, timeout=10)
            async with resp:
                if resp.status != 200:
                    return False

                content_type = resp.headers.get("Content-Type", "").lower()
                content_length = resp.headers.get("Content-Length")

                # если это HTML — точно не база
                if "text/html" in content_type:
                    return False

                # если сервер явно сказал, что размер = 0
                if content_length is not None and int(content_length) == 0:
                    return False

                # проверяем расширение в URL
                path = urlparse(url).path
                _, ext = os.path.splitext(path)
                if ext.lower() in DB_EXTENSIONS:
                    return True

                # иногда расширения нет, но content-type намекает
                if any(x in content_type for x in ["zip", "rar", "7z", "excel", "spreadsheet", "csv", "sql"]):
                    return True

                return False
    except Exception as e:
        logging.error(f"Ошибка при проверке ссылки {url}: {e}")
        return False


async def download_from_link(url, filepath):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(filepath, "wb") as f:
                    while True:
                        chunk = await resp.content.read(1024 * 1024)  # читаем кусками по 1 МБ
                        if not chunk:
                            break
                        f.write(chunk)
                return True
    return False


async def DownloadFiles(client, channel, num, my_collection, work_collection, marker_of_download):
    meta_info = "-"
    messages_list = []
    count = 0
    count_mes = 0
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
                    message_name = message.file.name
                    mass_names.append(message_name)
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
                        meta_info = await GetMetaInfoInInternet(message_name)
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
        else:
            if message.text:
                try:
                    urls = []
                    for entity in message.entities or []:
                        if isinstance(entity, MessageEntityUrl):
                            # ссылка из текста (полная строка в сообщении)
                            url = message.text[entity.offset: entity.offset + entity.length]
                            if url.startswith(("http://", "https://")):
                                urls.append(url)
                        elif isinstance(entity, MessageEntityTextUrl):
                            # скрытая ссылка (под анкором)
                            if entity.url:
                                urls.append(entity.url)
                    BLOCKED_DOMAINS = ["t.me/fulldbbot", "t.me/fulldbbot/subscription", "t.me/"]
                    urls = [u for u in urls if not any(block in u for block in BLOCKED_DOMAINS)]
                    for url in urls:
                        if await is_valid_link(url):
                            filename = os.path.basename(urlparse(url).path)
                            filepath = os.path.join(folders.get("files_path"), filename)
                            # Скачиваем
                            print(f"Найдена ссылка на БД: {url}, скачиваю...")
                            success = await download_from_link(url, filepath)
                            if not success:
                                print(f"⚠️ Файл по ссылке {url} не удалось скачать.")
                                continue
                            if success:
                                print(f"Файл {filename} успешно скачан по ссылке!")

                                # Добавляем файл в массивы как будто он был вложением
                                mass_names.append(filename)
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
                                    mass_sizes_for_check.append(os.path.getsize(filepath))
                                    if Size(os.path.getsize(filepath)) is not None:
                                        mass_sizes.append(Size(os.path.getsize(filepath)))
                                    else:
                                        mass_sizes.append("-")
                                except Exception:
                                    mass_sizes.append("-")
                                    mass_sizes_for_check.append(0)

                                tmp_info = ''
                                try:
                                    meta_info = await GetMetaInfoInInternet(filename)
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
                                                    info_parts.append(
                                                        f"Информация для предыдущего сообщения: {prev_message.text}")
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
                                        filename,
                                        "\t" * 2,
                                        Size(os.path.getsize(filepath)),
                                        "\nИнформация из текста сообщения: \t",
                                        message.text,
                                        "\nИнформация из интернета: ",
                                        meta_info,
                                    )
                                count += 1
                                if count % 10 == 0:
                                    await CheckConnect(client)
                    print("Сканировано сообщений: ", count_mes + 1)
                    count_mes += 1
                    if count_mes % 10 == 0:
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

    if ChooseAIProcess() == 1:
        print("\nАвтоматическое разделение по коллекциям...")
        clusters = AI_Analyse_For_Collection(mass_names, mass_sizes)
        clusters = Restructure(clusters)
        clusters = AskForRemakeClusters(clusters)
        if Confirm() == 1:
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
        else:
            print("\nВыполнение программы вручную...")
            all_numbers_of_files = ChooseTypeOfDownload(mass_names)
            numbers_of_only_files = all_numbers_of_files[0]
            numbers_of_files_for_collection = all_numbers_of_files[1]
            mass_collections = all_numbers_of_files[2]
            collection_names = all_numbers_of_files[3]
    else:
        all_numbers_of_files = ChooseTypeOfDownload(mass_names)
        numbers_of_only_files = all_numbers_of_files[0]
        numbers_of_files_for_collection = all_numbers_of_files[1]
        mass_collections = all_numbers_of_files[2]
        collection_names = all_numbers_of_files[3]

    await CheckConnect(client)
    if numbers_of_files_for_collection is not None:
        flag_of_collection = True
        for collection_of_files in mass_collections:
            path_to_files = folders.get("collection_files_path")
            index = mass_collections.index(collection_of_files)
            print(
                f"\nВыполняется загрузка  {mass_collections.index(collection_of_files) + 1}й коллекции."
            )
            paths = CreateDirectoryForCollection(path_to_files, collection_names, index)
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

    path_to_files = folders.get("files_path")
    path_to_info = folders.get("images_path")
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


async def DopMenuForChekMessages(client, channel):
    ch = input(
        "\nДля проверки сообщений в произвольном количестве, введите 1, для перехода к действиям, введите 0:\n"
        "Выбор: "
    )
    if ch == "1":
        tmp = input(
            "Введите количество сообщений, которое хотите проверить, но не более 1000: "
        )
        num = CountMessages(tmp)
        await CheckMessage(client, channel, num)
    elif ch == "0":
        return await TelegramWorkMenu(client, channel)
    else:
        print("Введите номер пункта по условию!\n")
        return await DopMenuForChekMessages(client, channel)


async def TelegramWorkMenu(client, channel):
    s = input(
        "\nДействия\n"
        "1. Для выбора канала нажмите 1\n"
        "2. Если хотите проверить все непрочитанные сообщения на наличие файлов, нажмите 2\n"
        "3. Для проверки сообщений на наличие файлов в произвольном количестве, нажмите 3\n"
        "4. Для загрузки файлов, нажмите 4\n"
        "5. Для загрузки только метаинформации, нажмите 5\n"
        "6. Для создания аналитического отчета, нажмите 6\n"
        "7. Для загрузки метаинформации для всех загруженных сообщений из всех каналов, нажмите 7\n"
        "8. Для выхода в главное меню, нажмите 0\n"
        "Выбор: "
    )
    if s == "1":
        channel = await ConnectionToChannel(client)
        return await TelegramWorkMenu(client, channel)

    if s == "2":
        if channel is None:
            print("\nСначала выполните подключение к каналу!")
            return await TelegramWorkMenu(client, channel)
        else:
            unread = channel[2]
            if unread == 0 or unread is None:
                print("Непрочитанных сообщений нет!")
                await DopMenuForChekMessages(client, channel)
            else:
                print(f"В этом канале {unread} непрочитанных сообщений.")
                return await CheckMessage(client, channel, unread)
    elif s == "3":
        if channel is None:
            print("\nСначала выполните подключение к каналу!")
            return await TelegramWorkMenu(client, channel)
        else:
            num = input(
                "Введите количество сообщений, которое хотите проверить, но не более 9999: "
            )
            need_num = CountMessages(num)
            return await CheckMessage(client, channel, need_num)
    elif s == "4" or s == "5":
        if channel is None:
            print("\nСначала выполните подключение к каналу!")
            return await TelegramWorkMenu(client, channel)
        else:
            if s == "4":
                marker_of_download = True
            else:
                marker_of_download = False
            num = input(
                "Введите количество сообщений из которых будут скачиваться файлы, но не более 9999: "
            )
            need_num = CountMessages(num)
            print()
            my_collection = ConnectToMongo(collection_parameters=MyMongoCollection())
            work_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
            print("Успешное подключение к серверу MongoDB!")
            await DownloadFiles(client, channel, need_num, my_collection, work_collection, marker_of_download)
            print("\nПроцесс загрузки завершен.")
            stop = True
            return stop

    elif s == "6":
        await AnaliticWork(client)
        # status = await Start(client)
        # if status:
        # AnaliticWork(client)
        time.sleep(3)
        return await TelegramWorkMenu(client, channel)
    elif s == "7":
        dialogs = await get_all_dialogs(client)
        print("Начинается потоковая выгрузка всей информации для скачанных заранее файлов...")
        my_collection = ConnectToMongo(collection_parameters=MyMongoCollection())
        work_collection = ConnectToMongo(collection_parameters=TmpMongoCollection())
        marker_of_download = False
        bf_flag = False
        for dialog in dialogs:
            channel_id = dialog.entity.id
            channel_name = dialog.name
            if channel_name == "BF Files":
                bf_flag = True
            tmp_channel = [channel_name, channel_id]
            print("Успешное подключение к серверу MongoDB!")
            print(f"\nПроцесс загрузки метаинформации для {channel_name}...")
            await DownloadFiles(client, tmp_channel, 1500, my_collection, work_collection, marker_of_download)
            print(f"\nПроцесс загрузки для {channel_name} завершен.")
        if not bf_flag:
            bf_channel = await client.get_entity("t.me/BF_files")
            channel_name = bf_channel.title
            tmp_channel = [channel_name, bf_channel.id]
            print(f"\nПроцесс загрузки метаинформации для {channel_name}...")
            await DownloadFiles(client, tmp_channel, 1500, my_collection, work_collection, marker_of_download)
            print(f"\nПроцесс загрузки для {channel_name} завершен.")
        return await TelegramWorkMenu(client, channel)
    elif s == "0":
        # await client.disconnect()
        stop = True
        return stop
    else:
        print("Введите номер пункта по условию!\n")
        return await TelegramWorkMenu(client, channel)
