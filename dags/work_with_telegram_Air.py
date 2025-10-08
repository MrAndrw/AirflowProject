import glob
import hashlib
import os.path
import shutil
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
from InfoFinder import *
import asyncio
from BaseAnalyse import *
from set_parameters import *
import subprocess
from typing import Optional, Dict, Any
from ArchivatorWithPasswords import TryOpenArchiveAndReturnPassword
from pathlib import Path
import uuid

logging.basicConfig(level=logging.INFO)


def TmpMongoCollection():
    mongo_url = (
        f"mongodb://{mongo_user_name}:{mongo_password}@{ip_addr}:27017/?authMechanism=DEFAULT"
    )
    return mongo_url, work_database_name, work_collection_name


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


def preprocess_filenames(filenames):
    print("\nОбработка имен файлов для более точной группировки...")

    def clean_filename(filename):
        return re.sub(r'[^\w\s]', '', filename).strip().lower()

    def get_base_name(filename):
        base_name = re.sub(r'\(.*\)', '', filename)
        return re.sub(r'\.\w+$', '', base_name)

    return [clean_filename(get_base_name(filename)) for filename in filenames]


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
    video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm")
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


def DisplayClusters(clusters):
    clusters_list = [(cluster_id, file_names) for cluster_id, file_names in clusters.items() if file_names]
    clusters_list_sorted = sorted(clusters_list, key=lambda x: x[1][0])
    sorted_clusters = {cluster_id: file_names for cluster_id, file_names in clusters_list_sorted}
    print("\nРезультаты группировки файлов: ")
    print("Номер группы:", "\t" * 5, "Названия файлов:")
    for cluster_id, file_names in sorted_clusters.items():
        print(cluster_id, "\t" * 3, file_names[0] if len(file_names) == 1 else file_names)


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
    path_to_files = folders_Air.get("collection_files_path")
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


def CreateDirectoryForLargeFoldersArch():
    path_to_files = folders_Air.get("collection_files_path")
    test_path_to_files = os.path.join(
        path_to_files,
        f"autoprocess_tmp",
    )
    if not os.path.exists(test_path_to_files):
        os.makedirs(test_path_to_files, exist_ok=True)
        print(f"Создана папка {test_path_to_files}.")
        return test_path_to_files
    else:
        print(f"Используется папка {test_path_to_files}")
        return test_path_to_files


last = 0


def IsNeedFile(filename):
    image_extensions = (".jpeg", ".png", ".jpg", ".bmp", ".dib", ".rle", ".pdf", ".gif", ".tiff", ".tgs")
    scripts_extensions = (".html", ".css", ".php", ".py", ".asp", ".xml", ".htm", ".exe")
    audios_extensions = (".mp3", ".wav", ".ogg", ".flac", ".wma")
    video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".webp")
    all_ext = image_extensions + scripts_extensions + audios_extensions + video_extensions
    return True if not filename.lower().endswith(all_ext) else False


async def StartDownloadFiles(
        count,  # счетчик для сообщений
        i,  # итерация для сообщений
        client,  # клиент телеграмм
        channel_id,  # id канала
        num,  # количество сообщений
        work_collection,  # рабочая коллекция files_hashes в mongodb
        massive_names,  # список имен файлов
        massive_id,  # список id файлов
        massive_sizes,  # список размера файлов
        path_to_file,  # путь к загружаемым файлам
):
    await CheckConnect(client)
    filename = massive_names[i]
    file_size = massive_sizes[i]
    filepath = os.path.join(path_to_file, filename)
    if not os.path.splitext(filename)[1]:
        filename += '.txt'  # Добавляем расширение .txt
        filepath = os.path.join(path_to_file, filename)

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
            try:
                if message.document:
                    if not IsNeedFile(filename):
                        file_size_mb = file_size / (1024 * 1024)
                        ext = os.path.splitext(filename)[1].lower()
                        print(f"Файл {filename} ({file_size_mb:.2f} МБ) с расширением {ext} пропущен.")
                        return count - 1
            except Exception as e:
                print(e)
            print(f"\nЗагрузка файла {filename} \t {file_size}...")
            await download_file(client, message, filepath, progress_callback=progress_callback)
    try:
        file_hash = TakeHash(filepath)
    except Exception as e:
        print(f"Файл {filename} пропущен.")
        return count - 1
    if CheckFilehashInMongo(work_collection, file_hash) is not False:
        print(f"Файл {filename} уже есть в базе данных!")
        os.remove(filepath)
        count -= 1
        print("Файл удален!")
    else:
        print("Загружено файлов: ", count)
    return count


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
        if marker_of_download is False:
            try:
                downloaded_files_paths = folders_Air.get("files_path")
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
                print(f"\nЗагрузка файла {filename} \t {file_size}...")
                await download_file(client, message, filepath, progress_callback=progress_callback)
    else:
        print(f"\nЗагрузка метаинформации для файла {filename} \t {file_size}...")
        await asyncio.sleep(1)

    download_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        file_hash = TakeHash(filepath)
    except Exception as e:
        print(f"Файл {filename} пропущен.")
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
        is_large_folders_arch = IsLargeFoldersArchive(file_entity)
        file_type = 'folder' if os.path.isdir(filepath) else 1
        report = RunFileAnalyse(filepath)
        file_report = report[0]
        is_mail_pass_file = report[1]
        if is_mail_pass_file is True:
            normalize_folder = CreateDirectoryForMailPassFilesAir()
            path_to_files = filepath.split(f'/{os.path.basename(filepath)}')[0]
            print(path_to_files)
            if not os.path.exists(normalize_folder):
                os.makedirs(normalize_folder, exist_ok=True)
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
            try:
                shutil.move(filepath, normalize_folder)
                infofilepath = os.path.join(normalize_folder, "info_file")
                filepath = normalize_filepath
            except Exception as e:
                print("Ошибка при переносе файла с Mail+Pass. Он будет загружен как обычный файл без "
                      " автоматической обработки строк.")

        elif is_large_folders_arch:
            normalize_folder = CreateDirectoryForLargeFoldersArch()
            path_to_files = filepath.split(f'/{os.path.basename(filepath)}')[0]
            if not os.path.exists(normalize_folder):
                os.makedirs(normalize_folder, exist_ok=True)
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
            try:
                shutil.move(filepath, normalize_folder)
                infofilepath = os.path.join(normalize_folder, "info_file")
                filepath = normalize_filepath
            except Exception as e:
                print("Ошибка при переносе файла с большим количеством папок. Он будет загружен как обычный файл без "
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

        # if is_large_folders_arch:
        #     #exit(0)

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


async def CheckConnect(client):
    if not client.is_connected():
        print("\nКлиент был отключен, повторное подключение...")
        await client.connect()
        print("Подключение восстановлено.\n")
    else:
        print("\nТекущее подключение к клиенту в норме.\n")


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
    downloaded_files_paths = folders_Air.get("files_path")
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


async def GetEntityFromInnerFile(tmp_directory, archive_path: str, parent_path: str = '', depth: int = 0,
                                 max_depth: int = 5, password: Optional[str] = None) -> \
        Dict[str, Any]:
    base_dir = Path(__file__).parent
    passwords_file = base_dir / 'passwords'
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
        for encoding in ['cp866', 'utf-8']:
            try:
                with zipfile.ZipFile(archive_path, 'r', metadata_encoding=encoding) as archive:
                    for file_info in archive.infolist():
                        relative_path = file_info.filename
                        if relative_path.endswith('/'):
                            relative_path = relative_path[:-1]
                        file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
                        if file_info.is_dir():
                            await AddToStructure(structure, relative_path, "folder")
                        else:
                            if relative_path.endswith(('.zip', '.rar', '.7z')):
                                nested_structure = await ProccessInnerArchiveZIP(
                                    tmp_directory, archive, archive_path,
                                    relative_path, file_extension, depth, max_depth, password
                                )
                                await AddToStructure(structure, relative_path, file_extension, nested_structure)
                            else:
                                await AddToStructure(structure, relative_path, file_extension)
                    break  # если всё прошло успешно — выходим из цикла
            except Exception as e:
                print(f"⚠️ Ошибка с кодировкой {encoding} при работе с архивом {archive_path}: {e}")
        else:
            # Если обе попытки не сработали — пробуем пароль или возвращаем structure
            password = await TryOpenArchiveAndReturnPassword(archive_path, str(passwords_file))
            if password:
                return await GetEntityFromInnerFile(tmp_directory, archive_path, parent_path, depth, max_depth,
                                                    password)
            return structure
    elif extension == '.rar':
        temp_dir = None
        try:
            # temp_dir = tempfile.mkdtemp(dir=tmp_directory)
            temp_dir = tmp_directory
            command = [
                r'/usr/bin/7z',
                # r'C:\Program Files\7-Zip\7z.exe',
                'x',  # extract with full paths
                *([f'-p{password}'] if password else []),  # пароль, даже если None
                '-y',  # ответ "да" на все запросы
                f'-o{temp_dir}',  # директория распаковки
                archive_path
            ]
            result = subprocess.run(command, capture_output=True, text=True)
            if "Wrong password" in result.stdout or "Can not open encrypted archive" in result.stdout:
                print(f"❌ Неверный пароль для {archive_path}")
                archive_password = await TryOpenArchiveAndReturnPassword(archive_path, str(passwords_file))
                if archive_password:
                    return await GetEntityFromInnerFile(
                        tmp_directory, archive_path, parent_path, depth, max_depth, archive_password
                    )
                return structure
            for root, dirs, files in os.walk(temp_dir):
                for folder in dirs:
                    relative_path = os.path.relpath(os.path.join(root, folder), temp_dir)
                    await AddToStructure(structure, relative_path, "folder")
                for file in files:
                    relative_path = os.path.relpath(os.path.join(root, file), temp_dir)
                    file_extension = os.path.splitext(file)[1].lower().replace(".", "")
                    # abs_file_path = os.path.join(root, file)
                    if file.endswith(('.zip', '.rar', '.7z')):
                        nested_structure = await ProccessInnerArchiveRAR(tmp_directory, archive_path, relative_path,
                                                                         file_extension, depth, max_depth, password)
                        await AddToStructure(structure, relative_path, file_extension, nested_structure)
                    else:
                        await AddToStructure(structure, relative_path, file_extension)

            # with rarfile.RarFile(archive_path, 'r', pwd=password) as archive:
            #     all_folders = []
            #     for file_info in archive.infolist():
            #         if file_info.filename.endswith('/'):
            #             all_folders.append(file_info.filename)
            #
            #     if not all_folders:
            #         for file_info in archive.infolist():
            #             if file_info.filename.count('/') > 0:
            #                 main_folder = file_info.filename.split('/')[0]
            #                 all_folders.append(main_folder)
            #
            #     all_folders = sorted(all_folders)
            #     for folder in all_folders:
            #         await AddToStructure(structure, folder[:-1] if folder[-1] == '/' else folder, "folder")
            #     for file_info in archive.infolist():
            #         relative_path = file_info.filename
            #         if relative_path.endswith('/'):
            #             continue
            #         file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
            #         if file_info.is_dir():
            #             await AddToStructure(structure, relative_path, "folder")
            #         else:
            #             if relative_path.endswith(('.zip', '.rar', '.7z')):
            #                 nested_structure = await ProccessInnerArchiveRAR(directory, archive, archive_path,
            #                                                                  relative_path,
            #                                                                  file_extension, depth, max_depth, password)
            #                 await AddToStructure(structure, relative_path, file_extension, nested_structure)
            #             else:
            #                 await AddToStructure(structure, relative_path, file_extension)
        except Exception as e:
            print(f"Ошибка при работе с архивом {archive_path}: {e}")
            password = await TryOpenArchiveAndReturnPassword(archive_path, str(passwords_file))
            if password:
                return await GetEntityFromInnerFile(tmp_directory, archive_path, parent_path, depth,
                                                    max_depth, password)
            return structure
        finally:
            # if temp_dir and os.path.exists(temp_dir):
            #     shutil.rmtree(temp_dir)
            prepare_temp_dir()
    elif extension == '.7z':
        try:
            with py7zr.SevenZipFile(archive_path, mode='r', password=password) as archive:
                for file_info in archive.list():
                    relative_path = file_info.filename
                    file_extension = os.path.splitext(relative_path)[1].lower().replace(".", "")
                    if file_info.is_directory:
                        await AddToStructure(structure, relative_path, "folder")
                    else:
                        if relative_path.endswith(('.zip', '.rar', '.7z')):
                            nested_structure = await ProccessInnerArchive7z(tmp_directory, archive, archive_path,
                                                                            relative_path,
                                                                            file_extension,
                                                                            depth, max_depth, password)
                            await AddToStructure(structure, relative_path, file_extension, nested_structure)
                        else:
                            await AddToStructure(structure, relative_path, file_extension)
        except rarfile.Error as e:
            print(f"Ошибка при работе с архивом {archive_path}: {e}")
            password = await TryOpenArchiveAndReturnPassword(archive_path, str(passwords_file))
            if password:
                return await GetEntityFromInnerFile(tmp_directory, archive_path, parent_path, depth,
                                                    max_depth, password)
            return structure
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


def prepare_temp_dir():
    BASE_TEMP_DIR = '/opt/airflow/temp_unpacks'
    if os.path.exists(BASE_TEMP_DIR):
        for filename in os.listdir(BASE_TEMP_DIR):
            file_path = os.path.join(BASE_TEMP_DIR, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"Ошибка при удалении {file_path}: {e}")
    else:
        os.makedirs(BASE_TEMP_DIR, exist_ok=True)


async def ProccessInnerArchiveZIP(tmp_directory, archive, parent_path, relative_path, file_extension, depth, max_depth,
                                  password):
    # temp_dir = tempfile.mkdtemp(dir=directory)
    temp_dir = tmp_directory
    try:
        file_list = archive.namelist()
        for file_name in file_list:
            if file_name == relative_path:
                archive.extract(file_name, path=temp_dir, pwd=bytes(password, 'utf-8') if password else None)
                nested_archive_path = os.path.join(temp_dir, file_name)
                nested_structure = await GetEntityFromInnerFile(tmp_directory, nested_archive_path, parent_path,
                                                                depth + 1,
                                                                max_depth, password)
                return nested_structure
    finally:
        prepare_temp_dir()
        # shutil.rmtree(temp_dir)


async def ProccessInnerArchiveRAR(tmp_directory, parent_path, relative_path, file_extension, depth, max_depth,
                                  password):
    # temp_dir = tempfile.mkdtemp(dir=directory)
    temp_dir = tmp_directory
    try:
        # Распаковываем нужный файл из архива через 7z.exe
        cmd = [
            r'/usr/bin/7z',
            'e',  # extract files
            *([f'-p{password}'] if password else []),
            '-o' + temp_dir,
            parent_path,
            relative_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Ошибка распаковки вложенного архива {relative_path}: {result.stderr}")
            return None
        nested_archive_path = os.path.join(temp_dir, os.path.basename(relative_path))
        nested_structure = await GetEntityFromInnerFile(tmp_directory, nested_archive_path, parent_path, depth + 1,
                                                        max_depth, password)
        return nested_structure

        # file_list = archive.namelist()
        # for file_name in file_list:
        #     if file_name == relative_path:
        #         archive.extract(file_name, path=temp_dir, pwd=password)
        #         nested_archive_path = os.path.join(temp_dir, file_name)
        #         nested_structure = await GetEntityFromInnerFile(directory, nested_archive_path, parent_path, depth + 1,
        #                                                         max_depth, password)
        #         return nested_structure
    finally:
        prepare_temp_dir()
        # shutil.rmtree(temp_dir)


async def ProccessInnerArchive7z(tmp_directory, archive, parent_path, relative_path, file_extension, depth, max_depth,
                                 password):
    # temp_dir = tempfile.mkdtemp(dir=directory)
    temp_dir = tmp_directory
    try:
        for file_info in archive.list():
            file_name = file_info.filename
            if file_name == relative_path:
                archive.extract(targets=[file_name], path=temp_dir)
                nested_archive_path = os.path.join(temp_dir, file_name)
                nested_structure = await GetEntityFromInnerFile(tmp_directory, nested_archive_path, parent_path,
                                                                depth + 1,
                                                                max_depth)
                return nested_structure
    finally:
        prepare_temp_dir()
        # shutil.rmtree(temp_dir)


async def TakeEntityInArchiveFile(filepath):
    file_list = []
    if not (zipfile.is_zipfile(filepath) or py7zr.is_7zfile(filepath) or rarfile.is_rarfile(filepath)):
        return None
    excluded_extensions = ('.xlsx', '.xlsm', '.docx', '.pptx', '.odt', '.ods', '.odp')
    if filepath.lower().endswith(excluded_extensions):
        return None
    try:
        tmp_directory = '/opt/airflow/temp_unpacks'
        prepare_temp_dir()
        print("\nВыполняется сбор информации о составе архива...")
        structure = await GetEntityFromInnerFile(tmp_directory, filepath)
        return structure if structure else None
    except Exception as e:
        print(e)
        file_list.append({"name": "-", "type": "-"})
    return file_list


def IsLargeFoldersArchive(structure):
    if not structure or not isinstance(structure, dict):
        return False
    if structure.get("type") != "folder":
        return False  # Файлы не содержат вложений

        # Получаем содержимое текущей папки
    items = structure.get("in_folder", [])
    file_names = set()
    folder_names = set()

    # Сначала собираем имена файлов и папок
    for item in items:
        name = item.get("name", "").lower()
        if item.get("type") == "folder":
            folder_names.add(name)
        else:
            file_names.add(name)

    # Проверка условия:
    # - наличие файла "all passwords"
    # - и хотя бы одной нужной папки
    if "all passwords" in file_names and folder_names.intersection({"applications", "chrome", "cookies"}):
        return True

    # Рекурсивный обход вложенных папок
    for item in items:
        if item.get("type") == "folder":
            if IsLargeFoldersArchive(item):
                return True

    return False


def DeleteCollectionFolder(folder_path):
    try:
        shutil.rmtree(folder_path)
        print(f'Коллекция "{folder_path}" удалена, так как в неё не были добавлены файлы.')
    except OSError as e:
        print(f'Ошибка при удалении папки "{folder_path}": {e}')
