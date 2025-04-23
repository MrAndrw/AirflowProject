# from work_with_telegram_Air import *
import glob
import re
from set_parameters import *
import time
from datetime import datetime
import asyncio

start_time = time.time()


def FilesMetainfoCollectionPaths():
    file_path = folders_Air.get("files_path")
    image_path = folders_Air.get("images_path")
    collection_file_path = folders_Air.get("collection_files_path")

    files = list(filter(os.path.isfile, glob.glob(f"{file_path}/*") + glob.glob(f"{file_path}/.*")))
    files.sort(key=lambda x: os.path.getctime(x))

    collections = list(filter(os.path.isdir, glob.glob(f"{collection_file_path}/*")))
    collections.sort(key=lambda x: os.path.getctime(x))

    images = list(filter(os.path.isfile, glob.glob(f"{image_path}/*") + glob.glob(f"{image_path}/.*")))
    images.sort(key=lambda x: os.path.getctime(x))

    return files, images, collections


def Archives_Path():
    archive_path = folders_Air.get("directory")
    archives_with_duplicates_path = folders_Air.get("archives_with_duplicates")
    archives = list(filter(os.path.isfile, glob.glob(f"{archive_path}/*") + glob.glob(f"{archive_path}/.*")))
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


def Update(collection, file_name, upload_time):
    document_archive = collection.find_one({"archive_name": file_name})
    if document_archive is not None:
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


def MergeMongoDocuments(archive_name, archive_size, archive_info, archive_channel, file_hashes, my_mongo_collection,
                        work_mongo_collection):
    print("Выполняется создание общего документа MongoDB...")
    merged_document = {'archive_name': archive_name, 'archive_channel': archive_channel, 'archive_size': archive_size,
                       'archive_info': archive_info, 'files': []}  # Создаем новый документ
    for file_hash in file_hashes:
        existing_doc = my_mongo_collection.find_one({'hash': file_hash})
        if existing_doc:
            merged_document['files'].append(existing_doc)
        else:
            print(f"Не найден документ по указанным данным: \n"
                  f"\t\t\t\t ХЭШ файла: {file_hash}\n"
                  f"\t\t\t\t Имя архива: {archive_name}\n"
                  f"\t\t\t\t Канал: {archive_channel}\n"
                  f"\t\t\t\t Информация об архиве: {archive_info}\n")
    if not merged_document['files']:
        return False
    work_mongo_collection.insert_one(
        merged_document)
    my_mongo_collection.insert_one(
        merged_document)
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


async def cancel_all_tasks():
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
