import zipfile
import py7zr
import rarfile
import os
import tempfile
import shutil
import subprocess
import asyncio
from functools import partial
import uuid
from pathlib import Path


async def TryOpenArchiveAndReturnPassword(filepath, passwords_file):
    loop = asyncio.get_running_loop()
    password = await loop.run_in_executor(None, partial(OpenArchiveWithPassword, filepath, passwords_file))
    return password


def create_unpack_dir(archive_path: str) -> str:
    BASE_UNPACK_DIR = '/opt/airflow/temp_unpacks'
    base_name = os.path.splitext(os.path.basename(archive_path))[0]
    unique_folder = f"{base_name}_{uuid.uuid4().hex}"
    temp_dir = os.path.join(BASE_UNPACK_DIR, unique_folder)
    os.makedirs(temp_dir, exist_ok=True)
    return temp_dir


def clear_directory(directory: str):
    """Удаляет все файлы и папки внутри директории, но не саму директорию."""
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)


def delete_directory(path: str):
    dir_path = Path(path)
    if dir_path.exists() and dir_path.is_dir():
        shutil.rmtree(dir_path)


def OpenArchiveWithPassword(filepath, passwords_file):
    print(f"Подбираем пароль для архива {os.path.basename(filepath)}...")
    if not os.path.isfile(filepath):
        print("ZIP-архив не найден.")
        return None
    if not os.path.isfile(passwords_file):
        print("Файл с паролями не найден.")
        return None
    if not (zipfile.is_zipfile(filepath) or py7zr.is_7zfile(filepath) or rarfile.is_rarfile(filepath)):
        print("Файл не является архивом!")
        return None
    excluded_extensions = ('.xlsx', '.xlsm', '.docx', '.pptx', '.odt', '.ods', '.odp')
    if filepath.lower().endswith(excluded_extensions):
        print("Файл не является архивом!")
        return None
    with open(passwords_file, 'r', encoding='utf-8') as f:
        passwords = [line.strip() for line in f if line.strip()]

    if zipfile.is_zipfile(filepath):
        with zipfile.ZipFile(filepath) as archive:
            encrypted_files = [info for info in archive.infolist() if info.flag_bits & 0x1]
            if not encrypted_files:
                print("⚠️ Архив не содержит зашифрованных файлов.")
                return None
            for password in passwords:
                try:
                    temp_dir = create_unpack_dir(filepath)
                    clear_directory(temp_dir)
                    archive.read(encrypted_files[0], pwd=bytes(password, 'utf-8'))
                    print(f"✅ Пароль подошёл: {password}")
                    delete_directory(temp_dir)
                    return password
                except Exception as e:
                    # print(f"⚠️ Ошибка: {e}")
                    if os.path.exists(temp_dir):
                        delete_directory(temp_dir)
                    continue
            print("❌ Не удалось найти подходящий пароль.")
            try:
                if os.path.exists(temp_dir):
                    delete_directory(temp_dir)
            except Exception as e:
                pass
            return None

    elif py7zr.is_7zfile(filepath):
        temp_dir = None
        for password in passwords:
            try:
                # print(f"🔑 Пробуем пароль: {password}")
                with py7zr.SevenZipFile(filepath, mode='r', password=password) as archive:
                    file_list = archive.getnames()
                    if not file_list:
                        print("⚠️ Архив пуст.")
                        continue
                temp_dir = create_unpack_dir(filepath)
                clear_directory(temp_dir)
                try:
                    def normalize_path(path):
                        return os.path.normpath(path).replace(os.sep, '/').rstrip('/')

                    with py7zr.SevenZipFile(filepath, mode='r', password=password) as archive:
                        archive.extract(targets=[file_list[0]], path=temp_dir)
                        print(f"✅ Пароль подошёл: {password}")
                        delete_directory(temp_dir)
                        return password
                    ### это нужно для дальнейшей разархивации (применение еще не реализовано)
                    #     archive.extractall(path=temp_dir)
                    # extracted_files = []
                    # for root, dirs, files in os.walk(temp_dir):
                    #     for dir in dirs:
                    #         rel_dir_path = os.path.relpath(os.path.join(root, dir), temp_dir)
                    #         extracted_files.append(normalize_path(rel_dir_path))
                    #     for file in files:
                    #         rel_path = os.path.relpath(os.path.join(root, file), temp_dir)
                    #         extracted_files.append(normalize_path(rel_path))
                    # normalized_file_list = [normalize_path(f) for f in file_list]
                    # matched_count = sum(1 for f in normalized_file_list if f in extracted_files)
                    # matched_percent = matched_count / len(file_list)
                    # if matched_percent > 0.89:
                    #     print(f"✅ Пароль подошёл: {password}")
                    #     delete_directory(temp_dir)
                    #     return password
                    # elif matched_percent < 0.89:
                    #     print(f"⚠️ Извлечены не все файлы — пароль {password} не подошёл полностью."
                    #           f"Процент разархивированных файлов — {matched_percent * 100}")
                    #     delete_directory(temp_dir)
                    #     continue
                except Exception as e:
                    delete_directory(temp_dir)
                    continue

            except (py7zr.exceptions.PasswordRequired,
                    py7zr.exceptions.UnsupportedCompressionMethodError,
                    RuntimeError, ValueError,
                    py7zr.exceptions.Bad7zFile,
                    Exception) as e:
                # print(f"⚠️ Ошибка: {e}")
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        delete_directory(temp_dir)
                    except Exception as del_err:
                        print(f"⚠️ Ошибка при удалении временной папки: {del_err}")
                if isinstance(e, py7zr.exceptions.Bad7zFile):
                    print("❌ Архив повреждён.")
                    return None
                continue
        print("❌ Не удалось подобрать пароль для 7z.")
        try:
            if temp_dir and os.path.exists(temp_dir):
                delete_directory(temp_dir)
        except Exception as e:
            pass
        return None

    elif rarfile.is_rarfile(filepath):
        for password in passwords:
            try:
                # print(f"🔑 Пробуем пароль: {password}")
                command = [
                    r'/usr/bin/7z',
                    # r'C:\Program Files\7-Zip\7z.exe',
                    't',  # 't' = test archive
                    f'-p{password}',  # пароль
                    filepath
                ]
                result = subprocess.run(command, capture_output=True, text=True)
                if "Everything is Ok" in result.stdout:
                    print(f"✅ Пароль подошёл: {password}")
                    return password

                if "Wrong password" in result.stdout or "Can not open encrypted archive" in result.stdout:
                    print(f"❌ Неверный пароль: {password}")
                    continue

                # with rarfile.RarFile(filepath) as archive:
                #     file_list = archive.infolist()
                #     if not file_list:
                #         print("⚠️ Архив пуст или нераспознаваем.")
                #         continue
                #     test_file = archive.infolist()[0]
                #     archive.read(test_file, pwd=password)
                #     archive.extractall(pwd=password)
                #     print(f"✅ Пароль подошёл: {password}")
                #     return
            except RuntimeError as e:
                continue
            except Exception as e:
                # print(f"⚠️ Ошибка: {e}")
                continue
        print("❌ Не удалось подобрать пароль для RAR.")
        return None

# if __name__ == "__main__":
#     # путь к запароленному архиву
#     filepath = r'C:\Users\and23\Downloads\@BabaCloudLogs { 335 } Cloud Logs 18_06_2025.7z'
#     # путь к файлу с паролями
#     passwords_file = r"passwords"
#     OpenArchiveWithPassword(filepath, passwords_file)
