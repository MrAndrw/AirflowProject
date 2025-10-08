import re
import os
from collections import Counter
from datetime import datetime
import time
import pandas as pd
import spacy
from spacy.matcher import PhraseMatcher
from langdetect import detect
from dags.InformationForFileAnalysis import *
import random


def TakePositionsAndCompanies(nlp, lang):
    matcher = PhraseMatcher(nlp.vocab)
    if lang == 'ru':
        positions = positions_ru
        company_names = company_names_ru
        patterns_ru = [nlp.make_doc(text) for text in company_names_ru + positions_ru]
        matcher.add("Entities", None, *patterns_ru)
        return matcher, positions, company_names
    elif lang == 'en':
        positions = positions_en
        company_names = company_names_en
        patterns_en = [nlp.make_doc(text) for text in company_names_en + positions_en]
        matcher.add("Entities", None, *patterns_en)
        return matcher, positions, company_names

    elif lang == 'uk':
        positions = positions_ukr
        company_names = company_names_ukr
        patterns_ukr = [nlp.make_doc(text) for text in company_names_ukr + positions_ukr]
        matcher.add("Entities", None, *patterns_ukr)
        return matcher, positions, company_names

    elif lang == 'zh':
        positions = positions_zh
        company_names = company_names_zh
        patterns_zh = [nlp.make_doc(text) for text in company_names_zh + positions_zh]
        matcher.add("Entities", None, *patterns_zh)
        return matcher, positions, company_names


def identify_file_lang_csv(head):
    try:
        lang = detect(head)
    except Exception as e:
        print(e)
        return 'en'
    language = ['en', 'ru', 'uk', 'zh']
    return lang if lang in language else 'en'


def identify_file_lang(head):
    try:
        lang = detect(head.strip())
    except Exception as e:
        print(e)
        return 'en'
    language = ['en', 'ru', 'uk', 'zh']
    return lang if lang in language else 'en'


def MatchWithPatterns(record):
    if re.match(r"[^@]+@[^@]+\.[^@]+", record):
        return 'Email'
    elif re.match(r"\b(\d{4}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}\.\d{1,2}"
                  r"\.\d{1,2}|\d{1,2}\.\d{1,2}\.\d{4}|\d{1,2}[-/]\d{4}|\d{4}[-/]\d{1,2})\b", record):
        return 'Date'
    elif re.match(r"^[+-]?([1-8]?\d(\.\d+)?|90(\.0+)?)[,;]\s*[+-]?(180(\.0+)?|1[0-7]?\d(\.\d+)?)$", record):
        return 'Coordinates'
    elif re.match(r"^\d{3}-\d{3}-\d{4}$", record) or re.match(r"^\+\d{1,3}\s?\d{1,14}$|\d{10,17}", record):
        return 'Phone'
    elif re.match(r"\b\d{16}\b|\b[0-9A-Fa-f]{32}\b", record):
        return 'Hash'
    # elif re.match(r"[0-9]{3,}", record):
    #     return 'ID/Number'
    elif re.match(r"^[A-ZА-Я][a-zа-яё]{2,10}|[a-zа-яё]{2,10}$", record):
        return 'First Name'
    elif re.match(r"^[A-Z][a-z]+(?: [A-Z][a-z]+)*$", record):
        return 'Full Name'

    elif re.match(r"^[a-zA-Z\s]+$", record):
        return 'Description'
    elif re.match(r"^\d{5}(-\d{4})?$|\d{6}|\d{3}(-)?\d{3}", record):
        return 'Zip'

    elif re.match(r'^(?=.*[A-Z]*)(?=.*\d)(?=.*[!@#$%^&*()_+]*)*[A-Za-z\d!.,\'\-@#$%^&*()_+]{6,}$', record):
        return 'Password'
    return 'Unknown'


def MatchWithAI(record, tmp_nlp, lang):
    parameters = TakePositionsAndCompanies(tmp_nlp, lang)
    # if len(parameters) == 5:
    #     nlp, matcher, positions, country_names, company_names = parameters
    matcher, positions, company_names = parameters
    matcher = matcher
    nlp = tmp_nlp
    doc = nlp(record)
    # Проверим наличие имени в тексте
    for ent in doc.ents:
        if ent.label_ == "PERSON":
            return 'Full Name'
        elif ent.label_ == "GPE":  # Местоположения/страны
            return 'Country'
        elif ent.label_ == "ORG":  # Организации/компании
            return 'Company'
        elif ent.label_ == "WORK_OF_ART":  # Должности (например, профессии)
            return 'Position'
        elif ent.label_ == "DATE":  # Должности (например, профессии)
            return 'Date'

    # Используем PhraseMatcher для поиска в компаниях, странах и должностях
    matches = matcher(doc)
    for match_id, start, end in matches:
        matched_span = doc[start:end].text
        if matched_span in company_names:
            return 'Company'
        if matched_span in country_names:
            return 'Country'
        if matched_span in positions:
            return 'Position'
    return 'Unknown'


def identify_record_type(record, tmp_nlp, lang):
    type_of_record = MatchWithPatterns(str(record))
    return type_of_record
    # return type_of_record if type_of_record != 'Unknown' else MatchWithAI(str(record), tmp_nlp, lang)


def analyze_data(dataframe, language_models):
    report = {}
    text = dataframe.head().to_string()

    lang = identify_file_lang_csv(text)
    if lang != 'Unknown Language':
        nlp = language_models[lang]
    else:
        print("Невозможно распознать язык в файле!")
        return
    count = 0
    for column in dataframe.columns:
        count += 1
        record_types = dataframe[column].apply(identify_record_type, args=(nlp, lang))
        type_counts = record_types.value_counts(normalize=True) * 100
        total_type = type_counts.idxmax()
        report[column] = {
            'percentages': type_counts.to_dict(),
            'final_type': total_type
        }
    return report


def detect_delimiter(file_path):
    possible_delimiters = [',', ';', '\t', ':', '|', ' ']
    delimiter_counter = Counter()
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        for line in file:
            for delimiter in possible_delimiters:
                if delimiter in line:
                    delimiter_counter[delimiter] += line.count(delimiter)
    if delimiter_counter:
        most_common_delimiter, _ = delimiter_counter.most_common(1)[0]
        return most_common_delimiter
    else:
        return None


def read_first_lines(file_path, num_lines=5):
    with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
        lines = [file.readline().strip() for _ in range(num_lines)]  # Читаем указанное количество строк
        text = " ".join(lines)
    return text


def FastAnalyseCSV(dataframe):
    column_names = dataframe.columns.tolist()
    for key in column_names:
        if key in head_names:
            return column_names, True
    return None, False


global delimiter


def read_part_csv(file_path, sep, n=10000):
    random.seed(42)
    sampled_rows = []
    chunksize = 100_000
    total_rows = 0
    for chunk in pd.read_csv(file_path, sep=sep, encoding="utf-8",
                             encoding_errors="replace", on_bad_lines="skip",
                             chunksize=chunksize):
        total_rows += len(chunk)
        if len(chunk) <= n:
            sampled_rows.append(chunk)
        else:
            sampled_rows.append(chunk.sample(n=min(n, len(chunk)), random_state=42))

    if total_rows <= n:
        return pd.concat(sampled_rows, ignore_index=True)
    else:
        combined = pd.concat(sampled_rows, ignore_index=True)
        return combined.sample(n=n, random_state=42).reset_index(drop=True)


    # # считаем количество строк (без заголовка)
    # with open(file_path, "r", encoding="utf-8", errors="replace") as f:
    #     total_lines = sum(1 for _ in f) - 1
    # if total_lines <= 0:
    #     return pd.DataFrame()  # пустой файл
    # if total_lines <= n:
    #     # строк меньше чем n → читаем весь файл
    #     df = pd.read_csv(
    #         file_path,
    #         sep=sep,
    #         encoding="utf-8",
    #         encoding_errors="replace",
    #         on_bad_lines="skip"
    #     )
    # else:
    #     # выбираем n случайных строк
    #     skip = sorted(random.sample(range(1, total_lines + 1), total_lines - n))
    #     df = pd.read_csv(
    #         file_path,
    #         sep=sep,
    #         encoding="utf-8",
    #         encoding_errors="replace",
    #         on_bad_lines="skip",
    #         skiprows=skip
    #     )
    # return df


def analyze_file(file_path):
    global delimiter
    language_models = {
        'en': spacy.load("en_core_web_sm"),
        'ru': spacy.load("ru_core_news_sm"),
        'uk': spacy.load("uk_core_news_sm"),  # Украина
        'zh': spacy.load("zh_core_web_sm"),  # Китай
    }
    if file_path.lower().endswith('.csv'):
        try:
            print("Определяем разделитель строк...")
            delimiter = detect_delimiter(file_path)
            print("Считываем файл...")
            dataframe = read_part_csv(file_path, delimiter)
            # dataframe = pd.read_csv(file_path, sep=delimiter, encoding='UTF-8', encoding_errors='replace',
            #                         on_bad_lines='skip')
        except Exception as e:
            print(e)
            try:
                dataframe = read_part_csv(file_path, ',')
                # dataframe = pd.read_csv(file_path, encoding='UTF-8', encoding_errors='replace',
                #                         on_bad_lines='skip')
            except Exception as e:
                print(e)
                return None, None, None, None
        total_record = dataframe.shape[0]
        max_keys = dataframe.shape[1]
        column_names, check = FastAnalyseCSV(dataframe)
        if check:
            return total_record, check, column_names, max_keys
        report = analyze_data(dataframe, language_models)
        return total_record, report, None, max_keys
    elif file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.xls'):
        try:
            dataframe = pd.read_excel(file_path)
        except Exception as e:
            print(e)
            return None, None, None, None
        total_record = dataframe.shape[0]
        max_keys = dataframe.shape[1]
        column_names, check = FastAnalyseCSV(dataframe)
        if check:
            return total_record, check, column_names, max_keys
        report = analyze_data(dataframe, language_models)
        return total_record, report, None, max_keys
    elif file_path.lower().endswith('.txt'):
        record_types = []
        delimiter = detect_delimiter(file_path)
        head = read_first_lines(file_path, num_lines=5)
        lang = identify_file_lang(head)
        if lang != 'Unknown Language':
            nlp = language_models[lang]
        else:
            print("Невозможно распознать язык в файле!")
            return None, None, None, None

        count = 0
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            for line in file:
                count += 1
            len_file = count
        count_columns = {}
        mass_columns = []
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\nВыполняется анализ файла {os.path.basename(file_path)}...\t\t Начало обработки: {current_time}\n")
            count = 0
            persent = 0
            for line in file:
                count += 1
                records = line.strip().replace("\n", "").split(delimiter)
                mass_columns.append(len(records))
                if len(records) not in count_columns.keys():
                    count_columns[
                        len(records)] = []
                for record in records:
                    record_type = identify_record_type(record, nlp, lang)
                    count_columns[len(records)].append(
                        record_type)
                if (count / len_file * 100) >= persent + 5:
                    persent += 5
                    print(f"Завершено {persent}%")

        total_records = count
        report_for_columns = {}
        max_key = 1
        clusters_list = [(cluster_id, file_names) for cluster_id, file_names in count_columns.items()]
        sorted_clusters = dict(sorted(clusters_list, key=lambda x: x[0]))
        for key, items in sorted_clusters.items():
            if max_key < key:
                max_key = key
            type_counts = Counter(items)
            report = {key: (value / sum(type_counts.values()) * 100) for key, value in type_counts.items()}
            report_for_columns[key] = report

        columns_counts = Counter(mass_columns)
        report_columns_count = {key: (value / total_records * 100) for key, value in columns_counts.items()}
        report_columns_count = dict(sorted(report_columns_count.items(), key=lambda x: x[1], reverse=True))
        return total_records, report_for_columns, report_columns_count, max_key
    else:
        print("Еще не придуман разбор состава файлов с расширениями не txt, xls, xlsx и csv")
        return None, None, None, None


def generate_fast_report(file_name, total_records, column_names, max_key):
    string_column_names = ", ".join(column_names)
    filename = f"\nФайл: {file_name}"
    max_cloumns = f"Максимальное количество столбцов: {max_key}"
    max_records = f"Строк в файле: {total_records}"
    all_column_names = f"Названия столбцов: {string_column_names}"
    is_mail_pass_file = False
    if len(column_names) == 2 and ('email' == x.lower() for x in column_names):
        is_mail_pass_file = True
    report = [filename, max_cloumns, max_records, all_column_names]
    string_report = "\n".join(report)
    return string_report, is_mail_pass_file


def generate_report(file_name, total_records, type_distribution, report_columns_count, max_key):
    is_mail_pass_file = False
    filename = f"\nФайл: {file_name}"
    max_cloumns = f"Максимальное количество столбцов: {max_key}"
    max_records = f"Строк в файле: {total_records}"
    need_percentage = 0
    need_key = None
    if file_name.lower().endswith('.txt'):
        mass_reports = []
        for count_columns, percentage in report_columns_count.items():
            if percentage > need_percentage:
                need_percentage = percentage

        if need_percentage == 0:
            print("Не удалось определить столбцы в файле!")
            return None, False

        for count_columns, percentage in report_columns_count.items():
            if percentage == need_percentage:
                summary_percentage = f"{percentage:.2f}% файла состоит из {count_columns} столбцов"
                mass_reports.append(summary_percentage)
                need_key = count_columns

        # title = "\nРезультат для части файла с определенным количеством столбцов:"
        # print("\nРезультат для части файла с определенным количеством столбцов:")
        # mass_reports.append(title)

        for key, report in type_distribution.items():
            if key != need_key:
                continue
            tmp_report = []
            # sub_title = f"\nКоличество столбцов: {key}"
            # tmp_report.append(sub_title)
            # print(f"\nКоличество столбцов: {key}")
            for record_type, percentage in report.items():
                if percentage > 5:
                    records_title = f"{percentage:.2f}% части файла состоит из {record_type}"
                    tmp_report.append(records_title)
                    if need_key == 2 and record_type == 'Email' and percentage > 30:
                        is_mail_pass_file = True
            unknown_percentage = 100 - sum(report.values())
            unknown_title = f"{unknown_percentage:.2f}% части файла не определено"
            tmp_report.append(unknown_title)
            tmp_report_join = "\n".join(tmp_report)
            mass_reports.append(tmp_report_join)
        string_mass_report = "\n".join(mass_reports)
        report = [filename, max_cloumns, max_records, string_mass_report]
        string_report = "\n".join(report)
        return string_report, is_mail_pass_file
    else:
        possible_column_names = []
        possible_types = []
        for record_type, percentage in type_distribution.items():
            if 'Column' not in record_type:
                possible_column_names.append(record_type)
            if percentage['final_type'] != 'Unknown':
                possible_types.append(percentage['final_type'])
        if len(possible_column_names) == 2 and 'Email' in possible_types:
            is_mail_pass_file = True
        possible_column_names = ', '.join(set(possible_column_names))
        possible_types = ', '.join(set(possible_types))

        all_column_names = f"Возможные названия столбцов: {possible_column_names}"
        all_types = f"Возможные типы информации: {possible_types}"
        report = [filename, max_cloumns, max_records, all_column_names, all_types]
        string_report = "\n".join(report)
        return string_report, is_mail_pass_file
        # print(f"Возможные названия столбцов: {possible_column_names}")
        # print(f"Возможные типы информации: {possible_types}")


def RunFileAnalyse(file_path):
    file_name = os.path.basename(file_path)
    total_records, type_distribution, report_columns_count, max_key = analyze_file(file_path)
    if type_distribution is True:
        column_names = report_columns_count
        report, is_mail_pass_file = generate_fast_report(file_name, total_records, column_names, max_key)
        return report, is_mail_pass_file
    elif type_distribution is not None:
        report, is_mail_pass_file = generate_report(file_name, total_records, type_distribution, report_columns_count,
                                                    max_key)
        return report, is_mail_pass_file
    else:
        print()
        return None, None


def extract_country_from_filename(filename):
    for country_code, country_names in country_mapping.items():
        for country_name in country_names:
            if country_name.lower() in filename.lower():
                return country_name.capitalize()

    country_pattern = re.compile(r'.*(\b[A-Za-z]{2}\b).*', re.IGNORECASE)
    match = country_pattern.search(filename)
    if match:
        checkword = match.group(1)
        country_code = checkword.lower()
        return country_mapping.get(country_code, [None])[0]
    return None


# Функция для извлечения кода страны из домена
def get_country_code_from_email(email):
    domain_parts = email.split('@')[-1].split('.')
    if len(domain_parts) > 1:
        return domain_parts[-1].lower()
    return None


def ExtractDataframe(file_path):
    try:
        if file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.xls'):
            dataframe = pd.read_excel(file_path)
            return dataframe
        elif file_path.lower().endswith('.csv'):
            delimeter = detect_delimiter(file_path)
            dataframe = read_part_csv(file_path, delimeter)
            # dataframe = pd.read_csv(file_path, delimiter=delimeter, encoding='utf-8', encoding_errors='replace')
            return dataframe
        elif file_path.lower().endswith('.txt'):
            delimeter = detect_delimiter(file_path)
            data = []
            with open(file_path, encoding='utf-8', errors='replace') as file:
                for line in file:
                    parts = line.split(delimeter)
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip()
                        data.append({'email': key, 'password': value})
            dataframe = pd.DataFrame(data)
            return dataframe
    except Exception as e:
        print(e)
        return None


# Функция для обработки файла
def NormalizeAndCreateMailPassFile(file_path, normalized_folder):
    print(f"Обработка файла {os.path.basename(file_path)}...")
    if file_path.lower().endswith('.csv') or file_path.lower().endswith('.xlsx') or file_path.lower().endswith('.txt'):
        try:
            dataframe = ExtractDataframe(file_path)
            if dataframe.empty:
                return None
            if dataframe.shape[1] == 2:
                dataframe.iloc[:, 0] = dataframe.iloc[:, 0].apply(lambda x: x.lower() if isinstance(x, str) else x)
                dataframe = dataframe[dataframe.iloc[:, 1].apply(lambda x: 4 <= len(str(x)) <= 40)]
                if dataframe.empty:
                    print(f"В файле {os.path.basename(file_path)} нет строк, удовлетворяющих условиям!")
                    return None
                country = extract_country_from_filename(file_path)
                dataframe['country'] = country if country else None

                def set_country_based_on_email(row):
                    email = row.iloc[0]
                    country_code = get_country_code_from_email(email)
                    if country_code and country_code in country_mapping:
                        return country_mapping[country_code][0]
                    return row['country']

                dataframe['country'] = dataframe.apply(set_country_based_on_email, axis=1)
                dataframe = dataframe.drop_duplicates()
                try:
                    random_samples = dataframe.sample(10)
                    max_length_email = max(random_samples['email'].apply(len))
                    print(
                        f'Пример обработанных данных (10 случайных строк):'
                        f' {"Email":<{max_length_email + 10}}{"Password":<40}{"Country":<40}')
                    for index, row in random_samples.iterrows():
                        a = None if not row['country'] else f"{row['country']:<30}"
                        print("\t" * 12, f"{row.iloc[0]:<{max_length_email + 10}}{row.iloc[1]:<40}{a}")
                except Exception as e:
                    print(e)
                    return None
                data_to_insert = dataframe.rename(
                    columns={dataframe.columns[0]: "email", dataframe.columns[1]: "password"})
                if not os.path.exists(normalized_folder):
                    os.makedirs(normalized_folder, exist_ok=True)
                normalized_filepath = os.path.join(normalized_folder, os.path.basename(file_path))
                base_name, ext = os.path.splitext(os.path.basename(file_path))
                i = 1
                while os.path.exists(normalized_filepath):
                    normalized_filepath = os.path.join(normalized_folder, f"{base_name} {i}{ext}")
                    i += 1
                if ext.lower() == '.txt':
                    data_to_insert.to_csv(normalized_filepath, index=False,
                                          sep='\t')
                else:
                    data_to_insert.to_csv(normalized_filepath, index=False)
                print(f"Данные успешно добавлены в файл")
                return normalized_filepath
                # records = data_to_insert.to_dict(orient='records')
                # collection.insert_many(records)
                # print("Данные успешно добавлены в коллекцию MongoDB.")
            else:
                print("Файл должен содержать два столбца (email и password).")
                print("Сейчас данные следующего вида: ")
                print(dataframe.head())
                return None
        except Exception as e:
            print(f"Ошибка при обработке файла: {e}")
            return None
    else:
        print("Поддерживаются только файлы форматов .csv, .xlsx, .xls или .txt")
        return None

# def MailPassWorker(filepath):
#     global delimiter
#     if filepath.endswith('.csv') or filepath.endswith('.CSV'):
#         try:
#             dataframe = pd.read_csv(filepath, encoding='UTF-8', encoding_errors='replace')
#         except Exception as e:
#             print(e)
#             return None, None, None, None
#         total_record = dataframe.shape[0]
#         max_keys = dataframe.shape[1]
#
#     elif filepath.endswith('.xlsx') or filepath.endswith('.xls') or filepath.endswith('.XLS') or filepath.endswith(
#             '.XLSX'):
#         try:
#             dataframe = pd.read_excel(filepath)
#         except Exception as e:
#             print(e)
#             return None, None, None, None
#         total_record = dataframe.shape[0]
#         max_keys = dataframe.shape[1]
#
#     elif filepath.endswith('.txt') or filepath.endswith('.TXT'):
#         with open(filepath, encoding='utf-8', errors='replace') as file:
#             for line in file:
#                 if len(line.split(delimiter)) == 2:


# if __name__ == "__main__":
#     file_path = r'C:\Users\and23\Downloads\Telegram Desktop\4K BELUIUM SUPER GOOD @LORDALI01.csv'  # Пример пути к файлу
#     file_report = RunFileAnalyse(file_path)
#     print(file_report)
