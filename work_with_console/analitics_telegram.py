import asyncio
import os
import time
import logging
from googlesearch import search
from bs4 import BeautifulSoup
import requests
from folders_for_analitics import *
import pandas as pd
from selenium import webdriver as wd
from urllib.parse import quote
from googlesearch import search
import asyncio
from selenium import webdriver as wd
import aiohttp
from aiohttp import TCPConnector
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from set_parameters import session_telegram, chrome_path


logging.basicConfig(level=logging.INFO)
start_time = time.time()
os.chdir(session_telegram)
massive_id = [
    2117186516,  #
    1781536189,  #
    1467443345,  #
    1345749946,  #
    1648715652,  #
    1403554887,  #
    1653475378,  #
    1704295282,  #
    1739898096,  #
    1582904494,  #
    1846442443,  #
    1888082481,  #
    1343256330,  #
    1670433615,  #
    1610892951,  #
    1312857635,  #
    1845261470,  #
    1647734410,  #
    1555377160,  #
    1616014329,  #
    1938105511,  #
    1432413995,  #
    1217574794,  #
    1691718153,  #
    2096281014,  #
    1647755561,  #
    1437080114,  #
    1452559011,  #
    1534246875,  #
    1692599650,  #
    1752888793,  #
    1542348593,  #
    1799356697,  #
    1879888864,  #
    1951766516,  #
    1406048795,  #
    1639852967,  #
    1416023308,  #
    1740299468,  #
    1204905393,  #
    1152933233,  #
    2238919888,  #
    2249063566,  #
    2003618439,  #
    1419760380,  #
    1387792617,  #
    1519986655,  #
    777000,  #
    2115142410,  #
    1685534381,  #
    1679221275,  #
    1696854185,  #
    1657415895,  #
    1756576503,  #
    1763436525,  #
    2048774895,  #
    1615707786,  #
    1894315236,  #
    1958435614,  #
    1591498225,  #
    1391702868,  #
    1987908216,  #
    1253096988,  #
    1737527143,  #
    1300294187,  #
    1840252495,  #
    2222520689,
    2037059849,
    1737441054,
    1947451780,
    1988659913,
    1685534381
]


async def fetch_summary(url):
    text = ''
    connector = TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=30)
    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.get(url) as response:
                soup = BeautifulSoup(await response.text(), 'lxml')
                try:
                    summary = soup.find('div', {'class': 'mw-parser-output'}).find_all('p')[0].text
                    return summary
                except Exception as e:
                    try:
                        summary = soup.find('div', {'class': 'article__content'}).find_all('p')[0].text
                        return summary
                    except Exception as e:
                        try:
                            paragraphs = soup.find_all('p', limit=2)
                            return '\n'.join([p.text for p in paragraphs]) if paragraphs else None
                        except Exception as e:
                            print(e)
                            return None
    except Exception as e:
        print(f"Error fetching summary: {e}")
        return None


# Рабочая функция для Гугл Хрома
async def TakeSrcWithWD(name, st_useragent):
    options = Options()
    options.binary_location = chrome_path
    options.add_argument(f"user-agent={st_useragent}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    with wd.Chrome(service=service, options=options) as driver:
        driver.get(f"https://yandex.ru/search/?text={name}")
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, "serp-item")))
        except Exception as e:
            print("Вероятно наткнулись на капчу при запросе в интернете")
            print(f"Error waiting for the page to load: {e}")
            return None
        src = driver.page_source
    return src




keywords = ["Обновите", "обновите", "ВКонтакте", "В Контакте", "В контакте", "Вконтакте", "вконтакте", "Ютуб", "ютуб",
            "YouTube", "youtube", "Устарел", "устарел"]

list_of_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Yandex/21.10.0.1257 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; AS; ru-RU) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36 Edge/12.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.864.64 Safari/537.36 Edge/91.0.864.64",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/13.1.2 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 OPR/77.0.4054.90",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Brave/1.30.89",
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 YaBrowser/20.11.4.197 Yowser/2.5 Safari/537.36",
    "Mozilla/5.0 (Linux; Android 9; Pixel 3 XL Build/PQ2A.190305.003) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.136 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; Pixel 3 Build/QP1A.190711.020) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.185 Mobile Safari/537.36 Edg/86.0.622.69",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Vivaldi/4.0",
    "Mozilla/5.0 (Linux; Android 10; SM-G965F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.136 Mobile Safari/537.36 SamsungBrowser/12.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"
]


async def TakeSrc(tmp, newname, list_of_agents):
    src = await TakeSrcWithWD(newname, list_of_agents[tmp])
    if src is None and tmp != len(list_of_agents) - 1:
        print("Повтор процесса для нового user-agent")
        tmp += 1
        time.sleep(5)
        return await TakeSrc(tmp, newname, list_of_agents)
    return src if src else '-'


async def GetMetaInfoInInternet(name):
    newname = "Утечка " + name.rsplit(".", 1)[0].replace(".", "_")
    all_summary = ''
    src = await TakeSrc(0, newname, list_of_agents)
    if src == '-':
        return src
    soup = BeautifulSoup(src, 'lxml')
    links = []
    for item in soup.find_all('li', {'class': 'serp-item'}, limit=2):
        link_tag = item.find('a', {'class': 'link'})
        if link_tag and link_tag.has_attr('href'):
            link = link_tag['href']
            links.append(link)
    for link in links:
        summary = await fetch_summary(link)
        if summary:
            if (not any(key in summary for key in keywords)) and (name.rsplit(".", 1)[0] in summary):
                all_summary = all_summary + f"Информация от {link}:" + '\n' + summary + '\n' + "=" * 80 + '\n'
    return all_summary if all_summary else '-'


def Confirm(message):
    return False
    # ch = input(
    #     f"\nДля подтверждения информации о сообщении {message.file.name} нажмите 1\n"
    #     f"Чтобы отменить добавление информации, нажмите 0\n"
    #     f"Ввод: "
    # )
    # if ch == "1":
    #     return True
    # elif ch == "0":
    #     return False
    # else:
    #     print("Введите номер пункта по условию!\n")
    #     return Confirm(message)


async def TakeVolumeAndCountFiles(client, channel_id, unread):
    count1 = 0
    volume1 = 0
    sizes1 = []
    async for message in client.iter_messages(channel_id, limit=unread):
        if message.media or message.file:
            try:
                if message.file.name is not None:
                    sizes1.append(message.media.document.size)
                    count1 += 1
            except Exception:
                continue
    for i in range(len(sizes1)):
        volume1 += sizes1[i]
    volume1 = str(round(volume1 / 1024 / 1024, 5)).replace(".", ",")
    if volume1 == "0,0":
        volume1 = "0"
    return volume1, count1


async def AnaliticMessages(client, day):
    value_mis = 67
    copy_massive_id = massive_id.copy()
    line1 = [
        "",
        "Название канала",
        "id",
        "Непрочитанные сообщения",
        "Cообщения c файлами",
        "Общий объем",
    ]
    line1_df = pd.DataFrame(columns=line1)
    with pd.ExcelWriter(f"{analitic_folders.get('work_folder')}\otchet {day}.xlsx") as writer:
        line1_df.to_excel(writer, index=False)

    dialogs = await client.get_dialogs()  # все каналы

    channel_names_line = {}
    first_line = []
    for dialog in dialogs:
        name = dialog.name
        channel_id = dialog.entity.id
        for ch_id in massive_id:
            if ch_id == channel_id:
                channel_names_line[massive_id.index(channel_id)] = str(name)

    for i in range(len(massive_id)):
        try:
            if channel_names_line[i]:
                continue
            else:
                channel_names_line[i] = f"Не доступен канал с id {massive_id[i]}"
        except Exception as e:
            channel_names_line[i] = f"Не доступен канал с id {massive_id[i]}"
    for index in sorted(channel_names_line):
        for i in range(3):
            first_line.append(channel_names_line[index])
    first_line_df = pd.DataFrame(columns=first_line)
    try:
        with pd.ExcelWriter(f"{analitic_folders.get('work_folder')}\otchet_messages {day}.xlsx") as writer:
            first_line_df.to_excel(writer, index=False)
    except Exception as e:
        print(e)
        exit(0)

    print(
        f"\nНазвание канала \t Непрочитанные сообщения \t Файлы в сообщениях \t Общий объем"
    )
    c = 0
    df = []
    counts = []
    needstring = []
    for dialog in dialogs:
        c += 1
        try:
            mass_mess_from_df = []
            count = 0

            message_names = []
            message_text = []
            sizes = []
            volume = 0
            name = dialog.name
            channel_id = dialog.entity.id
            unread = dialog.unread_count

            if channel_id == 2117186516:
                f = open(rf"{analitic_folders.get('work_folder')}\take.txt", "r")
                cur = f.readline()
                f.close()
                tmp = str(unread)
                f = open(rf"{analitic_folders.get('work_folder')}\take.txt", "w")
                f.write(tmp)
                f.close()
                unread = unread - int(cur)
            if unread == 0:
                mass_mess_from_df.append(["-", "-", "-"])

            for index in range(len(massive_id)):
                if channel_id == massive_id[index]:
                    count_files = await TakeVolumeAndCountFiles(client, channel_id, unread)
                    count_files_1 = count_files[1]
                    count_files_0 = count_files[0]
                    print(
                        f"\n{name}"
                        f" \n{channel_id} \t\t\t\t {unread} \t\t\t\t {count_files_1} \t\t\t\t {count_files_0}"
                    )

            async for message in client.iter_messages(channel_id, limit=unread):
                if message.media or message.file:
                    try:
                        if message.file.name is None:
                            mass_mess_from_df.append(["-", "-", "-"])
                        else:
                            df_mes = []
                            message_names.append(message.file.name)
                            df_mes.append(message.file.name)
                            sizes.append(message.media.document.size)
                            size_in_mb = str(
                                round(message.media.document.size / 1024 / 1024, 5)
                            ).replace(".", ",")
                            if size_in_mb == "0,0":
                                size_in_mb = "0"
                            df_mes.append(size_in_mb)
                            try:
                                if message.text != "":
                                    message_text.append(message.text)
                                    df_mes.append(message_text)
                                    print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                                elif message.text == "" or " " or "\t" or "\n":
                                    try:
                                        meta_info = await GetMetaInfoInInternet(message.file.name)
                                        if meta_info == "-":
                                            message_text.append("-")
                                            df_mes.append("-")
                                            print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                                        else:
                                            # print(
                                            #     f"\nИнформация из интернета для сообщения {message.file.name} из канала {name}:"
                                            # )
                                            # print(meta_info)
                                            if Confirm(message):
                                                message_text.append(
                                                    meta_info
                                                )
                                                df_mes.append(
                                                    meta_info
                                                )
                                                print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                                            else:
                                                message_text.append("-")
                                                df_mes.append("-")
                                                print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                                    except Exception:
                                        print(
                                            f"Ошибка при попытке получить метаинформацию для сообщения {message.file.name}!"
                                        )
                                        message_text.append("-")
                                        df_mes.append("-")
                                        time.sleep(2)
                                        print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                                else:
                                    print(
                                        f"Проверьте сообщение {message.file.name} вручную."
                                    )
                                    message_text.append("?")
                                    df_mes.append("?")
                                    time.sleep(2)
                                    print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                            except Exception as e:
                                print(
                                    f"Не удается получить доступ к тексту сообщения {message.file.name}."
                                    f"{e}"
                                )
                                message_text.append("-")
                                df_mes.append("-")
                                time.sleep(2)
                                print(f"обработано сообщений: {count + 1} из {count_files_1}...")
                            count += 1
                            mass_mess_from_df.append(df_mes)
                    except Exception:
                        mass_mess_from_df.append(["-", "-", "-"])
            counts.append(count)

            for i in range(len(copy_massive_id)):
                if copy_massive_id[i] == channel_id:
                    copy_massive_id[i] = mass_mess_from_df
            for i in range(len(message_names)):
                volume += sizes[i]
            volume = str(round(volume / 1024 / 1024, 5)).replace(".", ",")
            if volume == "0,0":
                volume = "0"

            for index in range(len(massive_id)):
                if channel_id == massive_id[index]:
                    line2 = [index + 1, name, channel_id, unread, count, volume]
                    df.append(line2)
                    last = name
        except Exception:
            print(f"\nКанал после {last} недоступен к подключению!")
            mass_mess_from_df.append(["-", "-", "-"])
            time.sleep(2)

    print("\n Максимум файлов в сообщениях: ", max(counts))
    time.sleep(2)

    for i in range(len(copy_massive_id)):
        if copy_massive_id[i] in massive_id:
            copy_massive_id[i] = [
                [
                    "Этот канал недоступен или удален",
                    "Этот канал недоступен или удален",
                    "Этот канал недоступен или удален",
                ]
            ]
    for i in range(len(copy_massive_id)):
        while len(copy_massive_id[i]) < max(counts):
            copy_massive_id[i].append(["-", "-", "-"])
    for i in range(max(counts)):
        string = []
        for j in range(len(copy_massive_id)):
            string = string + copy_massive_id[j][i]
        needstring.append(string)

    df_needstring = pd.DataFrame(needstring)
    df_needstring.to_excel(
        rf"{analitic_folders.get('work_folder')}\otchet_messages {day}.xlsx",
        header=False,
        index=False,
        startrow=1,
    )

    sorted_df = sorted(df, key=lambda x: x[0])
    for i in range(len(massive_id) - 1):
        if sorted_df[i + 1][0] - sorted_df[i][0] == 2:
            sorted_df.insert(
                i + 1, [i + 2, "Этот канал удален или недоступен", "-", "-", "-", "-"]
            )
        elif sorted_df[i + 1][0] - sorted_df[i][0] > 2:
            for k in range(sorted_df[i + 1][0] - sorted_df[i][0] - 2):
                sorted_df.insert(
                    i + k + 1,
                    [
                        sorted_df[i][0] + 1 + k,
                        "Этот канал удален или недоступен",
                        "-",
                        "-",
                        "-",
                        "-",
                    ],
                )
    df_sorted = pd.DataFrame(sorted_df)
    df_sorted.to_excel(
        rf"{analitic_folders.get('work_folder')}\otchet {day}.xlsx", header=False, index=False, startrow=1
    )
    print(f"Просмотрено {c} каналов")
    return True


# def ConnectToTelegram():
#     print("\nПодключение к клиенту Telegram...")
#     try:
#         client = TelegramClient("session_name", api_id, api_hash)
#         client.start()
#         print("Подключено.")
#         return client
#     except Exception:
#         print("Не удается подключиться к клиенту Telegram")
#         return False


async def AnaliticWork(client):
    app = Analitic_App()
    print("Выберите или создайте необходимые папки для работы.")
    app.mainloop()
    day = input("Введите дату (дд.мм.гг)\n" "Ввод: ")
    # client = ConnectToTelegram()
    return await AnaliticMessages(client, day)


async def Start(client):
    status = await AnaliticWork(client)
    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    # loop.run_until_complete(AnaliticWork(client))
    # loop.close()
    return status
# if __name__ == "__main__":
#     app = Analitic_App()
#     print("Выберите или создайте необходимые папки для работы.")
#     app.mainloop()
#     day = input("Введите дату (дд.мм.гг)\n" "Ввод: ")
#     client = ConnectToTelegram()
#     AnaliticMessages(client, day)

# end_time = time.time()
# total_time = end_time - start_time
# print(f"Программа выполнилась за {total_time} секунд.")
