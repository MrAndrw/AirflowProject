import time
import logging
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver as wd
import aiohttp
from aiohttp import TCPConnector
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

logging.basicConfig(level=logging.INFO)


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


async def TakeSrcWithWD(name, st_useragent):
    options = Options()  # airflow
    options.add_argument(f"user-agent={st_useragent}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--headless")  # Запуск без GUI airflow
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")  # Для использования в контейнерах airflow
    options.add_argument("--disable-dev-shm-usage")  # Для использования в контейнерах (если нужно) airflow
    service = Service(ChromeDriverManager().install())  # airflow
    with wd.Chrome(service=service, options=options) as driver:  # airflow
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
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
    # "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; AS; ru-RU) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36 Edge/12.0",

    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.864.64 Safari/537.36 Edge/91.0.864.64",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
    # "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/13.1.2 Safari/537.36",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 OPR/77.0.4054.90",
    # "Mozilla/5.0 (iPhone; CPU iPhone OS 14_2 like Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/537.36",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Brave/1.30.89",
    # "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 YaBrowser/20.11.4.197 Yowser/2.5 Safari/537.36",
    # "Mozilla/5.0 (Linux; Android 9; Pixel 3 XL Build/PQ2A.190305.003) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.136 Mobile Safari/537.36",
    # "Mozilla/5.0 (Linux; Android 10; Pixel 3 Build/QP1A.190711.020) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.185 Mobile Safari/537.36 Edg/86.0.622.69",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 Vivaldi/4.0",
    # "Mozilla/5.0 (Linux; Android 10; SM-G965F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.136 Mobile Safari/537.36 SamsungBrowser/12.1",
    # "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:78.0) Gecko/20100101 Firefox/78.0"
]


async def TakeSrc(tmp, newname, list_of_agents):
    print(f"Поиск информации в интернете по сообщению {newname}")
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
