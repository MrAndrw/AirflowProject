FROM apache/airflow:2.10.5

ADD requirements.txt .


ENV PYTHONPATH=/opt/airflow/dags

USER root

RUN apt-get update && apt-get install -y wget gnupg2 \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable


RUN apt-get update && apt-get install -y p7zip-full
USER airflow

RUN pip install spacy
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download ru_core_news_sm
RUN python -m spacy download uk_core_news_sm
RUN python -m spacy download zh_core_web_sm

RUN pip install --no-cache-dir -r requirements.txt


#COPY dags/set_parameters.py /opt/airflow/dags/set_parameters.py
COPY ./dags/parameters.ini /opt/airflow/dags/parameters.ini
COPY ./dags/passwords /opt/airflow/dags/passwords
#RUN python3 /set_parameters.py
#
#CMD ["python3", "/set_parameters.py"]