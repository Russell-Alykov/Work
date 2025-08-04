import pandas as pd
import json
import requests
import os
import time
import gzip
import io
import base64
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine
# Считывание .env
from dotenv import load_dotenv

# Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# # Google
# import gspread
# from df2gspread import df2gspread as d2g # заливает df в docs
# from oauth2client.service_account import ServiceAccountCredentials # Авторизация

# Чтение переменных среды
load_dotenv('/Users/ra/WORK_sunshine/scripts_local/product_resluts_report/.env')

# Amazon
spreadsheet_id = os.getenv('SPREADSHEET_ID')
endpoint_report = os.getenv('ENDPOINT_GENERATE_REPORT')
account_id = os.getenv('ACCOUNT_ID')
client_id = os.getenv('AMZ_CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
refresh_token = os.getenv('REFRESH_TOKEN')
sub_days = os.getenv('SUB_DAYS')
# SQL
base_db = os.getenv('BASE_DB')
ip = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER_DB')
password = os.getenv('PASS')

# Получение токена
def get_access_token(refresh_token, client_id, client_secret: str):
    '''
    Функция полуения токена с Amazon api
    '''
    # .env data parse
    data = {
    'grant_type': 'refresh_token', 
    'refresh_token': refresh_token,
    'client_id': client_id,
        'client_secret': client_secret
    }

    # Post звпрос
    url = "https://api.amazon.com/auth/o2/token"
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, data=data, headers=headers)

    # Проверка статуса
    if response.status_code != 200:
        return "", f"unexpected status: {response.status_code} - {response.text}"

    # Parse JSON ответ и запись токена
    token_response = response.json()
    access_token = token_response.get('access_token', '')
    return access_token, None

token = get_access_token(refresh_token, client_id, client_secret)
token = token[0]

def add_headers(request, token, client_id, account_id):
    '''
    Функция формирующая заголовки для запроса отчета
    '''
    request.headers['Content-Type'] = 'application/vnd.createasyncreportrequest.v3+json'
    request.headers['Amazon-Advertising-API-ClientId'] = client_id
    request.headers['Amazon-Advertising-API-Scope'] = account_id
    request.headers['Authorization'] = 'Bearer ' + token

def generate_report(url, api_key, client_id, account_id, sub_days: int):
    '''
    Функция делает запрос на отчет в Amazon API
    И возвращает report_id
    Конфигурация запроса в Payload
    '''
    method = "POST"
    
    # Генерация даты отчета
    name = "report-" + datetime.now().strftime("%Y-%m-%d")
    print("Name:", name)

    # Генерация начальной и конечной даты
    end_date = datetime.now().strftime("%Y-%m-%d")
    days = int(sub_days)
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    print("Start Date:", start_date)
    print("End Date:", end_date)

    # Payload
    payload = {
        "name": name,
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["advertiser"],
            "columns": [
            "campaignId",
			"campaignName",
            "advertisedSku",
            "advertisedAsin",
            "impressions",
            "clicks",
            "spend",
            "purchases30d",
            "sales30d",
            "costPerClick",
            "date"],
            "reportTypeId": "spAdvertisedProduct",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }
    }

    # Заголовки
    headers = {
        'Content-Type': 'application/json',
        'Amazon-Advertising-API-ClientId': client_id,
        'Amazon-Advertising-API-Scope': account_id,
        'Authorization': 'Bearer ' + api_key
    }
    # Запрос
    response = requests.post(url, json=payload, headers=headers)

    # Проверка статуса ответа
    if response.status_code != 200:
        return "", f"unexpected status: {response.status_code} - {response.text}"

    # Parse JSON ответ и запись report _id
    report_response = response.json()
    report_id = report_response.get('reportId', '')

    return report_id, None

# Вызов функции генерации отчета
report_id, error_message = generate_report(endpoint_report, token, client_id, account_id, sub_days)
if error_message:
    print("Error generating report:", error_message)
    # Если ошибка
else:
    print("Report ID:", report_id)

# Получение ссылки на отчет
def get_download_link(report_url, token, client_id, account_id):
    '''
    Функция получения ссылки на отчет
    '''
    method = "GET"
    client = requests.Session() # Новый объект сеанса
    # try:

    # Бесконечный цикл для получения ссылки пока она не станет доступна с тайм аутом в 5 минут
    while True:
        req = requests.Request(method, report_url) # Новый объект запроса
        prepared = req.prepare() # Объект подготовленного запроса
        
        # Добавляем заголовки к подготовленному (prepare) объекту запроса
        add_headers(prepared, token, client_id, account_id)
        resp = client.send(prepared)
        # Код состояния ответа
        print("Response Status:", resp.status_code)

        # Если код не равен 200, то возвращаем "непредвиденный"
        if resp.status_code != 200:
            return "", f"unexpected status code: {resp.status_code}"
        # Если = 200 то записываем json dump
        report_response = resp.json()
        # Получаем значние поля url из ответа с дампом
        res_url = report_response.get("url")
        # Если ссылки еще нет, то пробуем пока не получим 
        if res_url is None:
            print("'res_url' field is not a string")
            print("Retrying in 5 minutes...")
            time.sleep(300)  # Новая попытка через 5 минут
            continue

        return res_url, None

def add_headers(request, token, client_id, account_id):
    ''' 
    Функция формирует заголовки для получения ссылки на отчет
    '''
    request.headers["Amazon-Advertising-API-ClientId"] = client_id
    request.headers["Amazon-Advertising-API-Scope"] = account_id
    request.headers["Authorization"] = "Bearer " + token

# Вызов функции получения ссылки
download_link, error = get_download_link(endpoint_report+report_id, token, client_id, account_id)
if error:
    print("Error getting download link:", error)
else:
    print("Download Link:", download_link)

# Загрузка отчета
def download_report(report_url):
    '''
    Функция загрузки отчета в df
    report_url - получать с функции get_download_link
    '''
    method = "GET"
    client = requests.Session() # Объект сеанса

    req = requests.Request(method, report_url) # Объект запроса
    prepared = req.prepare() # Подготовленный объект запроса
    while True:
        try:
            resp = client.send(prepared)  # Отправка запроса
            break  # Выход с цикла при успехе
        except ConnectionError:
            time.sleep(60)
            continue
    # Код статуса
    print("Response Status:", resp.status_code)
    # Если не равен 200, то вернуть "неожиданный"
    if resp.status_code != 200:
        return None, f"unexpected status code: {resp.status_code}"
    # Если = 200 то загружаем контент с ответа в память
    data = resp.content 

    try:
        # Распаковывается содержимое ответа с использованием Gzip-декомпрессии.
        decompressed_data = gzip.decompress(data)
    except gzip.BadGzipFile:
        print("Error: Response body is not in gzip format")
        return None, "response body is not in gzip format"
    except Exception as e:
        print("Error decompressing data:", e)
        return None, f"error decompressing data: {e}"

    try:
        # Разбирается распакованное содержимое ответа как JSON
        json_data = json.loads(decompressed_data)
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
        return None, f"error decoding JSON: {e}"
    # Запись в df
    df = pd.DataFrame(json_data)
    df = df.sort_values('date', ascending=False)

    return df, None

# Вызов функции загрузки отчета
report_data, error = download_report(download_link)
if error:
    print("Error downloading report:", error)
else:
    print("Report Data:", report_data)
report_data['Платформа'] = 'Amazon'

#########################################################
# Прверка Amazon
asin_lst = report_data['advertisedAsin'].unique().tolist()
date_lst = report_data['date'].unique().tolist()
engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}')
db_connection = engine.connect()
id_check_list = ','.join("'" + str(asin) + "'" for asin in asin_lst if asin is not None and asin != '' and asin != ' ')
# id_check_list = id_check_list.replace({' ': ','})
date_check_list = ','.join("'" + str(date) + "'" for date in date_lst if date is not None and date != '' and date != ' ')
query = f'DELETE FROM dwh.amz_ads_rnp WHERE "advertisedAsin" IN ({id_check_list}) AND "date" IN ({date_check_list})'
engine.execute(query)
db_connection.close()  

# Запись
engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}')
db_connection = engine.connect()
report_data.to_sql('amz_ads_rnp', schema = 'dwh', con=db_connection, if_exists='append', index = False)
db_connection.close()      
print('Succsess')