# -*- coding: utf-8 -*-
#---------TITLE----------------
# tik_tok_stats
#---------DESCRIPTION----------
# Загружает отчет по спендам из TikTok API
# в таблицу SELECT * FROM public.tik_tok_stats
#------------------------------ 
import logging
import json
import os
import ast
import pandas as pd
import requests
import pendulum
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# ------------------------------------------------------------------------------
# Конфигурация окружения и логирования
# ------------------------------------------------------------------------------

# Загружаем переменные окружения из файла .env
load_dotenv('/opt/airflow/dags/creds_rr_alykov/.env')

# SQL-конфигурация
base_db = os.getenv('BASE_DB')
ip = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER_DB')
password = os.getenv('PASS')

# Другие переменные
app_id = os.getenv('app_id')
secret = os.getenv('secret')
access_token = os.getenv('access_token')
auth_df = pd.read_csv('/opt/airflow/dags/creds_rr_alykov/tokens_n_ids.csv')

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ------------------------------------------------------------------------------
# Функции для работы с API TikTok и обработки данных
# ------------------------------------------------------------------------------

def get_campaign_names(access_token, advertiser_id):
	"""
	Загружает все названия кампаний для указанного advertiser_id с учетом пагинации.
	"""
	url = "https://business-api.tiktok.com/open_api/v1.3/campaign/get/"
	headers = {
		"Access-Token": access_token,
		"Content-Type": "application/json"
	}
	campaign_map = {}
	page = 1
	page_size = 1000  # максимальное значение согласно документации
	while True:
		params = {
			"advertiser_id": str(advertiser_id),
			"page": page,
			"page_size": page_size
		}
		try:
			response = requests.get(url, headers=headers, params=params)
			response.raise_for_status()
			data = response.json()
			if data.get("code") != 0:
				logging.error(f"Ошибка TikTok API при получении кампаний: {data.get('message')}")
				break

			campaigns = data.get("data", {}).get("list", [])
			if not campaigns:
				break

			for c in campaigns:
				campaign_id = str(c.get("campaign_id"))
				campaign_name = c.get("campaign_name")
				campaign_map[campaign_id] = campaign_name

			# Если количество кампаний меньше лимита, значит страницы закончились
			if len(campaigns) < page_size:
				break

			page += 1
		except Exception as e:
			logging.error(f"Ошибка получения campaign_name: {e}")
			break

	return campaign_map

def get_basic_report_for_advertiser_v1_3(
	access_token,
	advertiser_id,
	start_date,
	end_date,
	dimensions,
	metrics,
	data_level,
	service_type="AUCTION",
	report_type="BASIC",
	page=1,
	page_size=1000,
	filtering=None,
	lifetime=False
):
	"""
	Делает запрос к /report/integrated/get/ для одного advertiser_id и возвращает JSON-ответ.
	"""
	url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
	headers = {
		"Access-Token": access_token,
		"Content-Type": "application/json"
	}
	params = {
		"advertiser_id": str(advertiser_id),
		"service_type": service_type,
		"report_type": report_type,
		"data_level": data_level,
		"dimensions": json.dumps(dimensions),
		"metrics": json.dumps(metrics),
		"start_date": start_date,
		"end_date": end_date,
		"page": page,
		"page_size": page_size,
		"query_lifetime": "true" if lifetime else "false"
	}
	if filtering:
		params["filtering"] = json.dumps(filtering)
	try:
		response = requests.get(url, headers=headers, params=params)
		response.raise_for_status()
		return response.json()
	except requests.exceptions.RequestException as e:
		logging.error(f"Ошибка запроса для advertiser_id {advertiser_id}: {e}")
		return {"code": -1, "message": f"Ошибка запроса: {e}"}
	except ValueError as e:
		logging.error(f"Ошибка декодирования JSON для advertiser_id {advertiser_id}: {e}")
		return {"code": -1, "message": f"Ошибка декодирования JSON: {e}"}

def get_all_basic_reports_from_df(
	auth_df,
	start_date,
	end_date,
	dimensions=None,
	metrics=None,
	data_level="AUCTION_CAMPAIGN",
	service_type="AUCTION",
	report_type="BASIC",
	page_size=1000,
	filtering=None,
	lifetime=False,
	add_campaign_names=False
):
	"""
	Извлекает отчёт по TikTok API для всех advertiser_id из auth_df с учетом пагинации.
	При необходимости добавляет campaign_name через отдельный запрос.
	После формирования DataFrame переупорядочивает столбцы:
	  - столбец advertiser_id становится вторым,
	  - столбец campaign_name — четвёртым.
	"""
	if dimensions is None:
		dimensions = ["advertiser_id", 'campaign_id', "stat_time_day"]

	if metrics is None:
		metrics = [
			"impressions", "spend", "clicks", "reach",
			"conversion", "cost_per_conversion", "conversion_rate", "conversion_rate_v2",
			"frequency", "result", "cost_per_result", "result_rate",
			"video_views_p100"
		]

	# Извлекаем данные авторизации
	row = auth_df.iloc[0]
	access_token_local = row["data.access_token"]
	advertiser_ids = row["data.advertiser_ids"]
	if isinstance(advertiser_ids, str):
		try:
			advertiser_ids = ast.literal_eval(advertiser_ids)
		except Exception as e:
			logging.error(f"Ошибка преобразования advertiser_ids: {e}")
			return pd.DataFrame()

	all_records = []

	for adv_id in advertiser_ids:
		current_page = 1
		while True:
			response_json = get_basic_report_for_advertiser_v1_3(
				access_token=access_token_local,
				advertiser_id=adv_id,
				start_date=start_date,
				end_date=end_date,
				dimensions=dimensions,
				metrics=metrics,
				data_level=data_level,
				service_type=service_type,
				report_type=report_type,
				page=current_page,
				page_size=page_size,
				filtering=filtering,
				lifetime=lifetime
			)

			if response_json.get("code") != 0:
				logging.error(f"Ошибка для advertiser_id {adv_id}: {response_json.get('message')}")
				break

			records = response_json.get("data", {}).get("list", [])
			if not records:
				break

			for rec in records:
				dims = rec.get("dimensions", {})
				mets = rec.get("metrics", {})
				for k, v in mets.items():
					if isinstance(v, str) and (v.isdigit() or v.replace('.', '', 1).isdigit()):
						mets[k] = float(v)
				combined = {**dims, **mets}
				combined["requested_advertiser_id"] = adv_id
				combined["source"] = "TikTok"
				all_records.append(combined)

			page_info = response_json.get("data", {}).get("page_info", {})
			if "total_page" in page_info:
				total_pages = page_info.get("total_page", 1)
				if current_page >= total_pages:
					break
			else:
				if len(records) < page_size:
					break
			current_page += 1

	df = pd.DataFrame(all_records)

	if add_campaign_names and "campaign_id" in df.columns:
		campaign_map = {}
		for adv_id in advertiser_ids:
			campaign_map.update(get_campaign_names(access_token_local, adv_id))
		df["campaign_id"] = df["campaign_id"].astype(str)
		df["campaign_name"] = df["campaign_id"].map(campaign_map)

	if "requested_advertiser_id" in df.columns:
		df["advertiser_id"] = df["requested_advertiser_id"]
		df.drop("requested_advertiser_id", axis=1, inplace=True)

	cols = list(df.columns)
	cols = [col for col in cols if col not in ("advertiser_id", "campaign_name")]
	new_order = []
	if len(cols) > 0:
		new_order.append(cols[0])
	else:
		new_order.append("advertiser_id")
	new_order.append("advertiser_id")
	if len(cols) > 1:
		new_order.append(cols[1])
	else:
		new_order.append("campaign_name")
	new_order.append("campaign_name")
	if len(cols) > 2:
		new_order.extend(cols[2:])
	
	for col in ("advertiser_id", "campaign_name"):
		if col not in df.columns:
			new_order.append(col)
	df = df[new_order]

	return df

# ------------------------------------------------------------------------------
# Функция для обновления таблицы в базе данных
# ------------------------------------------------------------------------------

def update_tik_tok_stats_table(df, engine, table_name='tik_tok_stats', schema='public'):
	"""
	Обновляет таблицу tik_tok_stats в Postgres на основе данных из df.
	Процесс:
	  1. Оставляет в df только записи за последние 2 недели (по полю stat_time_day).
	  2. Формирует список уникальных комбинаций (campaign_id, stat_time_day, spend) из отфильтрованных данных.
	  3. Удаляет из таблицы записи за выбранный период, у которых ключ (campaign_id, stat_time_day, spend) совпадает.
	  4. Заполняет столбец «id» в отфильтрованных данных новыми значениями, полученными на основе текущего максимального id в таблице.
	  5. Загружает отфильтрованные данные в таблицу (if_exists='append').
	"""
	df['stat_time_day'] = pd.to_datetime(df['stat_time_day'])
	today = datetime.today()
	two_weeks_ago = today - timedelta(days=7)  # измените на 14, если нужно именно 2 недели
	today_str = today.strftime('%Y-%m-%d')
	two_weeks_ago_str = two_weeks_ago.strftime('%Y-%m-%d')
	
	df_filtered = df[df['stat_time_day'] >= two_weeks_ago].copy()
	if df_filtered.empty:
		print("Нет записей за последние 2 недели; обновление не требуется.")
		return

	unique_keys = df_filtered[['campaign_id', 'stat_time_day', 'spend']].drop_duplicates()
	tuples_list = []
	for _, row in unique_keys.iterrows():
		campaign_id = row['campaign_id']
		stat_time_day = pd.to_datetime(row['stat_time_day']).strftime('%Y-%m-%d')
		spend = row['spend']
		tuples_list.append(f"('{campaign_id}', '{stat_time_day}', {spend})")
	keys_str = ", ".join(tuples_list)

	delete_query = f"""
	DELETE FROM {schema}.{table_name}
	WHERE stat_time_day BETWEEN '{two_weeks_ago_str}' AND '{today_str}'
	  AND (campaign_id, stat_time_day, spend) IN ({keys_str});
	"""
	with engine.begin() as connection:
		connection.execute(text(delete_query))
	
	with engine.begin() as connection:
		result = connection.execute(text(f"SELECT MAX(id) FROM {schema}.{table_name}"))
		max_id = result.scalar() or 0
	df_filtered['id'] = range(max_id + 1, max_id + 1 + len(df_filtered))
	
	cols = df_filtered.columns.tolist()
	if 'id' in cols:
		cols.remove('id')
		cols = ['id'] + cols
	df_filtered = df_filtered[cols]

	df_filtered.to_sql(
		name=table_name,
		con=engine,
		schema=schema,
		if_exists='append',
		index=False
	)

	print(f"Таблица {schema}.{table_name} успешно обновлена за период с {two_weeks_ago_str} по {today_str}.")

# ------------------------------------------------------------------------------
# Функция-обработчик для отправки уведомления по email при ошибке
# ------------------------------------------------------------------------------

def notify_email(context):
	exception = context.get('exception', 'Нет информации об ошибки')
	task_id = context.get('task_instance').task_id
	subject = "tik_tok_stats.py failure"  # Тема письма – название скрипта
	message = f"Задача {task_id} завершилась с ошибкой:\n{exception}"
	send_email("analytics-scripts-reports@example.com", subject, message)

# ------------------------------------------------------------------------------
# Функции для задач DAG
# ------------------------------------------------------------------------------

def get_report(**kwargs):
	"""
	Задача для получения отчёта из TikTok API.
	Считывает CSV с токенами, вызывает функцию get_all_basic_reports_from_df,
	приводит поле stat_time_day к формату date и возвращает DataFrame в виде JSON-строки.
	"""
	# # Считываем CSV с данными авторизации
	# auth_df = pd.read_csv('tokens_n_ids.csv')
	
	# Рассчитываем даты: последние 14 дней
	end_date = datetime.today().strftime("%Y-%m-%d")
	start_date = (datetime.today() - timedelta(days=14)).strftime("%Y-%m-%d")
	
	df_result = get_all_basic_reports_from_df(
		auth_df=auth_df,
		start_date=start_date,
		end_date=end_date,  # последние 14 дней
		dimensions=['campaign_id', 'ad_type', "stat_time_day"],
		metrics=[
			"impressions", "spend", "clicks",
			"conversion", "cost_per_conversion", "conversion_rate",
			"result", "currency", "cost_per_result", "result_rate",
			"video_views_p100", 
		],
		data_level="AUCTION_CAMPAIGN",
		service_type="AUCTION",
		report_type="BASIC",
		page_size=1000,
		filtering=None,
		lifetime=False,
		add_campaign_names=True  # автоматическое добавление campaign_name
	)
	
	# Приводим поле stat_time_day к типу date
	df_result['stat_time_day'] = pd.to_datetime(df_result['stat_time_day']).dt.date
	# Возвращаем DataFrame в виде JSON-строки (с orient, удобным для восстановления)
	return df_result.to_json(orient="split", date_format="iso")

def update_db(**kwargs):
	"""
	Задача для обновления таблицы в базе данных.
	Получает результат предыдущей задачи через XCom, преобразует его в DataFrame,
	создает SQLAlchemy engine и вызывает функцию update_tik_tok_stats_table.
	"""
	ti = kwargs['ti']
	df_json = ti.xcom_pull(task_ids='get_report')
	if not df_json:
		raise ValueError("Нет данных из задачи get_report")
	
	df_result = pd.read_json(df_json, orient="split")
	
	engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}')
	update_tik_tok_stats_table(df_result, engine, table_name='tik_tok_stats', schema='public')


moscow_tz = pendulum.timezone("Europe/Moscow")
# ------------------------------------------------------------------------------
# Определение DAG
# ------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2025, 1, 1, tz="Europe/Moscow"),
    'email': ['analytics-scripts-reports@example.com'],
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'on_failure_callback': notify_email,
    'timezone': moscow_tz,
}

with DAG(
    dag_id='tik_tok_stats_pipeline',
    default_args=default_args,
    schedule_interval="0 9,12,14 * * *",
    catchup=False,
    description="Пайплайн для получения отчёта из TikTok API и обновления таблицы в БД"
) as dag:

    get_report_task = PythonOperator(
        task_id='get_report',
        python_callable=get_report,
        provide_context=True
    )

    update_db_task = PythonOperator(
        task_id='update_db',
        python_callable=update_db,
        provide_context=True
    )

    get_report_task >> update_db_task
