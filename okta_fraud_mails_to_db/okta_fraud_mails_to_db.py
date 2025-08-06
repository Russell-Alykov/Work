# -*- coding: utf-8 -*-
#---------TITLE----------------
# fraud_mails_to_db
#---------DESCRIPTION----------
# Выгружает редирект почты с analytics-data-aggregator@example.com
# удаляет фрод данные из файла конверсий по AppsFlyer_ID
# грузит финальный результат в public.no_fraud_mail_redirect (хранение прошлый месяц + текущий),
# а так же врменно (6 дней) сгружает фрод в public.temporary_fraud_mails
#------------------------------ 

import warnings

warnings.filterwarnings('ignore')
import os
import io
import zipfile
import base64
import logging
from datetime import date, timedelta, datetime
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Импорт для работы с Gmail API
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Чтение переменных среды
load_dotenv('/home/analyst/analyst_dev/rr_alykov/scripts/fraud_mails_to_db/.env')

# SQL
base_db = os.getenv('BASE_DB')
ip = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER_DB')
password = os.getenv('PASS')

CLIENT_SECRET_FILE = os.getenv('CLIENT_SECRET_FILE')
TOKEN_FILE = os.getenv('TOKEN_FILE')
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
DB_CONNECTION_STRING = f'postgresql://{user}:{password}@{ip}:{port}/{base_db}'
# Названия таблиц в БД
TEMP_FRAUD_TABLE = 'temporary_fraud_mails'
NO_FRAUD_TABLE = 'no_fraud_mail_redirect'

# Словарь переименования столбцов для итогового DataFrame.
# Обратите внимание: если входной файл содержит столбец county_code вместо country_code,
# то он тоже будет переименован в State.
rename_mapping = {
	'event_time_dt': 'Event_Time',
	'install_time_dt': 'Install_Time',
	'attributed_touch_time': 'Attributed_Touch_Time',
	'attributed_touch_type': 'Attributed_Touch_Type',
	'app_id': 'App_ID',
	'appsflyer_id': 'AppsFlyer_ID',
	'event_name': 'Event_Name',
	'media_source': 'Media_Source',
	'country_code': 'Country_Code',
	'campaign_id': 'Campaign_ID',
	'campaign': 'Campaign',
	'agency': 'Partner',
	'af_siteid': 'Site_ID',
	'af_adset_id': 'Adset_ID',
	'af_adset': 'Adset'
}

# Список столбцов, которые необходимо удалить из итогового DataFrame
cols_to_drop = [
	'af_channel', 'af_ad', 'af_ad_id', 'af_ad_type', 'event_source',
	'region', 'os_version', 'app_version', 'sdk_version', 'is_retargeting'
]

# ------------------------- Функции работы с базой данных -------------------------
def get_db_engine():
	engine = create_engine(DB_CONNECTION_STRING)
	return engine

def upsert_no_fraud(table_name, engine, df, unique_cols):
	meta = MetaData()
	meta.reflect(bind=engine, only=[table_name])
	table = Table(table_name, meta, autoload_with=engine)
	# Исключаем столбец "id"
	table_cols = set(table.columns.keys())
	table_cols.discard('id')
	df = df[[col for col in df.columns if col in table_cols]]
	data = df.to_dict(orient='records')
	if not data:
		logging.info("Нет данных для вставки в таблицу %s", table_name)
		return
	stmt = pg_insert(table).values(data)
	update_dict = {
		col: stmt.excluded[col]
		for col in df.columns
		if col not in unique_cols and col in table_cols
	}
	stmt = stmt.on_conflict_do_update(index_elements=unique_cols, set_=update_dict)
	with engine.begin() as conn:
		conn.execute(stmt)
	logging.info("Upsert завершён для таблицы %s (%d записей).", table_name, len(df))

def update_fraud_data_temp(df_fraud, engine, table_name=TEMP_FRAUD_TABLE, schema='public', retention_days=10):
	df = df_fraud.copy()
	df = df.loc[:, ~df.columns.duplicated()]
	
	if 'install_time' in df.columns:
		df['install_time'] = pd.to_datetime(df['install_time'], errors='coerce')
	
	if 'blocked_reason' in df.columns and 'fraud_reason' in df.columns:
		df['blocked_reason'] = df['blocked_reason'].fillna(df['fraud_reason'])
		df.drop(columns=['fraud_reason'], inplace=True)
	elif 'fraud_reason' in df.columns and 'blocked_reason' not in df.columns:
		df.rename(columns={'fraud_reason': 'blocked_reason'}, inplace=True)
	
	if 'blocked_sub_reason' in df.columns and 'fraud_sub_reason' in df.columns:
		df['blocked_sub_reason'] = df['blocked_sub_reason'].fillna(df['fraud_sub_reason'])
		df.drop(columns=['fraud_sub_reason'], inplace=True)
	elif 'fraud_sub_reason' in df.columns and 'blocked_sub_reason' not in df.columns:
		df.rename(columns={'fraud_sub_reason': 'blocked_sub_reason'}, inplace=True)
	
	df = df.loc[:, ~df.columns.duplicated()]
	df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
	logging.info("Фрод-данные добавлены в таблицу %s (%d записей).", table_name, len(df))
	
	cutoff = date.today() - timedelta(days=retention_days)
	delete_query = text(f"DELETE FROM {schema}.{table_name} WHERE install_time < :cutoff")
	with engine.begin() as conn:
		result = conn.execute(delete_query, {'cutoff': cutoff})
	logging.info("Удалено %s записей из %s, старше %s.", result.rowcount, table_name, cutoff)

# ------------------------- Функции работы с Gmail API -------------------------
def get_gmail_service(client_secret_file=CLIENT_SECRET_FILE, token_file=TOKEN_FILE):
	creds = None
	if os.path.exists(token_file):
		logging.info("Файл токена найден: %s", token_file)
		try:
			creds = Credentials.from_authorized_user_file(token_file, SCOPES)
		except Exception as e:
			logging.error("Ошибка загрузки токена: %s", e)
	else:
		logging.info("Файл токена %s не найден.", token_file)
	if not creds or not creds.valid:
		if creds and creds.expired and creds.refresh_token:
			logging.info("Токен истёк, обновляем...")
			creds.refresh(Request())
		else:
			logging.info("Запуск OAuth Flow...")
			flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
			creds = flow.run_local_server(port=0)
		with open(token_file, 'w') as token:
			token.write(creds.to_json())
			logging.info("Новый токен сохранён: %s", token_file)
	service = build('gmail', 'v1', credentials=creds)
	logging.info("Gmail API service создан.")
	return service

def list_messages(service, user_id='me', max_results=50, q=None):
	params = {'userId': user_id, 'maxResults': max_results}
	if q:
		params['q'] = q
		logging.info("Поисковый запрос: %s", q)
	results = service.users().messages().list(**params).execute()
	messages = results.get('messages', [])
	logging.info("Получено %d сообщений.", len(messages))
	return messages

def get_message(service, user_id, msg_id):
	return service.users().messages().get(userId=user_id, id=msg_id, format='full').execute()

def extract_attachments_recursive(payload, filename_filter=None):
	attachments = []
	if 'parts' in payload:
		for part in payload['parts']:
			fname = part.get('filename')
			if fname and ((not filename_filter) or (filename_filter.lower() in fname.lower())):
				attach_id = part.get('body', {}).get('attachmentId')
				if attach_id:
					attachments.append((fname, attach_id))
			attachments.extend(extract_attachments_recursive(part, filename_filter))
	return attachments

def get_attachment_data(service, user_id, msg_id, attach_id):
	attach = service.users().messages().attachments().get(
		userId=user_id, messageId=msg_id, id=attach_id
	).execute()
	file_data = base64.urlsafe_b64decode(attach['data'].encode('UTF-8'))
	return file_data

def attachment_bytes_to_dfs(file_bytes, filename, encoding='utf-8'):
	result = []
	lower_name = filename.lower()
	if lower_name.endswith('.csv'):
		try:
			s = file_bytes.decode(encoding)
			df = pd.read_csv(io.StringIO(s))
			result.append((filename, df))
			logging.info("Файл %s прочитан как CSV.", filename)
		except Exception as e:
			logging.warning("Ошибка чтения CSV файла %s: %s", filename, e)
	elif lower_name.endswith('.zip'):
		try:
			with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
				for name in zf.namelist():
					if name.lower().endswith('.csv'):
						with zf.open(name) as f:
							df = pd.read_csv(f)
							result.append((name, df))
						logging.info("Из ZIP файла %s извлечён CSV: %s", filename, name)
		except Exception as e:
			logging.warning("Ошибка чтения ZIP файла %s: %s", filename, e)
	else:
		logging.info("Файл %s не распознаётся как CSV или ZIP.", filename)
	return result

def get_dfs_from_message(service, user_id, msg_id, filename_filter=None):
	message = get_message(service, user_id, msg_id)
	payload = message.get('payload', {})
	attachments_info = extract_attachments_recursive(payload, filename_filter=filename_filter)
	dfs = {}
	if not attachments_info:
		logging.error("Во вложениях письма %s ничего не найдено.", msg_id)
	for (filename, attach_id) in attachments_info:
		try:
			file_bytes = get_attachment_data(service, user_id, msg_id, attach_id)
			extracted = attachment_bytes_to_dfs(file_bytes, filename)
			for csv_name, df in extracted:
				key = os.path.splitext(csv_name)[0]
				dfs[key] = df
		except Exception as e:
			logging.warning("Ошибка загрузки файла %s: %s", filename, e)
	return dfs

def get_dfs_from_messages(service, user_id, msg_ids, filename_filter=None):
	final_dfs = {}
	for msg_id in msg_ids:
		dfs = get_dfs_from_message(service, user_id, msg_id, filename_filter)
		final_dfs.update(dfs)
	return final_dfs

# ------------------------- Функции обработки данных -------------------------
def filter_conversion_df(conv_df, fraud_dfs, id_col='appsflyer_id'):
	"""
	Оставляем только записи, где event_name (в нижнем регистре и без лишних пробелов) == 'af_realtime_activation',
	и исключаем строки, чей appsflyer_id (нормализованный) присутствует во фрод-данных.
	"""
	conv_df['event_name'] = conv_df['event_name'].str.lower().str.strip()
	conv_df = conv_df[conv_df['event_name'] == 'af_realtime_activation']
	if id_col in conv_df.columns:
		conv_df[id_col] = conv_df[id_col].astype(str).str.strip().str.lower()
	union_ids = set()
	for df in fraud_dfs:
		if id_col in df.columns:
			ids_local = df[id_col].astype(str).str.strip().str.lower().dropna().unique()
			union_ids.update(ids_local)
	filtered = conv_df[~conv_df[id_col].isin(union_ids)]
	logging.info("После фильтрации конверсий осталось %d строк.", len(filtered))
	return filtered

def convert_to_naive_datetime(series):
	parsed = pd.to_datetime(series, errors='coerce')
	return parsed.apply(lambda x: x.replace(tzinfo=None) if pd.notnull(x) and getattr(x, 'tzinfo', None) else x)

def fix_event_time_and_create_date(df):
	df = df.copy()
	if 'event_time_dt' in df.columns:
		df['event_time'] = convert_to_naive_datetime(df['event_time_dt'])
	elif 'event_time' in df.columns:
		df['event_time'] = convert_to_naive_datetime(df['event_time'])
	else:
		logging.error("Нет столбца для определения времени события.")
		df['event_time'] = pd.NaT
	if df['event_time'].notna().any():
		df['date'] = pd.to_datetime(df['event_time'], errors='coerce').dt.date
	elif 'install_time_dt' in df.columns:
		df['install_time_dt'] = pd.to_datetime(df['install_time_dt'], errors='coerce')
		df['date'] = df['install_time_dt'].dt.date
	else:
		df['date'] = None
	return df

def reorder_date_column(df):
	cols = list(df.columns)
	if 'date' in cols:
		cols.remove('date')
		cols.insert(1, 'date')
		df = df.loc[:, cols]
		logging.info("Столбец 'date' перемещён на вторую позицию.")
	return df

def update_no_fraud_data_final(df_final, engine, table_name=NO_FRAUD_TABLE, schema='public'):
	df = df_final.copy()
	df = df.loc[:, ~df.columns.duplicated()]

	# Переименование столбцов согласно rename_mapping
	df.rename(columns=rename_mapping, inplace=True)
	logging.info("Переименование столбцов выполнено.")

	# Удаляем лишние столбцы
	cols_present = [col for col in cols_to_drop if col in df.columns]
	if cols_present:
		df.drop(columns=cols_present, inplace=True, errors='ignore')
		logging.info("Удалены столбцы: %s", cols_present)

	if 'Install_Time' in df.columns:
		df['Install_Time'] = pd.to_datetime(df['Install_Time'], errors='coerce')

	# Создаём столбец date из Event_Time или Install_Time
	if 'Event_Time' in df.columns:
		df['Event_Time'] = pd.to_datetime(df['Event_Time'], errors='coerce')
		df['date'] = df['Event_Time'].dt.date
	elif 'Install_Time' in df.columns:
		df['date'] = pd.to_datetime(df['Install_Time'], errors='coerce').dt.date
	else:
		df['date'] = None

	df = df[df['date'].notnull()]
	df = reorder_date_column(df)

	if 'AppsFlyer_ID' in df.columns:
		df['AppsFlyer_ID'] = df['AppsFlyer_ID'].astype(str).str.strip().str.lower()

	# Убеждаемся, что есть уникальный индекс по (AppsFlyer_ID, date)
	with engine.begin() as conn:
		conn.execute(text(
			"""CREATE UNIQUE INDEX IF NOT EXISTS idx_no_fraud_unique
			   ON public.no_fraud_mail_redirect ("AppsFlyer_ID", date)"""
		))

	meta = MetaData()
	meta.reflect(bind=engine, only=[table_name])
	table = Table(table_name, meta, autoload_with=engine)
	table_cols = set(table.columns.keys())
	table_cols.discard('id')
	df = df[[col for col in df.columns if col in table_cols]]
	logging.info("Оставлены столбцы, присутствующие в таблице %s: %s", table_name, list(df.columns))

	unique_cols = ['AppsFlyer_ID', 'date']
	df = df.drop_duplicates(subset=unique_cols)

	upsert_no_fraud(table_name, engine, df, unique_cols=unique_cols)
	logging.info("Обновление итоговой таблицы %s завершено.", table_name)
	return df

# ------------------------- Основной блок обработки -------------------------
def main():
	service = get_gmail_service()
	user_id = 'me'
	engine = get_db_engine()

	# Обработка только сегодняшнего дня
	current_date = date.today()

	# 1) Получаем письма и формируем DataFrame конверсий
	q = f"after:{current_date.strftime('%Y/%m/%d')} before:{(current_date + timedelta(days=1)).strftime('%Y/%m/%d')}"
	logging.info("Поисковый запрос: %s", q)

	messages = list_messages(service, user_id=user_id, max_results=50, q=q)
	msg_ids = [m['id'] for m in messages]
	logging.info("Используем ID сообщений: %s", msg_ids)

	dfs_dict = get_dfs_from_messages(service, user_id, msg_ids)
	logging.info("Получены DataFrame из писем: %s", list(dfs_dict.keys()))

	conversion_key = "marketing_partners-boostapp-conversions_30_day_from_install_boostapp"
	if conversion_key not in dfs_dict:
		logging.error("Файл конверсий '%s' не найден для даты %s.", conversion_key, current_date)
	else:
		conv_df = dfs_dict[conversion_key]
		logging.info("Конверсии (первые 5 строк) для даты %s:\n%s", current_date, conv_df.head())

		# 2) Получаем фрод-данные из писем (если есть)
		fraud_df_email = pd.DataFrame()
		fraud_key1 = "marketing_partners-boostapp-postattr_fraud_installs_boostapp"
		fraud_key2 = "marketing_partners-boostapp-realtime_fraud_installs_boostapp"

		if fraud_key1 in dfs_dict or fraud_key2 in dfs_dict:
			if fraud_key1 in dfs_dict and fraud_key2 in dfs_dict:
				fraud_df_email = pd.concat([dfs_dict[fraud_key1], dfs_dict[fraud_key2]], ignore_index=True)
			elif fraud_key1 in dfs_dict:
				fraud_df_email = dfs_dict[fraud_key1]
			else:
				fraud_df_email = dfs_dict[fraud_key2]
			logging.info("Фрод данные из писем для даты %s (первые 5 строк):\n%s", current_date, fraud_df_email.head())
		else:
			logging.info("Фрод данные из писем отсутствуют для даты %s.", current_date)

		# 3) Получаем фрод-данные из БД
		fraud_df_db = pd.read_sql(text(f"SELECT * FROM public.{TEMP_FRAUD_TABLE}"), engine)
		logging.info("Фрод данные из БД для даты %s (первые 5 строк):\n%s", current_date, fraud_df_db.head())

		# 4) Объединяем фрод-данные
		fraud_union = pd.concat([fraud_df_email, fraud_df_db], ignore_index=True)

		# 5) Фильтруем конверсии, убирая фродовые ID
		final_df = filter_conversion_df(conv_df, [fraud_union], id_col='appsflyer_id')
		final_df = fix_event_time_and_create_date(final_df)
		logging.info("После фильтрации итоговый DataFrame (первые 5 строк) для даты %s:\n%s", current_date, final_df.head())

		# 6) Вставляем «чистые» данные в итоговую таблицу
		updated_df = update_no_fraud_data_final(final_df, engine, table_name=NO_FRAUD_TABLE)
		logging.info("После загрузки итоговый DataFrame (первые 5 строк) для даты %s:\n%s", current_date, updated_df.head())

		# 7) Очищаем итоговую таблицу по всем фродовым ID
		delete_query = text(f"""
			DELETE FROM public.no_fraud_mail_redirect
			WHERE lower("AppsFlyer_ID") IN (
				SELECT lower(appsflyer_id)
				FROM public.{TEMP_FRAUD_TABLE}
			)
		""")
		with engine.begin() as conn:
			result = conn.execute(delete_query)
		logging.info("Очистка итоговой таблицы по AppsFlyer_ID: удалено %d записей.", result.rowcount)

		# 8) Если во вложениях были новые фрод-файлы, дополняем TEMP_FRAUD_TABLE
		if not fraud_df_email.empty:
			update_fraud_data_temp(fraud_df_email, engine, table_name=TEMP_FRAUD_TABLE, retention_days=10)

	# Финальная очистка после обработки (на случай, если фрод-файлы поступят с опозданием)
	final_delete_query = text(f"""
		DELETE FROM public.no_fraud_mail_redirect
		WHERE lower("AppsFlyer_ID") IN (
			SELECT lower(appsflyer_id)
			FROM public.{TEMP_FRAUD_TABLE}
		)
	""")
	with engine.begin() as conn:
		final_result = conn.execute(final_delete_query)
	logging.info("Финальная очистка после обработки: удалено %d записей.", final_result.rowcount)

	logging.info("Обработка завершена для даты %s.", current_date)


if __name__ == '__main__':
	main()


