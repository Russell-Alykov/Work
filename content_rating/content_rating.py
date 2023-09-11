# -*- coding: utf-8 -*-
#---------TITLE----------------

#---------DESCRIPTION----------

#------------------------------


# Все ключевые данные анонимизированы

# Данный ETL пайплайн начинается с загрузки данных о контент рейтинге товара через API Ozon,
# заканчивается выгрузкой отчета с анализом по контент рейтингу каждого товара.
# Основная цель этого отчета - помочь дизайнерам карточек товара, следить за наполняемостью карточки каждого товара медиа-контентом.

# Финальные отчеты выгружаются в сетевое хранилище в формате Excel,
# в Google таблицу, а так же обновляется БД на PostgreSQL сервере

# Используется PostgreSQL и Python


import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import requests
import psycopg2
import configparser
import json
import numpy as np
from io import BytesIO
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Google sheets
import gspread
from df2gspread import df2gspread as d2g # заливает df в docs
from oauth2client.service_account import ServiceAccountCredentials # Авторизация

# Логирование для zabbix
def printstatuse(code,message1, message2):

	'''
	code - 1 INF - ok; 0 ERR - fail
	message1 - место ошибки в выполнении кода
	message2 - опционально, для текста ошибки, не использовать = ""

	printstatuse - cтрока статуса выполнения скрипта для мониторинга zabbix
	'''

	print(f'{code} >> {str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))} >> {message1} >> {message2}')

if __name__ == '__main__':

	config = configparser.ConfigParser()
	config.read('path_to/your_config')
	# чтение конфига
	try:
		base_db = config["db"]["base_db"]
		ip = config["db"]["ip"]
		port = config["db"]["port"]
		user = config["user"]["user"]
		password = config["user"]["pass"]
		printstatuse('1 INF','Чтение конфигурации для подключения к db_analytics выполнено успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка в чтении конфигурации', str(e))
		quit()

		# Подключение к db_analytics
	try:
		conn = psycopg2.connect(
		database=base_db,
		user=user,
		password=password,
		host=ip,
		port=port)
	except Exception as e:
		printstatuse('0 ERR','Ошибка в подключении к db_analytics', str(e))
		quit()

	# Запрос
	query = '''
	SELECT a.entity_name, iter, '"' || string_agg(fbo_sku::text, '","') || '"' skus_array, account_id, api_key FROM 
	(SELECT entity_name, fbo_sku,round((row_number() OVER (PARTITION BY entity_name))/100) iter
	FROM responses.oz_v2_product_info_v2
	WHERE fbo_sku IS NOT NULL AND fbo_sku != 0) a 
	JOIN dictionaries.oz_accounts b
	USING(entity_name)
	GROUP BY a.entity_name, iter, account_id, api_key
	'''
	query_brand = '''
	WITH otgruzka AS
	(SELECT DISTINCT "Артикул", MIN("Дата отгрузки 1C") AS "Дата отгрузки" FROM sandbox.otgruzka_dynamics
	WHERE "Дата отгрузки 1C" IS NOT NULL AND "Артикул" IS NOT NULL
	GROUP BY "Артикул")

	SELECT "Бренд", "Fbo sku", "Баркод", "Юр лицо", "Направление", "Артикул МП", b."Дата отгрузки"
	FROM "1c".product_full AS a
	LEFT JOIN otgruzka AS b
		ON a."Fbo sku" = b."Артикул"::NUMERIC
	WHERE "Бренд" IS NOT NULL AND "Fbo sku" IS NOT NULL AND b."Дата отгрузки" IS NOT NULL
	'''

	query_wb = '''
	SELECT DISTINCT ON ("nmID") "nmID", "imtID", entity_name, "mediaFiles"
	FROM responses.wb_v1_cards_filter
	WHERE "nmID" IS NOT NULL
	'''

	cur = conn.cursor()

	try:
		data = pd.read_sql_query(query, conn)
		data_to_join = pd.read_sql_query(query_brand, conn)
		data_wb = pd.read_sql_query(query_wb, conn)
	except Exception as e:
		printstatuse('0 ERR','Ошибка при экспорте sql запроса в df', str(e))
		quit()
	data_to_join = data_to_join.rename(columns={'Fbo sku': 'sku'})

	#################### Основной цикл загрузки данных с API Ozon ########################
	temp = pd.DataFrame()
	url = 'https://api-seller.ozon.ru/v1/product/rating-by-sku'

	for index, row in data.iterrows():
		headers = {'Client-id': str(row['account_id']), 'Api-key': str(row['api_key'])}
		body = '{"skus":[' + row['skus_array'] + ']}'
		body = json.loads(body)
		try:
			response = requests.post(url, headers=headers, json=body)
			result = response.json()
			df = pd.json_normalize(result["products"], record_path = ["groups","conditions"], meta = [["groups","name"],"rating","sku",["groups","key"], ["groups","rating"]])
			temp = pd.concat([temp, df], ignore_index=True)
			temp = temp.drop_duplicates(subset=['key', 'description', 'groups.name', 'sku', 'groups.key'])
		except Exception as e:
			printstatuse('0 ERR','Ошибка при выгрузке контент рейтинга', str(e))
			quit()
	#####################################################################################

	############# Обработка датасетов ###############
	temp = temp.drop_duplicates(subset=['key', 'description', 'groups.name', 'sku', 'groups.key'])
	temp = temp.drop(columns=['key', 'groups.key'])
	temp['sku'] = temp['sku'].astype('int64')
	temp = temp.merge(data_to_join, how='left', on='sku')

	pivot_out = pd.pivot_table(temp, index=['sku', 'Артикул МП', 'Юр лицо', 'Бренд', 'Направление'], columns='description', values='fulfilled', aggfunc=np.count_nonzero).fillna(0).astype(bool)
	pivot_out = pivot_out.replace({True: 'Да', False: 'Нет'})
	pivot_out = pivot_out.reset_index()
	pivot_out = pivot_out.merge(temp[['sku', 'Дата отгрузки']],  how='left', on='sku')
	pivot_out = pivot_out.drop_duplicates(subset=['sku', 'Бренд'])
	pivot_final = pivot_out.merge(temp[['sku', 'rating']], how='left', on='sku')
	pivot_out['Дата отгрузки'] = pd.to_datetime(pivot_out['Дата отгрузки']).dt.date
	pivot_final = pivot_final.drop_duplicates(subset=['sku', 'Бренд'])
	rating = pivot_final.pop('rating')
	pivot_final.insert(5, rating.name, rating)
	pivot_final = pivot_final.rename(columns={'rating': 'Рейтинг'})

	data_wb = data_wb.rename(columns={'nmID': 'sku'})
	data_wb = data_wb.merge(data_to_join, how='left', on='sku')
	data_wb = data_wb.drop_duplicates(subset=['sku','imtID', 'mediaFiles', 'Бренд', 'Баркод'])
	data_wb = data_wb.dropna(subset=['Бренд', 'Баркод'])
	data_wb = data_wb.reset_index(drop=True)
	##############################################

	######################### Циклы по проверке отсутствия фото и видео в карточке товара ##########################
	# Получение сегодняшней даты
	today = datetime.now().date()
	# Ozon
	pivot_final['Дни без видео'] = ''
	try:
		for index, row in pivot_final.iterrows():
			if row['Добавлено видео'] == 'Нет':
				pivot_final.loc[index, 'Дни без видео'] = (today - row['Дата отгрузки']).days
			else:
				pivot_final.loc[index, 'Дни без видео'] = 0

		for index, row in pivot_final.iterrows():
			if row['Добавлено 3 изображения и более'] == 'Нет':
				pivot_final.loc[index, 'Дни без фото'] = (today - row['Дата отгрузки']).days
			else:
				pivot_final.loc[index, 'Дни без фото'] = 'Фото добавлено'

		for index, row in pivot_final.iterrows():
			if type(row['Дни без видео']) == int and row['Дни без видео'] < 0:
				pivot_final.loc[index, 'Дни без видео'] = 'Еще не отгружен'

		for index, row in pivot_final.iterrows():
			if type(row['Дни без фото']) == int and row['Дни без фото'] < 0:
				pivot_final.loc[index, 'Дни без фото'] = 'Еще не отгружен'

		# WB
		data_wb['Наличие фото'] = ''
		for index, row in data_wb.iterrows():
			if 'jpg' in row['mediaFiles']:
				data_wb.loc[index, 'Наличие фото'] = 'Да'
			else:
				data_wb.loc[index, 'Наличие фото'] = 'Нет'

		data_wb['Наличие видео'] = ''
		for index, row in data_wb.iterrows():
			if 'mp4' in row['mediaFiles']:
				data_wb.loc[index, 'Наличие видео'] = 'Да'
			else:
				data_wb.loc[index, 'Наличие видео'] = 'Нет'

		data_wb['Дни без видео'] = ''
		for index, row in data_wb.iterrows():
			if row['Наличие видео'] == 'Нет':
				data_wb.loc[index, 'Дни без видео'] = (today - row['Дата отгрузки']).days
			else:
				data_wb.loc[index, 'Дни без видео'] = 0

		data_wb['Дни без фото'] = ''
		for index, row in data_wb.iterrows():
			if row['Наличие фото'] == 'Нет':
				data_wb.loc[index, 'Дни без фото'] = (today - row['Дата отгрузки']).days
			else:
				data_wb.loc[index, 'Дни без фото'] = 'Фото добавлено'

		for index, row in data_wb.iterrows():
			if type(row['Дни без видео']) == int and row['Дни без видео'] < 0:
				data_wb.loc[index, 'Дни без видео'] = 'Еще не отгружен'

		for index, row in data_wb.iterrows():
			if type(row['Дни без фото']) == int and row['Дни без фото'] < 0:
				data_wb.loc[index, 'Дни без фото'] = 'Еще не отгружен'
		printstatuse('1 INF','Фото, видео и отгрузка успешно обработанно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при обработке колонок с фото, видео и датой отгрузки', str(e))
		quit()
	data_wb = data_wb.drop(columns=['imtID', 'mediaFiles', 'entity_name'])
	data_wb = data_wb.drop_duplicates(subset = ['sku', 'Баркод', 'Артикул МП'])
	#########################################################################################################

	######### Запсиь Excel в сетевое хранилище ##########
	try:
		with pd.ExcelWriter('path/Контент рейтинг сводная таблица.xlsx') as writer:
			writer.close()
		printstatuse('1 INF','Таблица выгружена в NC',"")
	except Exception as e:
		printstatuse(f'0 ERR','Таблица не выгружена в NC.', str(e))
	####################################################

	##################### Запись в БД SQL (обновление старых, добавление новых) ########################
	try:
		engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}', echo=False)
		dbConnection = engine.raw_connection()
		sku_lst = temp['sku'].unique().tolist()
		api_answer_sku_lst = ','.join(f'{sku}' for sku in sku_lst)
		query = f"DELETE FROM responses.oz_v1_product_raiting_by_sku WHERE sku IN ({api_answer_sku_lst})"
		engine.execute(query)
		temp.to_sql('oz_v1_product_raiting_by_sku', schema = 'responses',con=engine, if_exists='append', index = False)
		dbConnection.close()
		printstatuse("1 INF","Успешная запись данных в БД ","")
	except Exception as e:
		printstatuse("0 ERR","Неудача при записи данных в БД ",str(e))
		quit()
	####################################################################################################

	####################### аутпут в  Google Sheets ###########################
	# API key Google и необходимые APIs
	path_to_credential = '/path/api_key.json' #
	scope = ['https://spreadsheets.google.com/feeds',
			'https://www.googleapis.com/auth/drive']

	# Авторизация
	try:
		credentials = ServiceAccountCredentials.from_json_keyfile_name(path_to_credential, scope)
		gs = gspread.authorize(credentials)
		printstatuse('1 INF','Авторизация в Google API прошла успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка авторизации в Google API',str(e))
		quit()

	# Google sheet
	spreadsheet = 'google_sheet_link'
	wks_name = 'Контент рейтинг'

	# Загрузка дф с гугл таблицы
	try:
		req = requests.get('https://docs.google.com/spreadsheets/d/google_sheet_link')
		gs_data = req.content
		df_from_gs = pd.read_csv(BytesIO(gs_data))
		printstatuse('1 INF','Данные с листа для сбора контент рейтинга загружены',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка загрузки данных с листа гугл таблицы',str(e))
		quit()

	df_from_gs = df_from_gs.drop(index=df_from_gs.index[0:1], axis=0, inplace=False)
	df_from_gs = df_from_gs.rename(columns={'Unnamed: 2': 'sku'})
	df_from_gs = df_from_gs.dropna(subset='sku')
	df_from_gs = df_from_gs['sku']
	df_from_gs = df_from_gs.astype('int64')
	pivot_gs = pivot_final.merge(df_from_gs, how='right', on='sku')
	pivot_gs = pivot_gs.rename(columns={'rating': 'Рейтинг'})
	try:
		d2g.upload(pivot_gs, spreadsheet, wks_name, col_names=True, row_names=False, credentials=credentials, clean=True)
		printstatuse('1 INF','Контент рейтинг загружен в gs',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка загрузки в gs.',str(e))
		quit()
	############################################################################
