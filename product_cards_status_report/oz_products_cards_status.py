# -*- coding: utf-8 -*-
#---------TITLE----------------

#---------DESCRIPTION----------

#------------------------------
import warnings
warnings.filterwarnings('ignore')
import sys
import os
sys.path.append(os.path.join(sys.path[0], '/usr/lib/python3/dist-packages'))

import psycopg2
import configparser
import json
import warnings
import time
import pandas as pd
from datetime import datetime, timedelta




# Логирование, f-строка для zabbix
def printstatuse(code,message1, message2):
	print(f'{code} >> {str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))} >> {message1} >> {message2}')
if __name__ == '__main__':
		# чтение конфига
	config = configparser.ConfigParser()
	config.read('/home/dev_0/sys/ralykov_db_connect.ini')
	try:
		base_db = config["db"]["base_db"]
		ip = config["db"]["ip"]
		port = config["db"]["port"]
		user = config["user"]["user"]
		password = config["user"]["pass"]
		printstatuse('1 INF','Чтение конфигурации для подключения к db_anlt выполнено успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка в чтении конфигурации', str(e))
		quit()
		# Подключение к db_anlt
	try:
		conn = psycopg2.connect(
		database=base_db,
		user=user,
		password=password,
		host=ip,
		port=port)
	except Exception as e:
		printstatuse('0 ERR','Ошибка в подключении к db_anlt', str(e))
		quit()

	# Запрос
	query='''
	SELECT 
		DATE_TRUNC('second', a.updated_at::TIMESTAMP) "Дата и время обновления выгрузки", 
		a.fbo_sku, 
		a.product_id, 
		c."ЛЮМ", 
		c."Бренд", 
		c."Направление", 
		a.name, 
		a.status, 
		b.is_fbo_visible,
		b.is_fbs_visible, 
		b.archived, 
		b.is_discounted, 
		a.stocks, 
		a.commissions, 
		a.discounted_stocks
	FROM responses.oz_v2_product_info_v2 AS a
	LEFT JOIN responses.oz_v2_product_list_v2 AS b
		USING(product_id)
	LEFT JOIN (SELECT DISTINCT ON("Fbo sku") "Fbo sku", "ЛЮМ", "Бренд", "Направление" FROM "1c".product_full WHERE "МП" = 'OZON' and "Fbo sku" IS NOT NULL) AS c
		ON a.fbo_sku = c."Fbo sku"
	'''
	cur = conn.cursor()

	# формирование df
	try:
		data = pd.read_sql_query(query, conn)
	except Exception as e:
		printstatuse('0 ERR','Ошибка при экспорте sql запроса в df', str(e))
		quit()


	data = data.rename(columns={'fbo_sku': 'SKU/Артикуль',
								'product_id': 'ID продукта',
								'name': 'Наименование',
								'is_fbo_visible': 'Видимость FBO',
								'is_fbs_visible': 'Видимость FBS',
								'archived': 'В архиве',
								'is_discounted': 'Дисконт'})
	try:
		df = pd.DataFrame()
		df_errors = pd.DataFrame()
		df_stocks = pd.DataFrame()
		df_discounted_stocks = pd.DataFrame()
		df_commissions = pd.DataFrame()
		for index, row in data.iterrows():
		    json_obj = json.loads(row['status'])
		    json_obj_item_errors = json.loads(row['status'])
		    json_obj_stocks = json.loads(row['stocks'])
		    json_obj_discounted_stocks = json.loads(row['discounted_stocks'])
		    json_obj_commissions = json.loads(row['commissions'])
		    temp = pd.json_normalize(json_obj)
		    temp_item_errors = pd.json_normalize(json_obj_item_errors, record_path="item_errors")
		    temp_stocks = pd.json_normalize(json_obj_stocks)
		    temp_discounted_stocks = pd.json_normalize(json_obj_discounted_stocks)
		    temp_commissions = pd.json_normalize(json_obj_commissions)
		    df = pd.concat([df, temp])
		    df_errors = pd.concat([df_errors, temp_item_errors])
		    df_stocks = pd.concat([df_stocks, temp_stocks])
		    df_discounted_stocks = pd.concat([df_discounted_stocks, temp_discounted_stocks])
		    df_commissions = pd.concat([df_commissions, temp_commissions])
		printstatuse('1 INF','Джейсон дампы собраны успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка в парсинге джесонов вурхизов', str(e))

	# джоины дампов
	df = df.reset_index(drop=True)
	df = df.reset_index()
	df = df.rename(columns={'state': 'Состояние товара', 
							'state_failed': 'Состояние товара, на переходе в которое произошла ошибка',
							'moderate_status': 'Статус модерации',
							'decline_reasons': 'Причины отклонения товара', 
							'validation_state': 'Статус валидации', 
							'state_name': 'Название состояния товара', 
							'state_description': 'Описание состояния товара',
							'is_failed': 'При создании товара возникли ошибки',
							'is_created': 'Товар создан',
							'state_tooltip': 'Подсказки для текущего состояния товара'})

	try:
		df_errors = df_errors.reset_index(drop=True)
		df_errors = df_errors.reset_index()
		df_errors = df_errors.drop(columns=["code", "field", "attribute_id", "state", "level", "attribute_name"], axis=1) #, inplace=True
		df_errors = df_errors.rename(columns={'description': 'Ошибки при загрузке товаров'})
	except Exception as e:
		printstatuse('0 ERR','df_errors вызывает тебя на бой', str(e))
		quit()
	df_stocks = df_stocks.reset_index(drop=True)
	df_stocks = df_stocks.reset_index()
	df_stocks = df_stocks.rename(columns={'coming': 'Остатки товаров/Ожидаемые', 
										'present': 'Остатки товаров/На складе', 
										'reserved': 'Остатки товаров/Зарезервировано'})
	df_discounted_stocks = df_discounted_stocks.reset_index(drop=True)
	df_discounted_stocks = df_discounted_stocks.reset_index()
	df_discounted_stocks = df_discounted_stocks.rename(columns={'coming': 'Остатки уцененных товаров/Ожидаемые', 
													'present': 'Остатки уцененных товаров/На складе',
													'reserved': 'Остатки уцененных товаров/Зарезервировано'})
	df_commissions = df_commissions.reset_index(drop=True)
	df_commissions = df_commissions.reset_index()
	df_commissions = df_commissions.rename(columns={'delivery_amount': 'Стоимость доставки',
													'min_value': 'Минимальная комиссия',
													'percent': 'Процент комиссии',
													'return_amount': 'Стоимость возврата',
													'sale_schema': 'Схема продажи',
													'value': 'Сумма комиссии'})
	data = data.reset_index(drop=True)
	data = data.reset_index()

	data = data.merge(df, how='left', on='index')
	data = data.merge(df_errors, how='left', on='index')
	data = data.merge(df_stocks, how='left', on='index')
	data = data.merge(df_discounted_stocks, how='left', on='index')
	data = data.merge(df_commissions, how='left', on='index')
	data = data.drop(columns=['index', 'status', 'stocks', 'commissions', 'item_errors', 'discounted_stocks', 'state_updated_at'])
	data[['Видимость FBO', 'Видимость FBS', 'В архиве', 'Дисконт', 'При создании товара возникли ошибки', 'Товар создан']] = data[['Видимость FBO', 'Видимость FBS', 'В архиве', 'Дисконт', 'При создании товара возникли ошибки', 'Товар создан']].replace({True: 'Да', False: 'Нет'})
	data['Статус модерации'] = data['Статус модерации'].replace({'approved': 'одобрено', 
																'declined': 'отклонено',
																'in-moderating': 'в модерации', 
																'postmoderation': 'постмодерация'})

	data['Состояние товара'] = data['Состояние товара'].replace({'price_sent': 'цена отправлена',
																'pdf_delivered': 'pdf доставлено',
																'declined': 'отклонено',
																'unmatched': 'нет совпадений', 
																'moderated': 'модерируется',
																'sku_created': 'артикуль создан',
																'create_variant': 'создать вариант',
																'imported': 'импортирован'})

	data['Статус валидации'] = data['Статус валидации'].replace({'success': 'успешно', 
																'fail': 'неуспешно',
																'pending': 'в процессе'})

	data['Состояние товара, на переходе в которое произошла ошибка'] = data['Состояние товара, на переходе в которое произошла ошибка'].replace({'pdf_delivered': 'pdf доставлено',
																																				'pics_delivered': 'фото доставлены',
																																				'pics_stored': 'фото сохранены',
																																				'validated': 'проверено',
																																				'create_variant': 'создать вариант',
																																				'hang': 'висит',
																																				'item_created': 'товар создан',
																																				'imported': 'импортирован',
																																				'price_sent': 'цена отправлена',
																																				'offer_validated': 'предложение подтверждено',
																																				'variant_wait': 'ожидание варианта'})
	data['ЛЮМ'] = data['ЛЮМ'].replace({None: 'отсуствует'})
	data['Бренд'] = data['Бренд'].replace({None: 'отсуствует'})
	data['Ошибки при загрузке товаров'] = data['Ошибки при загрузке товаров'].replace({None: 'отсуствуют'})
	data['Направление'] = data['Направление'].replace({None: 'отсуствует'})
	data = data.sort_values('Дата и время обновления выгрузки', ascending=False)
	data = data.reset_index(drop=True)

	# аутпут
	try:
		writer = pd.ExcelWriter('/var/nextcloud/users/report_data/Товары OZON/Статусы карточек.xlsx')
		data.to_excel(writer, sheet_name='Статусы карточек', index=False, na_rep='NaN')
		for column in data:
			column_length = max(data[column].astype(str).map(len).max(), len(column))
			col_idx = data.columns.get_loc(column)
			writer.sheets['Статусы карточек'].set_column(col_idx, col_idx, column_length)
		writer.close()
		#data.to_excel("/users/report_data/Товары OZON/Статусы карточек.xlsx", index=False)
		printstatuse('1 INF','Таблица выгружена в NC',"")
	except Exception as e:
		printstatuse('0 ERR','Таблица не выгружена в NC', str(e))




