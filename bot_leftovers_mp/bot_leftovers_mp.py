# -*- coding: utf-8 -*-
#---------TITLE----------------

#---------DESCRIPTION----------

#------------------------------


# Все ключевые данные анонимизированы

# Данный ETL пайплайн начинается с загрузки и преобразования данных с SQL сервера и закарчивается выгрузкой отчета каждому менеджеру в виде писма с HTML контентом.
# Оснавная цель этого проекта помочь менеджерам маркетплейсов отслеживать свои товары на складе, у которых заканчивается остаток
# и уровень оборачиваемости (скорости продажи товара в днях) равен определенному порогу.

# Оповещения о новой выгрузке приходят в Телеграмм, общаяя выгрузка по двум маркетплейсам выгружается в сетевое хранилище в формате Эксель,
# каждый менеджер получает письмо с HTML контентом карточки его товара.

# Используется PostgreSQL и Python


import warnings

warnings.filterwarnings('ignore')

import pandas as pd
import psycopg2
import configparser
import datetime as dt
import telebot as tb
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# Логирование для zabbix
def printstatuse(code,message1, message2):

	'''
	code - 1 INF - ok; 0 ERR - fail
	message1 - место ошибки в выполнении кода
	message2 - опционально, для текста ошибки, не использовать = ""

	printstatuse - cтрока статуса выполнения скрипта для мониторинга zabbix
	'''

	print(f'{code} >> {str(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))} >> {message1} >> {message2}')

if __name__ == '__main__':
	try:
		# Чтение конфига
		config = configparser.ConfigParser()
		config.read('/path_to/your_config')

		base_db = config["db_analytics"]["base_db"]
		ip = config["db_analytics"]["host_db"]
		port = config["db_analytics"]["port"]
		user = config["db_analytics"]["user"]
		password = config["db_analytics"]["pass"]
		printstatuse('1 INF','Чтение конфигурации для подключения к db_analytics выполнено успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка в чтении конфигурации', str(e))
		quit()

	# Подключение к БД
	try:
		conn = psycopg2.connect(
		database=base_db,
		user=user,
		password=password,
		host=ip,
		port=port)
		printstatuse('1 INF','Подключение к db_analytics выполнено успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при подключении к db_analytics', str(e))
		quit()

	################ Основной запрос к серверу PostgreSQL с данными за 3 дня ##################
	query_grand = '''
	WITH avg_orders_three_days_oz AS
	(SELECT "sku OZON", ROUND(SUM("Кол-во")::NUMERIC/3, 1) AS "Средние продажи за 3 дня Озон"
	FROM reports.gross 
	WHERE "Дата заказа"::DATE >= NOW()::DATE - INTERVAL '3 days' 
	GROUP BY "sku OZON"),

	avg_orders_three_days_wb AS
	(SELECT "Код WB", ROUND(SUM("Кол-во")::NUMERIC/3, 1) AS "Средние продажи за 3 дня WB"
	FROM reports.gross  
	WHERE "Дата заказа"::DATE >= NOW()::DATE - INTERVAL '3 days' 
	GROUP BY "Код WB"),

	priemka AS
	(SELECT "ЛЮМ", SUM("Принимается") AS "Принимается", "Наименование склада"
	FROM "1c".vypolnenie_priemki_tovarov
	WHERE  "Наименование склада" ilike 'Основной%'
	AND "Принимается" IS NOT NULL
	GROUP BY "ЛЮМ", "Наименование склада"),

	edp AS
	(SELECT "Баркод", SUM("EDP") AS "EDP"
	FROM middata.edp_every_day
	GROUP BY "Баркод"),

	ostatki_wb AS
	(SELECT "nmId", SUM(quantity) AS quantity
	FROM middata.wb_stocks
	GROUP BY "nmId"),

	ostatki_oz AS
	(SELECT fbo_sku, SUM(fbo_sku_qutt_present) AS fbo_sku_qutt_present, 
		SUM(fbo_sku_qutt_reserved) AS fbo_sku_qutt_reserved
	FROM middata.oz_stocks
	GROUP BY fbo_sku),

	oz_images_n_prices AS
	(SELECT
		a.product_id,
		barcode,
		fbo_sku, 
		name, 
		marketing_seller_price AS "Ваша цена Озон",
		primary_image
	FROM responses.oz_v2_product_info_v2 AS a
	LEFT JOIN responses.oz_v4_product_info_prices_v2 AS b
		ON a.product_id = b.product_id
	WHERE fbo_sku IS NOT NULL AND fbo_sku != 0
	ORDER BY b.actual_date),

	wb_images AS
	(SELECT nmid, "object", mediafiles
	FROM sandbox.wb_contentс_v1_cards_cursor_list
	WHERE mediafiles ILIKE '%/1.jpg%'),

	ostatki_1c AS
	(SELECT SPLIT_PART("Артикул", '_', 1) AS "ЛЮМ", SUM("В наличии") AS "В наличии"
	FROM "1c".stock_online
	GROUP BY "ЛЮМ"),

	otgruzka AS
	(SELECT "Баркод"  , SUM("Кол-во 1С") as "Забронировано к отгрузке"
	FROM sandbox.otgruzka_dynamics
	WHERE "Баркод" IS NOT null
	AND "Баркод" != '' 
	AND "Дата отгрузки 1C"::DATE  = NOW()::DATE
	AND  "Кол-во 1С" > 0 
	GROUP BY "Баркод"),

	spravochnik AS 
	(SELECT "Наименование 1С", "Ответственный" AS "КМ", SPLIT_PART("Ответственный", ' ', 1) AS last_name, SPLIT_PART("ЛЮМ", '_', 1) AS "ЛЮМ"
	FROM "1c".product_full),

	wb_prices AS
	(SELECT DISTINCT ON (nmid) nmid, price, price - (price * discount / 100) AS "Ваша цена WB"
	FROM responses.wb_v1_info
	WHERE price != 0 AND nmid IS NOT NULL AND actual_date::DATE >= NOW()::DATE - INTERVAL '3 days'),

	wb_vp AS
	(SELECT DISTINCT ON (barcode) barcode, sku ,ROUND((("Ваша цена WB"::numeric*a + b)/("Ваша цена WB"::numeric)*100)::NUMERIC, 1) AS "ВП WB"   
	FROM middata.valov_koeff_calculate AS a
	LEFT JOIN wb_prices AS b
		ON a.sku = b.nmid
	WHERE marketplace = 'WB'),

	oz_vp AS
	(SELECT sku, "Ваша цена Озон", ROUND((((a*"Ваша цена Озон") + (b+ ("Последняя миля старая" - "Последняя миля новая")*(1+"Доля возвратов"/"Доля выкупов")))/"Ваша цена Озон"*100)::NUMERIC, 1) AS "ВП Озон"  
	FROM 
	   (SELECT DISTINCT ON (sku) sku, "Доля выкупов", "Доля возвратов", a, b, "Ваша цена Озон",
			CASE WHEN "Цена для расчета" * 0.055 <= 20.0 THEN 20.0
				 WHEN "Цена для расчета" * 0.055 >= 500.0 THEN 500.0
				 ELSE "Цена для расчета" * 0.055 END "Последняя миля старая"
			,CASE WHEN "Ваша цена Озон" * 0.055 <= 20.0 THEN 20.0
				 WHEN "Ваша цена Озон" * 0.055 >= 500.0 THEN 500.0
				 ELSE "Ваша цена Озон" * 0.055 END "Последняя миля новая"
			FROM  middata.valov_koeff_calculate AS a
			LEFT JOIN oz_images_n_prices AS b
				ON a.sku = b.fbo_sku
			WHERE marketplace = 'OZON') val),

	email_dictionary AS 
	(SELECT a.last_name, b.last_name, email
	FROM  dictionaries."user" AS a
	RIGHT JOIN spravochnik AS b
	ON a.last_name = b.last_name)

	SELECT
		"Операция", a."Дата заказа", 
		 a."ЛЮМ", a."Баркод" , s."КМ", s.last_name, s."Наименование 1С", "Бренд", "МП", "Блок", 
		b_wb."Код WB", e."nmId", e.quantity AS "Остаток МП: ВБ",
		b_oz."sku OZON", f.fbo_sku, f.fbo_sku_qutt_present AS "Остаток МП: Озон", f.fbo_sku_qutt_reserved AS "Зарезервированно Озон",
		g.name AS "Наименование Озон", g."Ваша цена Озон", CONCAT(m."ВП Озон", '%') AS "ВП Озон",  primary_image AS "Картинка Озон",
		h."object" AS "Наименование ВБ", k."Ваша цена WB", CONCAT(l."ВП WB", '%') AS "ВП WB", mediafiles AS "Картинка ВБ",
		d."Наименование склада" AS "Наименование основного склада", "Наименование склада WB" AS "Наименование склада", "Кол-во",
		d."Принимается", i."В наличии"::INTEGER AS "Остатки 1С", c."EDP" AS "ЕДП", j."Забронировано к отгрузке",
		b_wb."Средние продажи за 3 дня WB", b_oz."Средние продажи за 3 дня Озон",
		CASE
		  WHEN "МП" = 'WB'
		  THEN ROUND((COALESCE(e.quantity, 0) + COALESCE(c."EDP", 0) + COALESCE(j."Забронировано к отгрузке", 0))::DECIMAL / (b_wb."Средние продажи за 3 дня WB")::DECIMAL, 1)
		  WHEN "МП" = 'OZON'
		  THEN ROUND((COALESCE(f.fbo_sku_qutt_present, 0) + COALESCE(c."EDP", 0) + COALESCE(j."Забронировано к отгрузке", 0))::DECIMAL / (b_oz."Средние продажи за 3 дня Озон")::DECIMAL, 1)
		END AS "Оборачиваемость" -- Основная формула    
	FROM reports.gross AS a
	LEFT JOIN avg_orders_three_days_oz AS b_oz
	  ON a."sku OZON"::NUMERIC = b_oz."sku OZON"::NUMERIC
	LEFT JOIN avg_orders_three_days_wb AS b_wb
	  ON a."Код WB"::NUMERIC = b_wb."Код WB"::NUMERIC
	LEFT JOIN edp AS c
	  ON a."Баркод" = c."Баркод"
	LEFT JOIN priemka AS d
	  ON a."ЛЮМ" = d."ЛЮМ"
	LEFT JOIN ostatki_wb AS e
	  ON a."Код WB"::NUMERIC = e."nmId"
	LEFT JOIN ostatki_oz AS f
	  ON a."sku OZON"::NUMERIC = f.fbo_sku
	LEFT JOIN oz_images_n_prices AS g
	  ON a."sku OZON"::NUMERIC = g.fbo_sku
	LEFT JOIN wb_images AS h
	  ON a."Код WB"::NUMERIC = h.nmid
	LEFT JOIN ostatki_1c AS i
	  ON a."ЛЮМ" = i."ЛЮМ" 
	LEFT JOIN otgruzka AS j
	  ON a."Баркод" = j."Баркод"
	LEFT JOIN spravochnik AS s
	  ON a."ЛЮМ" = s."ЛЮМ"
	LEFT JOIN wb_prices AS k
	  ON a."Код WB"::NUMERIC = k."nmid"
	LEFT JOIN wb_vp AS l
		ON a."Код WB"::NUMERIC = l.sku
	LEFT JOIN  oz_vp AS m
		ON a."sku OZON"::NUMERIC = m.sku
	WHERE "Операция" = 'Заказ' AND "Дата заказа" >= (SELECT MAX("Дата заказа"::DATE) - INTERVAL '3 days' FROM reports.gross) AND "Блок" IN ('бренд_1', 'бренд_2', 'бренд_3', 'бренд_4')
	ORDER BY a."Дата заказа" DESC
	'''

	query_email_dictionary = ''' 
	WITH last_name_tab AS
	(SELECT SPLIT_PART("Ответственный", ' ', 1) AS last_name
	FROM "1c".product_full
	WHERE "Ответственный" IS NOT NULL
	GROUP BY last_name)

	SELECT b.last_name, email
	FROM  dictionaries."user" AS a
	RIGHT JOIN last_name_tab AS b
		ON a.last_name = b.last_name
	'''
	########################################################################################

	################## Формирование датафреймов ###########################
	try:
		df_grand = pd.read_sql_query(query_grand, conn)
		query_email_dictionary = pd.read_sql_query(query_email_dictionary, conn)
		printstatuse('1 INF','Формирование датафреймов выполнено успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при формирование датафреймов', str(e))
		quit()
	#######################################################################

	############ Обработка датафреймов #############
	df_final = df_grand[df_grand["Оборачиваемость"] < 7]
	df_final = df_final.sort_values('Дата заказа')
	df_final['Кол-во продаж'] = df_final.groupby('ЛЮМ')['Кол-во'].transform('sum')
	df_final['Динамика продаж'] = df_final['Кол-во продаж'].diff().apply(lambda x: 'Рост' if x > 0 else ('Падение' if x < 0 else 'Нейтрально'))

	df_final = df_final.drop_duplicates(subset=['ЛЮМ', 'Баркод'])

	df_final[['Принимается', 'Остатки 1С', 'ЕДП', 'Забронировано к отгрузке',
		   'Средние продажи за 3 дня WB', 'Средние продажи за 3 дня Озон', 'Оборачиваемость']] = df_final[['Принимается', 'Остатки 1С', 'ЕДП', 'Забронировано к отгрузке',
		   'Средние продажи за 3 дня WB', 'Средние продажи за 3 дня Озон', 'Оборачиваемость']].fillna(0)

	df_final['Наименование основного склада'] = df_final['Наименование основного склада'].fillna('Отсутствует')

	df_final_wb = df_final[df_final['МП'] == 'WB']
	df_final_wb = df_final_wb.reset_index(drop=True)
	df_final_wb = df_final_wb.drop(columns=['Операция', 'Дата заказа', 'sku OZON', 'fbo_sku', 'nmId',
		   'Остаток МП: Озон', 'Зарезервированно Озон', 'Наименование Озон',
		   'Ваша цена Озон', 'ВП Озон', 'Картинка Озон', 'Средние продажи за 3 дня Озон', 'Кол-во продаж'])

	df_final_oz = df_final[df_final['МП'] == 'OZON']
	df_final_oz = df_final_oz.reset_index(drop=True)
	df_final_oz = df_final_oz.drop(columns=['Операция', 'Дата заказа', 'Код WB', 'nmId', 'Зарезервированно Озон', 'Остаток МП: ВБ', 'Наименование ВБ',
		   'Ваша цена WB', 'ВП WB', 'Картинка ВБ', 'Средние продажи за 3 дня WB', 'fbo_sku', 'Кол-во продаж'])
	#################################################

	############ Запись в эксель #############
	try:
		# ЧЕРЕЗ УДАЛЕНИЕ ЛИСТА
		with pd.ExcelWriter('/path/статки Ozon.xlsx') as writer_oz:
			df_final_oz.to_excel(writer_oz, sheet_name='Карточки', index=False, na_rep='NaN')
			writer_oz.close()

		with pd.ExcelWriter('/path/Остатки Wildberries.xlsx') as writer_wb:
			df_final_wb.to_excel(writer_wb, sheet_name='Карточки', index=False, na_rep='NaN')
			writer_wb.close()
			printstatuse('1 INF','Загрузка в NC прошла успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при загрузке в NC', str(e))
		quit()
	##########################################

	########## Отправка в уведомлений в Телеграмм ############
	message = f'''
	Общая выгрузка:\n
	OZON. Ссылка на выгрузку: \n
	https://link \n\n
	WILDBERRIES. Ссылка на выгрузку:\n
	https://link\n
	Так же каждому менеджеру на почту направлена выгрузка карточек только по его продуктам\n'''
	bot = tb.TeleBot(token='your_token')
	chat_id = 'your_chat_id'
	try:
		bot.send_message(chat_id, message)
		printstatuse('1 INF','Уведомление отправлено',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при отправке уведомления', str(e))
		quit()
	#####################################

	########################## Рассылка HTML контента на почту #############################
	df_email_send = df_final

	# Получение уникальных адресов электронной почты
	unique_emails = query_email_dictionary['email'].unique()

	try:
		creditionals = config["mail"]
		server = smtplib.SMTP_SSL(creditionals["ip"], creditionals["port"])
		sender_email = creditionals["user"]
		sender_password = creditionals["pass"]
		server.login(sender_email, sender_password)
		printstatuse('1 INF','Считывание конфига и авторизация SMPT успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при считывании конфига и авторизации SMPT', str(e))
		quit()

	html_content = ""
	try:
		# Цикл по уникальным адресам электронной почты
		for email in unique_emails:
			# Создание письма и установка получателя
			message = MIMEMultipart()
			message['From'] = sender_email
			message['To'] = email
			message['Subject'] = 'Остатки'

			# Итерация по строкам df_email_send
			for index, row in df_email_send.iterrows():
				if row['МП'] == 'WB':
					# Проверка на уникальность по столбцу 'last_name'. Каждый менеджер получает только свои данные
					if row['last_name'] == query_email_dictionary.loc[query_email_dictionary['email'] == email, 'last_name'].values[0]:
						# Текст письма
						html_content += f'''
						<img src="{row["Картинка ВБ"]}" width="20%" height="20%"><br> 
						<b>КМ: {row["КМ"]}</b><br>
						<b>МП: {row["МП"]}</b><br>
						<b>Бренд: {row["Бренд"]}</b><br>
						<b>Наименование ВБ: {row["Наименование ВБ"]}</b><br>
						<b>Артикул: {row["Код WB"]}</b><br>
						<b>ЛЮМ: {row["ЛЮМ"]}</b><br>
						<b>Главный склад по заказам: {row["Наименование основного склада"]}</b><br>
						<b>Склад: {row["Наименование склада"]}</b><br>
						<b>Ср заказы общие для карточки (за 3 дня): {row["Средние продажи за 3 дня WB"]}</b><br>
						<b>Оборачиваемость общая: {row["Оборачиваемость"]}</b><br>
						<b>Остаток 1с: {row["Остатки 1С"]}</b><br>
						<b>Остаток МП: ВБ: {row["Остаток МП: ВБ"]}</b><br>
						<b>На приемке: {row["Принимается"]}</b><br>
						<b>Ваша цена WB: {row["Ваша цена WB"]}</b><br>
						<b>ВП WB: {row["ВП WB"]}</b><br><br><br>
						'''
						
					elif row['МП'] == 'OZON':
						html_content += f'''
						<img src="{row["Картинка Озон"]}" width="20%" height="20%><br> 
						<b>КМ: {row["КМ"]}</b><br>
						<b>МП: {row["МП"]}</b><br>
						<b>Бренд: {row["Бренд"]}</b><br>
						<b>Наименование Озон: {row["Наименование Озон"]}</b><br>
						<b>Артикул: {row["sku OZON"]}</b><br>
						<b>ЛЮМ: {row["ЛЮМ"]}</b><br>
						<b>Главный склад по заказам: {row["Наименование основного склада"]}</b><br>
						<b>Склад: {row["Наименование склада"]}</b><br>
						<b>Ср заказы общие для карточки (за 3 дня): {row["Средние продажи за 3 дня Озон"]}</b><br>
						<b>Оборачиваемость общая: {row["Оборачиваемость"]}</b><br>
						<b>Остаток 1с: {row["Остатки 1С"]}</b><br>
						<b>Остаток МП: Озон: {row["Остаток МП: Озон"]}</b><br>
						<b>На приемке: {row["Принимается"]}</b><br>
						<b>Ваша цена Озон: {row["Ваша цена Озон"]}</b><br>
						<b>ВП Озон: {row["ВП Озон"]}</b><br><br><br>
						'''

			html_part = MIMEText(html_content, 'html', 'utf-8')
			# Заголовок Content-Type для HTML части
			html_part['Content-Transfer-Encoding'] = 'quoted-printable'
			html_part['Content-Language'] = 'ru'
			html_part['Content-Type'] = 'text/html; charset=utf-8'
			message.attach(html_part)
			
			 # Отправка письма
			server.sendmail(sender_email, email, message.as_string())
			html_content = ""
			##########################################################################
		printstatuse('1 INF','Письма отправлены успешно',"")
	except Exception as e:
		printstatuse('0 ERR','Ошибка при отправке писем', str(e))
		quit()

	server.quit()