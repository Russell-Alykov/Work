import pandas as pd
import os
# Считывание .env
from dotenv import load_dotenv
from sqlalchemy import create_engine
import sqlalchemy
# Google
import gspread
from df2gspread import df2gspread as d2g # заливает df в docs
from oauth2client.service_account import ServiceAccountCredentials # Авторизация

# Чтение переменных среды
load_dotenv('/Users/ra/WORK_sunshine/scripts_local/product_resluts_report/.env')
# SQL
base_db = os.getenv('BASE_DB')
ip = os.getenv('HOST')
port = os.getenv('PORT')
user = os.getenv('USER_DB')
password = os.getenv('PASS')
# Google
gs_product_dict = os.getenv('GS_PRODUCT_DICT')
gs_product_dict_sheet = os.getenv('GS_PRODUCT_DICT_SHEET')
gs_product_dict_sheet_2 = os.getenv('GS_PRODUCT_DICT_SHEET_2')
gs_site_import = os.getenv('GS_SITE_IMPORT')
gs_site_import_sheet = os.getenv('GS_SITE_IMPORT_SHEET')
gs_shopping = os.getenv('GS_SHOPPING')
gs_shopping_sheet = os.getenv('GS_SHOPPING_SHEET')
gs_search_sheet = os.getenv('GS_SEARCH_SHEET')
gs_facebook = os.getenv('GS_FACEBOOK')
gs_facebook_sheet = os.getenv('GS_FACEBOOK_SHEET')
gs_rnp_plan = os.getenv('GS_RNP_PLAN')
gs_rnp_plan_sheet = os.getenv('GS_RNP_PLAN_SHEET')

#Авторизация
gp = gspread.service_account(filename='/Users/ra/WORK_sunshine/scripts_local/product_resluts_report/credentials.json')

# Справочник товаров
sh = gp.open_by_url(gs_product_dict)
name_worksheet = gs_product_dict_sheet
worksheet = sh.worksheet(name_worksheet)
data = worksheet.get_all_values()
headers = data.pop(0)

def get_all_dict(name_worksheet):
    try:
        # Открываем Google Sheet по URL
        sh = gp.open_by_url(gs_product_dict)
        
        # Открываем лист по имени
        worksheet = sh.worksheet(name_worksheet)
        
        # Получаем все данные из листа
        data = worksheet.get_all_values()
        
        if not data:
            raise ValueError("Пустой лист данных")
        
        # Извлекаем заголовки и данные
        headers = data.pop(0)
        
        # Преобразуем данные в DataFrame
        df = pd.DataFrame(data, columns=headers)
        
        # Проверяем наличие нужных колонок
        required_columns = ['id', 'title', 'asin']
        if not all(column in df.columns for column in required_columns):
            raise ValueError(f"Ожидаемые колонки {required_columns} отсутствуют в данных")
        
        # Выбираем нужные колонки и заполняем отсутствующие значения
        df = df[required_columns]
        df['asin'] = df['asin'].fillna('Не определен')
        return df
    
    except Exception as e:
        print(f"Ошибка при работе с листом {name_worksheet}: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame при ошибке


def merge_dict_dataframes(df1, df2):
    # Объединяем два DataFrame на основе столбца 'id', при этом сохраняем все id из df1 и дополняем их из df2
    df_final = pd.merge(df1, df2, on='id', how='outer', suffixes=('', '_test'))
    
    # Если есть дублирующиеся колонки, выбираем данные из df1 в первую очередь
    df_final['title'] = df_final['title'].combine_first(df_final['title_test'])
    df_final['asin'] = df_final['asin'].combine_first(df_final['asin_test'])
    
    # Удаляем ненужные дублирующие колонки
    df_final = df_final.drop(columns=['title_test', 'asin_test'])
    
    return df_final

def get_all_dict(name_worksheet):
    try:
        # Открываем Google Sheet по URL
        sh = gp.open_by_url(gs_product_dict)
        
        # Открываем лист по имени
        worksheet = sh.worksheet(name_worksheet)
        
        # Получаем все данные из листа
        data = worksheet.get_all_values()
        
        if not data:
            raise ValueError("Пустой лист данных")
        
        # Извлекаем заголовки и данные
        headers = data.pop(0)
        
        # Преобразуем данные в DataFrame
        df = pd.DataFrame(data, columns=headers)
        
        # Проверяем наличие нужных колонок
        required_columns = ['id', 'title', 'asin', 'link']
        for column in required_columns:
            if column not in df.columns:
                print(f"Колонка {column} отсутствует. Добавление пустой колонки.")
                df[column] = ''  # Добавляем пустую колонку, если она отсутствует
        
        # Заполняем отсутствующие значения для 'asin' и 'link'
        df['asin'] = df['asin'].fillna('Не определен')
        df['link'] = df['link'].fillna('')  # Заполняем отсутствующие значения пустыми строками для 'link'
        
        # Возвращаем результат
        return df[required_columns]
    
    except Exception as e:
        print(f"Ошибка при работе с листом {name_worksheet}: {e}")
        return pd.DataFrame()  # Возвращаем пустой DataFrame при ошибке


def merge_dict_dataframes(df1, df2):
    # Объединяем два DataFrame на основе столбца 'id', при этом сохраняем все id из df1 и дополняем их из df2
    df_final = pd.merge(df1, df2, on='id', how='outer', suffixes=('', '_test'))
    
    # Если есть дублирующиеся колонки, выбираем данные из df1 в первую очередь
    df_final['title'] = df_final['title'].combine_first(df_final['title_test'])
    df_final['asin'] = df_final['asin'].combine_first(df_final['asin_test'])
    df_final['link'] = df_final['link'].combine_first(df_final['link_test'])
    
    # Удаляем ненужные дублирующие колонки
    df_final = df_final.drop(columns=['title_test', 'asin_test', 'link_test'])
    
    return df_final

# Используем функции для получения и объединения данных
df_dict = get_all_dict(gs_product_dict_sheet)
df_dict_test_products = get_all_dict(gs_product_dict_sheet_2)

# Объединяем данные
df_final_dict = merge_dict_dataframes(df_dict, df_dict_test_products)
print(df_final_dict)

#  Site Import orders
sh = gp.open_by_url(gs_site_import)
name_worksheet = gs_site_import_sheet
worksheet = sh.worksheet(name_worksheet)
records = worksheet.get_all_records()
df_site_import = pd.DataFrame(records)
df_site_import['Day'] = pd.to_datetime(df_site_import['Day'], format='%Y-%m-%d').dt.date
df_site_import = df_site_import.groupby('Day', as_index=False).agg({'Revenue': 'max', 'Visitors': 'max', 'Orders': 'max'})
df_site_import = df_site_import.drop_duplicates(subset=['Day', 'Revenue'])
print(df_site_import)

# Google Shopping
sh = gp.open_by_url(gs_shopping)
name_worksheet = gs_shopping_sheet
worksheet = sh.worksheet(name_worksheet)
records = worksheet.get_all_records()
df_g_shop = pd.DataFrame(records)
df_g_shop['metrics.value_per_conversion'] = df_g_shop['metrics.value_per_conversion'].replace({'': 0, ' ': 0})
df_g_shop['metrics.value_per_conversion'] = df_g_shop['metrics.value_per_conversion'].astype(float)
df_g_shop['Платформа'] = 'Google shopping'
print(df_g_shop)

# Google Search
name_worksheet = gs_search_sheet
worksheet = sh.worksheet(name_worksheet)
records = worksheet.get_all_records()
df_g_search = pd.DataFrame(records)
df_g_search['metrics.value_per_conversion'] = df_g_search['metrics.value_per_conversion'].replace({'': 0, ' ': 0})
df_g_search['metrics.value_per_conversion'] = df_g_search['metrics.value_per_conversion'].astype(float)
print(df_g_search)

# Facebook
sh = gp.open_by_url(gs_facebook)
name_worksheet = gs_facebook_sheet
worksheet = sh.worksheet(name_worksheet)
records = worksheet.get_all_records()
df_fcbk = pd.DataFrame(records)
print(df_fcbk.columns)

# Определяем столбцы для преобразования
columns_to_convert = [
    'CPM (Cost per 1,000 Impressions)', 
    'CPC (Cost per Link Click)',
    'Cost per Content View',
    'Cost per Add to Cart',
    'Cost per Checkout Initiated',
    'Cost per Purchase',
    'Amount Spent',
    'Purchases Conversion Value',
    'Purchases',
    'Reach',
    'Frequency',
    'Content Views',
    'Adds to Cart',
    'Link Clicks',
    'Checkouts Initiated'
]

# Очистка и преобразование данных
df_fcbk[columns_to_convert] = df_fcbk[columns_to_convert] \
    .replace(r'[\$,]', '', regex=True) \
    .replace('', 0) \
    .fillna(0) \
    .astype(float)

# Дополнительные обработки для отдельных столбцов
df_fcbk['Purchases'] = df_fcbk['Purchases'].replace({'': 0}).fillna(0).astype(int)
df_fcbk['Link Clicks'] = df_fcbk['Link Clicks'].replace({'': 0}).fillna(0).astype(float)
df_fcbk['Frequency'] = df_fcbk['Frequency'].replace({'': 0}).fillna(0).astype(float)
df_fcbk['Reach'] = df_fcbk['Reach'].replace({'': 0}).fillna(0).astype(float)
df_fcbk['CPC (All)'] = df_fcbk['CPC (All)'].replace({'': 0}).fillna(0).astype(float)
df_fcbk['CTR (Link Click-Through Rate)'] = df_fcbk['CTR (Link Click-Through Rate)'].replace({'': 0}).fillna(0).astype(float)

print(df_fcbk)

################################################
# Google Shopping
id_lst = df_g_shop['segments.product_item_id'].unique().tolist()
date_lst = df_g_shop['segments.date'].unique().tolist()
engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}')
db_connection = engine.connect()
id_check_list = ','.join("'" + str(id) + "'" for id in id_lst if id is not None and id != '' and id != ' ')
date_check_list = ','.join("'" + str(date) + "'" for date in date_lst if date is not None and date != '' and date != ' ')
query = f'DELETE FROM dwh.google_shopping_rnp WHERE "segments.product_item_id" IN ({id_check_list}) AND "segments.date" IN ({date_check_list})'
engine.execute(query)
################################################
# Google Search
campaign_lst = df_g_search['campaign.name'].unique().tolist()
date_lst = df_g_search['segments.date'].unique().tolist()
campaign_check_list = ','.join("'" + str(campaign) + "'" for campaign in campaign_lst if campaign is not None and campaign != '' and campaign != ' ')
date_check_list = ','.join("'" + str(date) + "'" for date in date_lst if date is not None and date != '' and date != ' ')
query = f'DELETE FROM dwh.google_search_rnp WHERE "campaign.name" IN ({campaign_check_list}) AND "segments.date" IN ({date_check_list})'
engine.execute(query)
################################################
# Site import
date_lst = df_site_import['Day'].unique().tolist()
revenue_lst = df_site_import['Revenue'].unique().tolist()
revenue_check_list = ','.join("'" + str(revenue) + "'" for revenue in revenue_lst if revenue is not None and revenue != '' and revenue != ' ')
date_check_list = ','.join("'" + str(date) + "'" for date in date_lst if date is not None and date != '' and date != ' ')
query = f'DELETE FROM dwh.site_import_rnp WHERE "Revenue" IN ({revenue_check_list}) AND "Day" IN ({date_check_list})'
engine.execute(query)
################################################
# Facebook
date_lst = df_fcbk['Day'].unique().tolist()
revenue_lst = df_fcbk['Purchases Conversion Value'].unique().tolist()
revenue_check_list = ','.join("'" + str(revenue) + "'" for revenue in revenue_lst if revenue is not None and revenue != '' and revenue != ' ')
date_check_list = ','.join("'" + str(date) + "'" for date in date_lst if date is not None and date != '' and date != ' ')
query = f'DELETE FROM dwh.facebook_rnp WHERE "Purchases Conversion Value" IN ({revenue_check_list}) AND "Day" IN ({date_check_list})'
engine.execute(query)
################################################
# Справочник
id_lst = df_final_dict['id'].unique().tolist()
title_lst = df_final_dict['title'].unique().tolist()
asin_lst = df_final_dict['asin'].unique().tolist()
id_check_list = ','.join("'" + str(id) + "'" for id in id_lst if id is not None and id.strip() != '')  # Проверка на пробелы
asin_check_list = ','.join("'" + str(asin) + "'" for asin in asin_lst if asin is not None and asin.strip() != '')  # Проверка на пробелы
query = f'DELETE FROM dwh.product_dict WHERE "id" IN ({id_check_list}) AND "asin" IN ({asin_check_list})'
engine.execute(query)
################################################
db_connection.close()

# Замена пустых значений на None (NaN) в числовых колонках
df_g_shop.replace('', None, inplace=True)
df_g_shop['metrics.ctr'] = df_g_shop['metrics.ctr'].replace('', None)

# Запись в БД dwh
engine = create_engine(f'postgresql://{user}:{password}@{ip}:{port}/{base_db}')
# dbConnection = engine.connect()
db_connection = engine.connect()
df_final_dict.to_sql('product_dict', schema = 'dwh', con=db_connection, if_exists='append', index = False)
df_site_import.to_sql('site_import_rnp', schema = 'dwh', con=db_connection, if_exists='append', index = False)
df_g_shop.to_sql('google_shopping_rnp', schema = 'dwh', con=db_connection, if_exists='append', index = False)
df_g_search.to_sql('google_search_rnp', schema = 'dwh', con=db_connection, if_exists='append', index = False)
df_fcbk.to_sql('facebook_rnp', schema = 'dwh', con=db_connection, if_exists='append', index = False)
# df_plan.to_sql('rnp_plan', schema = 'dwh', con=db_connection, if_exists='append', index = False)
db_connection.close()
print('Succsess')



