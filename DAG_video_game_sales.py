#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from datetime import timedelta
from datetime import datetime
import textwrap
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

# Настройка уведомлений об успешном выполнении DAG (через tg-бот)
def send_telegram_message(context):
    try:
        # Токен бота и id чата скрыты в коде, но вы можете использовать своего бота
        # Переменные BOT_TOKEN и CHAT_ID заданы в Airflow -> Admin -> Variables
        BOT_TOKEN = Variable.get('BOT_TOKEN')
        CHAT_ID = Variable.get('CHAT_ID')
        
        dag_id = context['dag'].dag_id
        date = context['ds']
        execution_date = context['dag_run'].execution_date
        bot_message = f'''
✅ <b>DAG выполнен успешно!</b>

📊 DAG: {dag_id}
📅 Дата выполнения: {date}
🕒 Время запуска: {execution_date}

Все задачи завершены успешно! 🎉'''
        
        # Формируем URL для API Telegram
        url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
        
        # Данные для отправки
        data = {'chat_id': CHAT_ID,
                'text': bot_message,
                'parse_mode': 'HTML'}
        
        # Отправляем запрос
        response = requests.post(url, json=data, timeout=30)
        
        # Проверяем ответ
        if response.status_code == 200:
            print('✅ Уведомление отправлено в Telegram')
            return True
        else:
            print(f'❌ Ошибка Telegram API: {response.status_code} - {response.text}')
            return False
            
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")
        return False


link = 'https://kc-course-static.hb.ru-msk.vkcs.cloud/startda/Video%20Game%20Sales.csv'
login = 'alisa-bulgakova-phl7749'
year = 1994 + hash(f'{login}') % 23

# Задаем аргументы DAGа
default_args = {
    'owner': 'alisa-bulgakova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 9, 24),
}
        

# Создаем DAG
@dag(dag_id='Dag_with_bot', 
     default_args=default_args,
     schedule_interval='0 12 * * *',
     catchup=False,
     on_success_callback=send_telegram_message)

# Запускаем DAG
def my_dag():
    
    # Считываем данные и оставляем только один год (year)
    @task()
    def get_game_data():
        game_data = pd.read_csv(link)
        game_data_year = game_data[game_data.Year == year]
        if game_data_year.empty: # Проверка на пустой df
            raise ValueError(f'Нет данных за {year} год')
        return game_data_year
    
    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def best_selling_games(game_data_year):
        max_sales = game_data_year.groupby('Name').Global_Sales.sum().sort_values(ascending=False).index[0]
        return max_sales
    
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task()
    def eu_best_selling_genres(game_data_year):
        eu_max_sales = game_data_year.groupby('Genre').sum().nlargest(1, 'EU_Sales').index[0]
        return eu_max_sales
    
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    # Перечислить все, если их несколько
    @task()
    def na_best_selling_platforms(game_data_year):
        # Фильтруем игры с продажами более 1 млн в NA
        na_games = game_data_year[game_data_year['NA_Sales'] > 1]
        # Если нет игр, удовлетворяющих условию
        if na_games.empty:
            return 'Нет таких игр в Северной Америке'
        # Считаем количество игр по платформам
        platforms_count = na_games.groupby('Platform').count()
        max_count = platforms_count.max()
        best_platforms = platforms_count[platforms_count == max_count].index
        platforms_na = ', '.join(best_platforms)
        return platforms_na

    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def jp_best_selling_publishers(game_data_year):
        # Группируем по издателям и считаем средние продажи
        jp_publishers_mean = game_data_year.groupby('Publisher').JP_Sales.mean()
        max_mean = jp_publishers_mean.max()
        best_publishers = jp_publishers_mean[jp_publishers_mean == max_mean].index
        jp_publishers = ', '.join(best_publishers)
        return jp_publishers

    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def eu_more_than_jp_games(game_data_year):
        eu_jp_games = game_data_year.groupby('Name').agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        better_in_eu = eu_jp_games[eu_jp_games['EU_Sales'] > eu_jp_games['JP_Sales']].shape[0]
        return better_in_eu

    # Вывод отчета
    @task()
    def print_info(max_sales, eu_max_sales, platforms_na, jp_publishers, better_in_eu):
        context = get_current_context()
        date = context['ds']
        dag_id = context['dag'].dag_id
        # Текст отчета
        message = textwrap.dedent(f'''
        Результаты выполнения DAG {dag_id} на {date}.
        Отчет за {year} год:
        - Самая продаваемая игра в мире: {max_sales}
        - Самые продаваемые жанры в Европе: {eu_max_sales}
        - Платформы с самыми продаваемыми играми в Северной Америке: {platforms_na}
        - Издатели с самыми высокими средними продажами в Японии: {jp_publishers}
        - Количество игр, проданных лучше в Европе, чем в Японии: {better_in_eu}''') 
        
        print(message)
        
    game_data_year = get_game_data()
    max_sales = best_selling_games(game_data_year)
    eu_max_sales = eu_best_selling_genres(game_data_year)
    platforms_na = na_best_selling_platforms(game_data_year)
    jp_publishers = jp_best_selling_publishers(game_data_year)
    better_in_eu = eu_more_than_jp_games(game_data_year)
    print_info(max_sales, eu_max_sales, platforms_na, jp_publishers, better_in_eu)
    
dag = my_dag()

