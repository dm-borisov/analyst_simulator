import telegram
import numpy as np
import pandahouse as ph
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import io

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Подключаем бота
token = '5425838340:AAHEHruR7Pe-nZRbAk8bxuMHp6D_Y97e1dg'
bot = telegram.Bot(token=token)
chat_id = -677113209

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230120'
}

default_args = {
    'owner': 'dm-borisov', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 3, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2023, 2, 11), # Дата начала выполнения DAG
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def borisov_report():
    # Задаем фон для графиков
    sns.set_theme()
    
    @task
    def extract_feed_yesterday():
        """
        Выгружает нужные метрики за предыдущий день
        
        Возвращает датафрейм с метриками
        """
        
        query = """
                SELECT toDate(time) date,
                       countIf(action='view') views,
                       countIf(action='like') likes,
                       likes / views ctr,
                       count(DISTINCT user_id) dau
                  FROM simulator_20230120.feed_actions
                 WHERE toDate(time) = yesterday()
                 GROUP BY toDate(time)
                """
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed        
    
    @task
    def extract_feed_week():
        """
        Выгружает нужные метрики за предыдущие 7 дней
        
        Возвращает датафрейм с метриками
        """
        
        query = """
                SELECT toDate(time) date,
                       countIf(action='view') views,
                       countIf(action='like') likes,
                       likes / views ctr,
                       count(DISTINCT user_id) dau
                  FROM simulator_20230120.feed_actions
                 WHERE toDate(time) BETWEEN yesterday() - 6
                                    AND yesterday()
                 GROUP BY toDate(time)
                """
        df_feed = ph.read_clickhouse(query, connection=connection)
        return df_feed

    @task
    def metrics_for_yesterday(feed):
        """
        Формирует текст с информацией о значениях ключевых метрик
        за предыдущий день

        Параметры
        ---------
        feed: ключевые метрики за предыдущий день
        
        Возвращает строку с ключевыми метриками        
        """
        
        
        text = f'''<b>Метрики за {feed.loc[0, "date"].strftime("%Y-%m-%d")}:</b>
{"dau:": <8} {feed.loc[0, 'dau']}
{"views:": <7} {feed.loc[0, 'views']}
{"likes:": <9} {feed.loc[0, 'likes']}
{"ctr:": <10} {feed.loc[0, 'ctr']:.2%}
'''
        return text

    @task
    def metrics_for_previous_days(feed):
        """
        Создает график со значениями метрик
        за предыдущие 7 дней
    
        Параметры
        ---------
        feed: датафрейм с ключевыми метриками
        
        Возвращает график с четыремя ключевыми метриками    
        """
        
        # Создаем четыре графика
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15,15))
        axes_flatten = axes.flatten()

        fig.suptitle('Значения метрик за предыдущие 7 дней', fontsize=32, fontweight="bold")

        # Рисуем каждый график
        for ax, metric in zip(axes_flatten, feed.columns[1:]):
            ax.set_title(metric, fontsize=18)
            ax.tick_params(axis='x', labelrotation=30, labelsize=10)
            ax.tick_params(axis='y', labelsize=10)
            ax.set_xlabel('date', fontsize=14)
            ax.set_ylabel('cnt', fontsize=14)
            sns.lineplot(data=feed, x='date', y=metric, ax=ax, linewidth=3)
    
        # Добавляем место между графиками
        plt.subplots_adjust(wspace = 0.20, hspace=0.25)
    
        # Сохраняем графики в буфере
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'
        plt.close()

        return plot_object  

    @task
    def send_to_chat(text, charts):
        """
        Посылает текстовую информацию и графики
        в группу
        
        Параметры
        ---------
        text: текстовая информация по ключевым метрикам
        charts: графики по ключевым метрикам
 
        Ничего не возвращает
        """
    
        bot.sendMessage(chat_id=chat_id, text=text, parse_mode='HTML')
        bot.sendPhoto(chat_id=chat_id, photo=charts)

    df_yesterday = extract_feed_yesterday()
    df_week = extract_feed_week()
    text = metrics_for_yesterday(df_yesterday)
    charts = metrics_for_previous_days(df_week)
    send_to_chat(text, charts)

borisov_report_1 = borisov_report()