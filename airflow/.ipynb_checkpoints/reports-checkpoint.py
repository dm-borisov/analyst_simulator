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
token = '5980888909:AAGvALMOpNB_fqfDYvu2kq3aEWSdqerm_IY'
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
    
    @task
    def extract_feed():
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
        
        yesterday_feed = df_feed.iloc[-1]
        
        text = f'''Метрики за {yesterday_feed['date'].strftime("%Y-%m-%d")}:
{"dau:": <8} {yesterday_feed['dau']}
{"views:": <7} {yesterday_feed['views']}
{"likes:": <9} {yesterday_feed['likes']}
{"ctr:": <10} {yesterday_feed['ctr']:.2%}
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
        fig, axes = plt.subplots(nrows=2, ncols=2)
        axes_flatten = axes.flatten()
        # Увеличиваем размер графиков
        sns.set(rc={'figure.figsize':(12,12)})

        fig.suptitle('Значения метрик за предыдущие 7 дней', fontsize=16)

        # Рисуем каждый график
        for ax, metric in zip(axes_flatten, df_metrics_week):
            ax.set_title(metric)
            ax.tick_params(axis='x', labelrotation=45)
            sns.lineplot(data=df_metrics_week, x='date', y=metric, ax=ax)
    
        # Добавляем место между графиками
        plt.subplots_adjust(wspace = 0.30, hspace=0.40)
    
        # Сохраняем графики в буфере
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics'
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
    
        bot.sendMessage(chat_id=chat_id, text=text)
        bot.sendPhoto(chat_id=chat_id, photo=charts)

    df_feed = extract_feed()
    text = metrics_for_yesterday(df_feed)
    charts = metrics_for_previous_days(df_feed)
    send_to_chat(text, charts)

borisov_report()