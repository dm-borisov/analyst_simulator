import telegram
import numpy as np
from datetime import datetime, date, timedelta
import pandahouse as ph
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as md
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Подключаем бота
token = '6151637821:AAGitRfkYQqN51NQFIgaFOk0LSSYRykG_a8'
bot = telegram.Bot(token=token)
chat_id = -520311152

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
    'start_date': datetime(2023, 2, 16), # Дата начала выполнения DAG
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def borisov_alert():
    # Задаем фон для графиков
    sns.set()
    
    def iqr_method(df_entry, metric, periods=4, width=1.5):
        """
        Вычисляет доверительный интервал с помощью квартилей
        и проверят выходит ли текущее значения за границы интервала
        
        Параметры
        ---------
        df_entry: датафрейм с метрикой
        metric: интересующая метрика
        periods: количество периодов (по умолчанию 4)
        width: коэффициент, определяющий ширину интервала (по умолчанию 1.5)
        
        Возвращает проверку на выход из интервала, текущее значение, нижнюю
        и верхнюю границы, а также датафрейм с вычислениями 
        """
        
        # копируем датафрейм
        df = df_entry.copy()
        
        df['previous_value'] = df[metric].shift(1) # смещаем значения на одну позицию
        df['quantile25'] = df['previous_value'].rolling(periods).quantile(0.25) # считаем 1 квартиль
        df['quantile75'] = df['previous_value'].rolling(periods).quantile(0.75) # считаем 3 квартиль 
        df['iqr'] = df['quantile75'] - df['quantile25'] # считаем межквартильный размах
        df['low'] = df['quantile25'] - width * df['iqr'] # считаем нижнюю границу доверительного интервала
        df['high'] = df['quantile75'] + width * df['iqr'] # считаем верхнюю границу доверительного интервала
        df['low'] = df['low'].rolling(periods, center=True, min_periods=1).mean() # сглаживаем нижнюю границу
        df['high'] = df['high'].rolling(periods, center=True, min_periods=1).mean() # сглаживаем верхнюю границу
        
        is_alert = not df.low.iloc[-1] <= df[metric].iloc[-1] <= df.high.iloc[-1] # проверяем на выход из интервала
    
        return is_alert, df[metric].iloc[-1], df.low.iloc[-1], df.high.iloc[-1], df
    
    @task
    def extract_feed_ios():
        """
        Выгружает из базы ключевые метрики Ленты для проверки
        на аномалии за сегодня, и за вчерашний день с интервалом
        в 15 минут
        
        Возвращает датафереймы по срезу iOS
        """
        
        query = """
        SELECT toStartOfFifteenMinutes(time) ts,
               uniqExact(user_id) active_users,
               countIf(action='like') likes,
               countIf(action='view') views,
               likes / views ctr
          FROM simulator_20230120.feed_actions
         WHERE ts >= yesterday()
               AND ts < toStartOfFifteenMinutes(now())
               AND os = 'iOS'
         GROUP BY ts
         ORDER BY ts
        """
        
        # Выполняем запрос
        df_feed_ios = ph.read_clickhouse(query, connection=connection)
        return df_feed_ios

    @task
    def extract_feed_android():
        """
        Выгружает из базы ключевые метрики Ленты для проверки
        на аномалии за сегодня, и за вчерашний день с интервалом
        в 15 минут
        
        Возвращает датафереймы по срезу Android
        """
        
        query = """
        SELECT toStartOfFifteenMinutes(time) ts,
               uniqExact(user_id) active_users,
               countIf(action='like') likes,
               countIf(action='view') views,
               likes / views ctr
          FROM simulator_20230120.feed_actions
         WHERE ts >= yesterday()
               AND ts < toStartOfFifteenMinutes(now())
               AND os = 'Android'
         GROUP BY ts
         ORDER BY ts
        """
        
        # Выполняем запрос
        df_feed_android = ph.read_clickhouse(query, connection=connection)
        return df_feed_android
    
    @task
    def extract_messenger():
        """
        Выгружает из базы ключевые метрики Мессенджера для проверки
        на аномалии за сегодня, и за вчерашний день с интервалом
        в 15 минут
        
        Возвращает датаферейм с этими метриками
        """
        
        query = """
        SELECT toStartOfFifteenMinutes(time) ts,
               uniqExact(user_id) active_users,
               count(user_id) msg_sent
          FROM simulator_20230120.message_actions
         WHERE ts >= yesterday()
               AND ts < toStartOfFifteenMinutes(now())
         GROUP BY ts
         ORDER BY ts
        """
        
        # Выполняем запрос
        df_messenger = ph.read_clickhouse(query, connection=connection)
        
        return df_messenger
    
    @task
    def run_alerts(name, df):
        """
        Проверяет каждую метрику датафрейма на аномалии.
        В случае ее нахождения, посылат в телеграм отчет
        
        Параметры:
        name: имя среза/сервиса
        df: датафрейм с метриками
        
        Ничего не возвращает
        """
        
        # Создаем шаблоны сообщений для ленты и мессенджера
        msg_feed = """
Лента:
Метрика {0} в срезе {1}.
Текущее значение {2:.2f} не вошло в доверительный интервал ({3:.2f}, {4:.2f}).        
"""
        msg_messenger = """
Мессенджер:
Метрика {0}.
Текущее значение {1:.2f} не вошло в доверительный интервал ({2:.2f}, {3:.2f}).       
"""
        # Проходим по каждой метрике из датафрейма
        for metric in df.columns[1:]:
            
            # Выполняем проверку реализованным методом
            is_alert, current_value, low, high, result = iqr_method(df[['ts', metric]], metric, periods=5, width=3)
            
            # Отсекаем данные за вчерашний день
            result = result[result.ts.dt.strftime("%Y-%m-%d") == datetime.today().strftime("%Y-%m-%d")]
            
            if is_alert:
                # Выбираем нужный шаблон
                if name == 'messenger':
                    text = msg_messenger.format(metric, current_value, low, high)
                else:
                    text = msg_feed.format(metric, name, current_value, low, high)
                
                # Создаем график
                fig, ax = plt.subplots(figsize=(10, 10))
                ax = sns.lineplot(data=result, x='ts', y=metric) # линейчатый график метрики
                ax.fill_between(x=result['ts'], y1=result['low'], y2=result['high'], alpha=.3) # доверительный интервал
                
                # Форматируем даты для оси x
                ax.xaxis.set_major_formatter(md.DateFormatter('%H:%M'))
                
                # Настраиваем остальные часть графика
                ax.set(xlabel='time',
                       ylabel=metric,
                       title=metric,
                       ylim=(0, None))
                
                # Сохраняем график в буфере
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = name + "_" + metric + '.png'
                plt.close()
                
                # Отправляем сообщение и график в чат
                bot.sendMessage(chat_id=chat_id, text=text)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    # Запускает систему алертов
    df_feed_android = extract_feed_android()
    df_feed_ios = extract_feed_ios()
    df_messenger = extract_messenger()
    run_alerts('android', df_feed_android)
    run_alerts('ios', df_feed_ios)
    run_alerts('messenger', df_messenger)
    
borisov_alert = borisov_alert()