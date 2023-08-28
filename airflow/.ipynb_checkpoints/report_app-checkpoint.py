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
    'start_date': datetime(2023, 2, 14), # Дата начала выполнения DAG
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def borisov_report_app():
    # Задаем фон для графиков
    sns.set_theme()
    
    # Декоратор для сохранения графиков в буфер
    def save_plots(plot_func):
        def inner_func(df, name):
            fig = plot_func(df)
        
            # Сохраняем графики в буфере
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = name
            plt.close()
        
            return plot_object
        return inner_func
    
    @task
    def extract_metrics():
        """
        Выгружает из базы данных ключевые метрики и
        метрики на пользователя
        
        Возвращает датафрейм с метриками
        """
        
        query = """
          WITH feed_metrics AS (
        SELECT toDate(time) date,
               countIf(action='like') likes,
               countIf(action='view') views,
               likes / views ctr,
               countIf(action='like') / countIf(distinct user_id, action='like') likes_per_user,
               countIf(action='view') / countIf(distinct user_id, action='view') views_per_user
          FROM simulator_20230120.feed_actions
         WHERE toDate(time) BETWEEN yesterday() - 6
                            AND yesterday()
         GROUP BY toDate(time)),
               message_metrics AS (
        SELECT toDate(time) date,
               count(user_id) messages_sent,
               count(user_id) / count(distinct user_id) messages_sent_per_user,
               count(reciever_id) / count(distinct reciever_id) messages_recieved_per_user
          FROM simulator_20230120.message_actions
         WHERE toDate(time) BETWEEN yesterday() - 6
                            AND yesterday()
         GROUP BY toDate(time))
        SELECT fm.date,
               fm.likes,
               fm.views,
               fm.ctr,
               mm.messages_sent,
               fm.likes_per_user,
               fm.views_per_user,
               mm.messages_sent_per_user,
               mm.messages_recieved_per_user
          FROM feed_metrics fm
               INNER JOIN message_metrics mm ON fm.date = mm.date
        """
        
        df_metrics = ph.read_clickhouse(query, connection=connection)
        return df_metrics
    
    @task
    def extract_dau():
        """
        Выгружает из базы данных dau для пользователей, которые
        пользуются только лентой, лентой и мессенджером, только мессенджером
        
        Возвращает датафрейм с dau
        
        Самая долгий запрос из всех, но я не нашел способа его ускорить
        """
        
        query = """
          WITH user_service AS (
        SELECT DISTINCT if(fa.user_id != 0, fa.user_id, ma.user_id) user_id,
               multiIf(fa.user_id = ma.user_id, 'both',
                       fa.user_id = 0, 'message',
                       'feed') service
          FROM simulator_20230120.feed_actions fa
               FULL JOIN simulator_20230120.message_actions ma ON fa.user_id = ma.user_id),
               users_joined AS (
        SELECT if(fa.user_id != 0, fa.user_id, ma.user_id) user_id,
               toDate(if(fa.user_id != 0, fa.time, ma.time)) date
          FROM simulator_20230120.feed_actions fa
               FULL JOIN simulator_20230120.message_actions ma ON fa.user_id = ma.user_id
                                                               AND toDate(fa.time) = toDate(ma.time)
         WHERE toDate(if(fa.user_id != 0, fa.time, ma.time)) BETWEEN yesterday() - 6
                                                             AND yesterday())
        SELECT date,
               countIf(distinct uj.user_id, us.service = 'both') dau_both,
               countIf(distinct uj.user_id, us.service = 'feed') dau_feed,
               countIf(distinct uj.user_id, us.service = 'message') dau_message
          FROM users_joined uj
               INNER JOIN user_service us ON uj.user_id = us.user_id
         GROUP BY date
         """
        df_dau = ph.read_clickhouse(query, connection=connection)
        return df_dau
    
    @task
    def extract_decomposition():
        """
        Выгружает из базы данных декомпозицию пользователей по дням
        
        Возвращает датафрейм с декомпозицией
        """
        
        query = """
          WITH user_day AS (
        SELECT DISTINCT if(fa.user_id != 0, fa.user_id, ma.user_id) user_id,
                        toDate(if(fa.user_id !=0, fa.time, ma.time)) date
          FROM simulator_20230120.feed_actions fa
               FULL JOIN simulator_20230120.message_actions ma ON fa.user_id = ma.user_id
                                                                  AND toDate(fa.time) = toDate(ma.time)
         WHERE toDate(if(fa.user_id !=0, fa.time, ma.time)) BETWEEN yesterday() - 7
                                                            AND yesterday()),
               new_retained AS (
        SELECT ud1.date date,
               SUM(if(ud1.date = ud2.date, 1, 0)) dau,
               SUM(if(subtractDays(ud1.date, 1) = ud2.date, 1, 0)) retain
          FROM user_day ud1
               INNER JOIN user_day ud2 ON ud1.user_id = ud2.user_id
         GROUP BY ud1.date),
               all_groups AS (
        SELECT date,
               dau - retain new,
               retain,
               lagInFrame(dau, 1) OVER(ORDER BY date) previous_dau
          FROM new_retained
         ORDER BY date)
        SELECT date,
               new,
               retain,
               retain - previous_dau gone
          FROM all_groups
         WHERE date >= yesterday() - 6
        """

        df_decomposition = ph.read_clickhouse(query, connection=connection)
        return df_decomposition
    
    @task
    def extract_usage():
        """
        Выгружает из базы данных пользователей, которые
        стали пользоваться обоими сервисами
        
        Возвращает датафрейм с пользователями
        """
        
        query = """
          WITH feed_first_entry AS (
        SELECT user_id,
               MIN(toDate(time)) first_entry
          FROM simulator_20230120.feed_actions
         GROUP BY user_id),
               message_first_entry AS (
        SELECT user_id,
               MIN(toDate(time)) first_entry
         FROM simulator_20230120.message_actions
        GROUP BY user_id)
       SELECT if(ffe.first_entry > mfe.first_entry, ffe.first_entry, mfe.first_entry) date,
              COUNT(ffe.user_id) user_cnt
         FROM feed_first_entry ffe
              INNER JOIN message_first_entry mfe ON ffe.user_id = mfe.user_id
        WHERE ffe.first_entry = yesterday()
              OR mfe.first_entry BETWEEN yesterday() - 6
                                 AND yesterday()
        GROUP BY if(ffe.first_entry > mfe.first_entry, ffe.first_entry, mfe.first_entry)
        """
        
        df_usage = ph.read_clickhouse(query, connection=connection)
        return df_usage
    
    @task
    def make_main_metrics_yesterday_text(df):
        """
        Создает сообщения о ключевых метриках за вчерашний день
        
        Параметры
        ---------
        df: датафрейм с ключевыми метриками
        
        Возвращает строку с ключевыми метриками
        """
        df_yesterday = df.iloc[-1]
        
        text_main = f'''<b>Метрики за {df_yesterday[0].strftime("%Y-%m-%d")}:</b>
likes: {df_yesterday[1]}
views: {df_yesterday[2]}
ctr: {df_yesterday[3]:.2%}
msg_sent: {df_yesterday[4]}
        '''
        
        return text_main
    
    @task
    @save_plots
    def make_main_metrics_graphs(df):
        """
        Создает графики ключевых метрик за предыдущие 7 дней
        
        Параметры
        ---------
        df: график с ключевыми метриками
        
        Возвращает графики для лайков, просмотров, ctr и отправленных
        сообщений
        """
        
        # Создаем четыре графика
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 15))
        axes_flatten = axes.flatten()
            
        fig.suptitle('Ключевые метрики за предыдущие 7 дней', fontsize=32, fontweight="bold")

        # Рисуем каждый график
        for ax, metric in zip(axes_flatten, df.columns[1:5]):
            ax.set_title(metric, fontsize=18)
            ax.tick_params(axis='x', labelrotation=30, labelsize=10)
            ax.tick_params(axis='y', labelsize=10)
            ax.set_xlabel('date', fontsize=14)
            ax.set_ylabel('cnt', fontsize=14)
            sns.lineplot(data=df, x='date', y=metric, ax=ax, linewidth=3)
    
        # Добавляем место между графиками
        plt.subplots_adjust(wspace = 0.20, hspace=0.25)

        return fig
    
    @task
    @save_plots
    def make_dau_graphs(df):
        """
        Создает графики для dau за предыдущие 7 дней
        
        Параметры
        ---------
        df: датафрейм с dau
        
        Возвращает графики dau для пользователей, пользующихся разными сервисами
        """
        
        fig, (ax1, ax2) = plt.subplots(ncols=2, figsize=(15, 10))

        fig.suptitle('DAU за предыдущие 7 дней', fontsize=32, fontweight="bold")
        
        # график dau для пользователей, которые пользуются и лентой, и мессенджером
        ax1.set_title('И лентой, и мессенджером', fontsize=18)
        ax1.tick_params(axis='x', labelrotation=30, labelsize=10)
        ax1.tick_params(axis='y', labelsize=10)
        ax1.set_xlabel('date', fontsize=14)
        ax1.set_ylabel('active_users', fontsize=14)
        sns.lineplot(data=df, x='date', y='dau_both', ax=ax1, linewidth=3)

        # график dau для пользователей, которые пользуются только лентой
        ax2.set_title('Только лентой', fontsize=18)
        ax2.tick_params(axis='x', labelrotation=30, labelsize=10)
        ax2.tick_params(axis='y', labelsize=10)
        ax2.set_xlabel('date', fontsize=14)
        ax2.set_ylabel('active_users', fontsize=14)
        sns.lineplot(data=df, x='date', y='dau_feed', ax=ax2, linewidth=3)
        
        return fig
    
    @task
    @save_plots
    def make_metrics_per_user_graphs(df):
        """
        Создает графики для метрик на пользователя
        
        Параметры:
        df: датафрейм с ключевыми метриками
        
        Возвращает графики о лайках, просмотрах, сообщений на пользователя
        """
        
        # Создаем четыре графика
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 15))
        axes_flatten = axes.flatten()

        fig.suptitle('Метрики на пользователя за предыдущие 7 дней', fontsize=32, fontweight="bold")

        # Рисуем каждый график
        for ax, metric in zip(axes_flatten, df.columns[5:]):
            ax.set_title(metric, fontsize=18)
            ax.tick_params(axis='x', labelrotation=30, labelsize=10)
            ax.tick_params(axis='y', labelsize=10)
            ax.set_xlabel('date', fontsize=14)
            ax.set_ylabel('cnt', fontsize=14)
            sns.lineplot(data=df, x='date', y=metric, ax=ax, linewidth=3)
    
        # Добавляем место между графиками
        plt.subplots_adjust(wspace = 0.20, hspace=0.25)
        
        return fig
    
    @task
    @save_plots
    def make_decomposition_graph(df):
        """
        Создает график декомпозиции пользователей за последние 7 дней
    
        Параметры
        ---------
        df: датафрейм с декомпозицией
    
        Возвращает график декомпозиции пользователей
        """
    
        fig, ax = plt.subplots(figsize=(12, 12))

        fig.suptitle('Декомпозиция пользователей', fontsize=32, fontweight="bold")

        ax.bar(df['date'], df['retain'], color='#734C8F', label='retain')
        ax.bar(df['date'], df['new'], bottom=df['retain'], color='#4B956F', label='new')
        ax.bar(df['date'], df['gone'], color='#D4A36A', label='gone')
        ax.set_xlabel('date', fontsize=14)
        ax.set_ylabel('cnt', fontsize=14)

        ax.legend()
        
        return fig
    
    @task
    @save_plots
    def make_usage_graph(df):
        """
        Создает график пользователей, который в определенный день
        стали пользоваться обоими сервисами
        
        Параметры
        ---------
        df: датафрейм с пользователями
        
        Возвращает график пользователей, которые стали пользоваться
        обоими сервисами
        """
        
        fig, ax = plt.subplots(figsize=(15, 15))
        fig.suptitle('Пользователи, начавшие использовать оба сервиса', fontsize=32, fontweight="bold")

        ax.tick_params(axis='x', labelsize=12)
        ax.tick_params(axis='y', labelsize=12)
        ax.set_xlabel('date', fontsize=14)
        ax.set_ylabel('cnt', fontsize=14)
        sns.lineplot(data=df, x='date', y='user_cnt', linewidth=3)
 
        return fig
    
    @task
    def send_to_bot(metrics_yesterday, metrics_graphs, dau_graphs,
                    per_user_graphs, decomposition_graph, usage_graph):
        """
        Отправляет сообщение и графики в чат
        
        Параметры
        ---------
        metrics_yesterday: текст с ключевыми метриками за вчерашний день
        metrics_graphs: графики с ключевыми метриками
        dau_graphs: графики с dau
        per_user_graphs: графики с метриками на пользователя
        decomposition_graph: график с декомпозицией пользователей
        usage_graph: график с использованием обоих сервисов
        
        Ничего не возвращает        
        """
        
        bot.sendMessage(chat_id=chat_id, text=metrics_yesterday, parse_mode='HTML')
        bot.sendPhoto(chat_id=chat_id, photo=metrics_graphs)
        bot.sendPhoto(chat_id=chat_id, photo=dau_graphs)
        bot.sendPhoto(chat_id=chat_id, photo=per_user_graphs)
        bot.sendPhoto(chat_id=chat_id, photo=decomposition_graph)
        bot.sendPhoto(chat_id=chat_id, photo=usage_graph)
    
    # Создаем датафремы для метрик
    df_metrics = extract_metrics()
    df_dau = extract_dau()
    df_decomposition = extract_decomposition()
    df_usage = extract_usage()
    
    # Создаем графики и сообщения
    main_metrics_yesterday_text = make_main_metrics_yesterday_text(df_metrics)
    main_metrics_graphs = make_main_metrics_graphs(df_metrics, 'metrics.png')
    dau_graphs = make_dau_graphs(df_dau, 'dau.png')
    metrics_per_user_graphs = make_metrics_per_user_graphs(df_metrics, 'per_user.png')
    decomposition_graph = make_decomposition_graph(df_decomposition, 'decomposition.png')
    usage_graph = make_usage_graph(df_usage, 'usage.png')
    
    # Отправляем отчет в телеграм
    send_to_bot(main_metrics_yesterday_text,
                main_metrics_graphs,
                dau_graphs,
                metrics_per_user_graphs,
                decomposition_graph,
                usage_graph)

borisov_report_app = borisov_report_app()