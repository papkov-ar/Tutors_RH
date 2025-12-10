import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import urllib3
from clickhouse_driver import Client
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.exceptions import AirflowSkipException
from airflow.models.variable import Variable
import logging
from telegram import Bot
import asyncio

urllib3.disable_warnings()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


# Функция извлечения объектов из ячеек представляющих собой список словарей
def extract_values(row, key):
    if isinstance(row, list):
        if key == 'name':
            separator = ','  # Разделитель для ключа 'name'
        elif key == 'url':
            separator = ' '  # Разделитель для ключа 'url'
        else:
            return ''
        return separator.join([d[key] for d in row if key in d]) # Извлекаю значения по ключу и объединяю с нужным разделителем
    return ''

# Функция обновления столбца по окну
def update_column(frame, window_column, target_column, key_column, question_value, answer_column):
    """Обновляет значения в target_column с помощью answer_column
    вдоль окна значений из window_column, если в key_column найдено значение question_value.
    Аргументы:
        frame: Df для обновления.
        window_column: Столбец, значения которого определяют окно для функции
        target_column: Столбе, в котором будут обновления
        key_column: Столбец, из которого берется ключ
        question_value: Значение в столбце key_column, т.е. ключ
        answer_column: Столбец, из которого берется значение для обновления
    Возвращает:
        Df с обновленными значениями.
    """
    def update_value(x, value):
        try:
            return x.loc[frame[key_column] == value].iloc[0]
        except IndexError:
            return frame[target_column]
    frame[target_column] = frame.\
                        groupby([window_column])[answer_column].\
                        transform(lambda x: update_value(x, question_value)).\
                        fillna(frame[target_column])
    return frame

# Функция конвертации типов данных
def convert_column_type(column):
    if column.dtype   == 'float64':
        return column.astype('float64')
    elif column.dtype == 'int64':
        return column.astype('int64')
    elif column.dtype == 'O':
        return column.astype(str)
    elif column.dtype == '<M8[ns]':
        return column.astype(str)


# Функции для отправки уведомлений в Телегу
async def send_tg_async(message):
    token = # телеграмм токен
    chat_id = # Id группы с коллегами
    thread_id = # id темы с в группе
    bot = Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=message, message_thread_id = thread_id)
def send_tg(message):
    asyncio.run(send_tg_async(message))

    
# Параметры для get 
api_key   = Variable.get("TUTORS_TOKEN")
url       = # URL приложения для проведения проверок
headers   = {'API-Key': api_key,
             'accept': 'application/json',
             'charset': 'utf-8'
             }
# ========================================================

# Дефолт дага    
default_args = {
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 25)
    } 

@dag(default_args=default_args, schedule_interval= '6 0,17 * * *', catchup=False) # Декоратор дага
def uploading_data_tutors_to_ch(): # Функция дага
    clickhouse_connection = Variable.get('CLICKHOUSE_ON_PREMISE_AUTHORIZATION', deserialize_json=True)
    def get_polls(yesterday): # Запрашиваю список заполненных анкет, у которых были какие-то изменения вчера
        query     = {'statuses[]': ['completed'],
                     'updated_at_from': yesterday,
                     'page': 1,
                     'per_page': 50
                    }
        all_data  = []
        retry_count = 0
        try:
            while True:
                        response = requests.get(url, headers=headers, params=query, verify=False)
                        if response.status_code == 200:
                            data = response.json()['data']
                            if not data:
                                break
                            if response.json()['last_page'] < response.json()['current_page']:
                                break
                            all_data.extend(data)
                            query["page"] += 1
                            retry_count = 0
                        else:
                            retry_count += 1
                            if retry_count >= 3:
                                log.error(f"Ошибка подключения для получения id анкет. {response.status_code}: {response.text}")
                                send_tg(f"Наставники. ❌ Ошибка подключения для получения id анкет. {response.status_code}: {response.text}")
                                break
            list_polls = list(pd.json_normalize(all_data).id)
            log.info(f"Получено {len(list_polls)} анкет от вчерашнего дня.") #Logging
            return list_polls
        except Exception as e:
            log.error(f"Ошибка получения анкет: {e}") #Logging
            return []
        
    def get_answers(list_polls): # Запрашиваю заполенные анкеты
        if not list_polls: #Проверка на наличие анкет за вчера. Если не было, то даг прерывается
            raise AirflowSkipException('Не было данных за период')
        yesterday_answers = []
        try:
            for poll in list_polls:
                response_poll = requests.get(F"{url}/{poll}", headers=headers, verify=False)
                data_poll = response_poll.json()
                yesterday_answers.append(data_poll)
                continue
        except Exception as e:
            log.error(f"Ошибка запроса ответов на анкету {poll}: {e}") #Logging
            send_tg('Наставники. ❌ Ошибка получения ответов')
        polls = pd.json_normalize(yesterday_answers)
        log.info(f"Из {len(list_polls)} анкет пришли ответы по {len(polls[polls['data.items'].isna()==False])}") #Logging
        if len(list_polls) < len(polls[polls['data.items'].isna()==False]):
            send_tg(f"Наставники. ❌ За период из {len(list_polls)} анкет пришли ответы по {len(polls[polls['data.items'].isna()==False])}")
        yesterday_answers = json.dumps(yesterday_answers)
        return yesterday_answers

    def answers_processing(yesterday_answers): #Обрабатываю данные
        try:
            # Отбираю необходимые колонки из polls
            yesterday_answers = json.loads(yesterday_answers) #Десериализация для работы
            polls = pd.json_normalize(yesterday_answers)
            polls.columns = polls.columns.str.replace('.', '_', regex = False)
            polls_filter = polls[['data_public_id', 'data_id', 'data_place_name', 'data_place_groups',
                                  'data_pattern_name', 'data_assignee_name', 'data_fact_rate', 'data_max_rate', 'data_started_at',
                                  'data_finished_at', 'data_creator_name', 'data_items']].copy()
            # Создаю столбец с ссылками на анкету в приложении
            polls_filter['check_link'] = 'https://rosakhutor.checkoffice.ru/inspections/' + polls_filter['data_id'].astype(str) + '/view'
            # Извлекаю группы объекта
            polls_filter['data_place_groups'] = polls_filter['data_place_groups'].apply(lambda x: extract_values(x, 'name'))
            # Рассчитываю процент выполнения анкеты
            polls_filter['percent_point'] = round(polls_filter['data_fact_rate'] / polls_filter['data_max_rate'] * 100, 2)
            # Расскрываю json с вопросами/ответами из списка
            exploded_df = polls_filter.explode('data_items', ignore_index=True)
            merged_df = pd.concat([exploded_df.drop('data_items', axis=1), pd.json_normalize(exploded_df['data_items'])], axis=1)
            # Отбираю нужные колонки
            merged_df = merged_df[['data_public_id', 'data_id', 'data_place_name',
                   'data_place_groups', 'data_pattern_name', 'data_assignee_name',
                   'data_fact_rate', 'data_max_rate', 'data_started_at',
                   'data_finished_at', 'data_creator_name', 'check_link', 'percent_point',
                   'type', 'name', 'value', 'is_violated', 'factRate', 'rate', 'photos', 'videos', 'comment', 'audios']]
            # Извлекаю из списка словарей ссылки на медиа файлы
            merged_df['photos'] = merged_df['photos'].apply(lambda x: extract_values(x, 'url'))
            merged_df['videos'] = merged_df['videos'].apply(lambda x: extract_values(x, 'url'))
            merged_df['audios'] = merged_df['audios'].apply(lambda x: extract_values(x, 'url'))
            # Корректирую ответы для вопросов типа "TextComment"
            merged_df.loc[merged_df['type'] == 'TextComment', 'value'] = merged_df['comment']
            merged_df.loc[merged_df['type'] == 'TextComment', 'comment'] = ''
            # Извлекаю имя из соответствующего вопроса
            merged_df.loc[merged_df['name'] == 'ФИО оцениваемого сотрудника', 'value'] = merged_df['comment']
            merged_df.loc[merged_df['name'] == 'ФИО оцениваемого сотрудника', 'comment'] = ''
            # Корректирую признак нарушения из bool в int
            merged_df.loc[merged_df['is_violated'] == False, 'is_violated'] = 0
            merged_df.loc[merged_df['is_violated'] == True,  'is_violated'] = 1
            # Выделяю секцию вопроса
            merged_df['check_section'] = ''
            current_section = ''
            for index, row in merged_df.iterrows():
                if row['type'] == 'Section':
                    current_section = row['name']
                merged_df.at[index, 'check_section'] = current_section
            # Выделяю группу вопроса    
            merged_df['check_category'] = ''
            current_category = ''
            for index, row in merged_df.iterrows():
                if row['type'] == 'Category':
                    current_category = row['name']
                merged_df.at[index, 'check_category'] = current_category
            # Избавляюсь от ненужных больше строк
            values = ['Section', 'Category']
            merged_df = merged_df[merged_df.type.isin(values) == False]
            # Рассчитываю количество ошибок в рамках анкеты
            merged_df['count_error'] = merged_df.groupby('data_public_id')['is_violated'].transform('sum')
            log.info(f"Обработка по плану до переименования колонок") #Logging
        except Exception as e:
            log.error(f"Ошибка в обработке до переименовывания колонок")#Logging
            send_tg('Наставники. ❌ в обработке до переименовывания колонок')
        
        try:
            # Переименовываю столбцы в соответствии с БД
            new_data_rename = merged_df.rename(columns = {
                'data_public_id'    : 'check_number',
                'data_id'           : 'check_id',
                'data_place_name'   : 'object_name',
                'data_place_groups' : 'object_group',
                'data_pattern_name' : 'check_name',
                'data_assignee_name': 'user_name',
                'data_fact_rate'    : 'sum_point',
                'data_max_rate'     : 'max_point',
                'data_started_at'   : 'datetime_start',
                'data_finished_at'  : 'datetime_finish',
                'data_creator_name' : 'check_creator',
                'type'              : 'answer_type',
                'name'              : 'question',
                'value'             : 'answer',
                'is_violated'       :'have_error',
                'factRate'          : 'result_point',
                'rate'              : 'weight_question',
                'photos'            : 'foto_link',
                'videos'            : 'video_link',
                'audios'            : 'audio_link'
            })
            # Располагаю столбцы в нужном порядке для загрузки в БД
            new_data_1 = new_data_rename[['check_number', 'check_id', 'check_link', 'object_name', 'object_group',
                   'check_name', 'user_name', 'sum_point', 'max_point', 'percent_point',
                   'count_error', 'datetime_start', 'datetime_finish', 'check_section',
                   'check_category', 'answer_type', 'question', 'answer', 'have_error',
                   'result_point', 'weight_question', 'foto_link', 'video_link', 'comment',
                   'audio_link', 'check_creator']]
            # Избавляюсь от пустых значений
            new_data_1 = new_data_1.fillna(0)
            # Вытягиваю дату из ответа на вопрос про дату/время проверки. Обновляю datetime_start
            new_data_1 = update_column(new_data_1, 'check_number', 'datetime_start', 'question', 'Дата и время', 'answer')
            # Привожу данные к необходимому формату
            new_data_1['check_id'] = new_data_1['check_id'].astype('int64')
            new_data_1['datetime_start']  = pd.to_datetime(new_data_1['datetime_start']) .dt.strftime('%Y-%m-%d %H:%M:%S')
            new_data_1['datetime_finish'] = pd.to_datetime(new_data_1['datetime_finish']).dt.strftime('%Y-%m-%d %H:%M:%S')
            new_data_1['count_error'] = new_data_1['count_error'].astype('int64')
            new_data_1['have_error'] = new_data_1['have_error'].astype('int64')
            new_data_1['result_point'] = new_data_1['result_point'].astype('int64')
            new_data_1['weight_question'] = new_data_1['weight_question'].astype('int64')

            new_data_1 = new_data_1.apply(convert_column_type)
            # Перевожу анкеты в соответствующие службы
            # Определяю словари для переводов анкет по нужным службам
            updates = {
                #  Кассы кафе
                ('Кассы', 'Берлога')  : ('Кафе', 'Берлога кассир'),
                ('Кассы', 'Йети кафе'): ('Кафе', 'Йети кафе кассир'),

                #  Пляж и каток
                ('Контролер пляжа', 'Пляж')   : ('Пляж', 'Контролер'),
                ('Кассы', 'Пляж')             : ('Пляж', 'Кассир'),
                ('Кассы', 'Каток')            : ('Каток', 'Кассир'),
                ('Прокат', 'Каток')           : ('Каток', 'Прокат'),
                ('Инструктор катка', 'Каток') : ('Каток', 'Инструктор'),

                #  ЦАО и ГШ
                ('Контактный центр', 'Горнолыжная школа')     : ('ЦАО и ГШ', 'КЦ ГШ'),
                ('Контактный центр', 'Центр активного отдыха'): ('ЦАО и ГШ', 'КЦ ЦАО'),
                ('Кассы', 'Йети парк')                        : ('ЦАО и ГШ', 'Кассир Йети парк'),
                ('Инструктор Йети парк', 'Йети парк')         : ('ЦАО и ГШ', 'Инструктор Йети парк'),
                ('Инструктор ГШ', 'Школа Егорка')             : ('ЦАО и ГШ', 'Инструктор Егорка'),
                ('Инструктор ГШ', 'Горнолыжная школа')        : ('ЦАО и ГШ', 'Инструктор ГШ'),
                ('Гиды ЦАО', 'Центр активного отдыха')        : ('ЦАО и ГШ', 'Гид ЦАО'),

                ('Администраторы ЦАО', 'Романов Мост')        : ('ЦАО и ГШ', 'Админ ЦАО Романов Мост'),
                ('Администраторы ЦАО', 'Роза Приют')          : ('ЦАО и ГШ', 'Админ ЦАО Роза Приют'),
                
                ('Администраторы ГШ', 'Роза Приют')           : ('ЦАО и ГШ', 'Админ ГШ Роза Приют'),
                ('Администраторы ГШ', 'Романов Мост')         : ('ЦАО и ГШ', 'Админ ГШ Романов Мост'),
                ('Администраторы ГШ', 'Роза Приют Премиум')   : ('ЦАО и ГШ', 'Админ ГШ Роза Приют Премиум'),
                ('Администраторы ГШ', 'Школа Егорка')         : ('ЦАО и ГШ', 'Админ ГШ Егорка'),
                ('Администраторы ГШ', 'Админы ГШ на 1600')    : ('ЦАО и ГШ', 'Админ ГШ на 1600'),
                ('Администраторы ГШ', 'Роза Спрингс')         : ('ЦАО и ГШ', 'Админ ГШ Роза Спрингс'),
                ('Администраторы ГШ', 'Рэдиссон')             : ('ЦАО и ГШ', 'Админ ГШ Рэдиссон'),
                
                #  Музей археологии и Моя РФ
                ('Магазин', 'Музей Археологии')                     : ('Музей Археологии', 'Магазин'),
                ('Администратор музея', 'Музей Археологии')         : ('Музей Археологии', 'Администратор'),
                ('Экскурсовод музея археологии', 'Музей Археологии'): ('Музей Археологии', 'Экскурсовод'),
                ('Экскурсовод Моей России', 'Моя Россия')           : ('Моя Россия', 'Экскурсовод'),
                
                #  Фотографы
                ('Фотограф', 'Волчья скала')        : ('Фотослужба', 'Фотограф Волчья скала'),
                ('Фотограф', 'Йети на Пике')        : ('Фотослужба', 'Фотограф Йети на Пике'),
                ('Фотограф', 'Качели над облаками') : ('Фотослужба', 'Фотограф Качели над облаками'),
                ('Фотограф', 'Олимпийские кольца')  : ('Фотослужба', 'Фотограф Олимпийские кольца'),
                ('Фотограф', 'Фотослужба 1600')     : ('Фотослужба', 'Фотограф 1600'),
                ('Фотограф', 'Пик 1. Роза Пик')     : ('Фотослужба', 'Фотограф Роза Пик'),
                ('Фотограф', 'Пик 2. Высота 2320')  : ('Фотослужба', 'Фотограф Высота 2320'),
                ('Фотограф', 'Сердце у Родельбана') : ('Фотослужба', 'Фотограф Сердце у Родельбана'),
                ('Фотограф', 'Качели на 1350')      : ('Фотослужба', 'Фотограф Качели на 1350'),
                
                #  Кассы фотослужб
                ('Кассир фотозоны', 'Волчья скала')        : ('Фотослужба', 'Кассир Волчья скала'),
                ('Кассир фотозоны', 'Йети на Пике')        : ('Фотослужба', 'Кассир Йети на Пике'),
                ('Кассир фотозоны', 'Качели над облаками') : ('Фотослужба', 'Кассир Качели над облаками'),
                ('Кассир фотозоны', 'Олимпийские кольца')  : ('Фотослужба', 'Кассир Олимпийские кольца'),
                ('Кассир фотозоны', 'Эдельвейс')           : ('Фотослужба', 'Кассир Эдельвейс'),
                ('Кассир фотозоны', 'Стрела')              : ('Фотослужба', 'Кассир Стрела'),
                ('Кассир фотозоны', 'Фотослужба 1600')     : ('Фотослужба', 'Кассир 1600'),
                ('Кассир фотозоны', 'Пик 1. Роза Пик')     : ('Фотослужба', 'Кассир Роза Пик'),
                ('Кассир фотозоны', 'Пик 2. Высота 2320')  : ('Фотослужба', 'Кассир Высота 2320'),
                ('Кассир фотозоны', 'Сердце у Родельбана') : ('Фотослужба', 'Кассир Сердце у Родельбана'),
                ('Кассир фотозоны', 'Качели на 1350')      : ('Фотослужба', 'Кассир Качели на 1350'),

                #  Прочие
                ('Отдел бронирования отелей', 'Вальсет Центр'): ('Отдел бронирования отелей', 'Вальсет'),
                ('Прокат', 'Вальсет Центр')                   : ('Прокат', 'Вальсет'),
                ('Контролер посадчик родельбана', 'Родельбан'): ('Родельбан', 'Контролер посадчик'),
            }
            # Прохожу по словарям и применяю обновления
            for (old_check_name, old_object_name), (new_check_name, new_object_name) in updates.items():
                new_data_1.loc[
                    (new_data_1['check_name'] == old_check_name) & 
                    (new_data_1['object_name'] == old_object_name), 
                    ['check_name', 'object_name']
                ] = [new_check_name, new_object_name]
            log.info(f"Обработка успешно") #Logging
            # Перевожу в json для передачи в следующую таску и сериализую
            new_data_1 = new_data_1.to_dict(orient='records')
            new_data_1 = json.dumps(new_data_1)
        except Exception as e:
            log.error(f"Ошибка в обработке после переименовывания колонок") #Logging
            send_tg('Наставники. ❌ в обработке после переименовывания колонок')
            return []
        return new_data_1

    def push_data(new_data_1): # Загружаю данные в БД
        if not new_data_1:
            send_tg('Наставники. ❌ Почему-то не дошли данные до загрузки')
            raise AirflowSkipException("Нет подготовленных данных для загрузки в БД")
        new_data_1 = json.loads(new_data_1) #Десериализация
        new_data_1 = pd.json_normalize(new_data_1)
        # Подключаюсь к БД
        host = clickhouse_connection['host']
        port = clickhouse_connection['port']
        user = clickhouse_connection['user']
        password = clickhouse_connection['password']
        database = clickhouse_connection['database']
        client = Client(host=host, port=port, user = user, password = password, database = database,
                        secure=True, verify="/opt/airflow/dags/lib/certs/YandexInternalRootCA.crt")
        # Запрашиваю данные номеров проверок
        query = '''
            SELECT distinct check_number
            FROM tutors
        '''
        id_from_db = client.query_dataframe(query)
        # Пушу данные в БД, обновляю дубли (если есть)
        to_drop = list(new_data_1[new_data_1['check_number'].isin(id_from_db['check_number'])]['check_number'].unique())
        new_polls = list(new_data_1[~new_data_1['check_number'].isin(id_from_db['check_number'])]['check_number'].unique())

        query = '''
            INSERT INTO department.tutors (check_number, check_id, check_link,
                                           object_name, object_group, check_name,
                                           user_name, sum_point, max_point,
                                           percent_point, count_error, datetime_start,
                                           datetime_finish, check_section, check_category,
                                           answer_type, question, answer,
                                           have_error, result_point, weight_question,
                                           foto_link, video_link, comment,
                                           audio_link, check_creator)
            VALUES
                '''
        if len(to_drop) == 0:
            # Загружаю данные если дублей нет
            data_for_push = [tuple(row) for row in new_data_1.itertuples(index=False)]
            client.execute(query, data_for_push)
            log.info(f"Загружены данные без изменения загруженных в прошлом") #Logging
        else:
            # Удаляю дубли из БД
            query_for_drop = F'''
            ALTER TABLE tutors
            DELETE WHERE check_number IN {tuple(to_drop)}
            '''
            client.execute(query_for_drop)
            # Загружаю обновленные данные после удаления дублей
            data_for_push = [tuple(row) for row in new_data_1.itertuples(index=False)]
            client.execute(query, data_for_push)
            log.info(f"Данные загружены, в т.ч. обновлены:{to_drop}") #Logging
            
#   Порядок выполнения
    @task
    def pipeline(**kwargs):
        execution_date = kwargs['execution_date'].isoformat()
        top_data  = get_polls(yesterday = execution_date)
        answers   = get_answers(top_data)
        data_proc = answers_processing(answers)
        pushing   = push_data(data_proc)
        
    pipeline()


uploading_data_tutors_to_ch = uploading_data_tutors_to_ch()
