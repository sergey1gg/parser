import asyncio
from telethon import TelegramClient, events
import re
import time
import datetime
import mysql.connector
import difflib
from collections import defaultdict
import requests

api_id =21757601  #25277649
api_hash = 'be858f9dfb60353f4974d37701d82b99' #b8d91ac3e90edfff8a36c27411a2b2e9
client = TelegramClient('anon', api_id, api_hash)
global_messages_to_send = []

time_diff=0
percent=0
similar_posts_count=0

async def getPosts(channels, cursor, db):
    global time_diff
    postsToInsert=[]
    for channel_username in channels:
        channel_entity = await client.get_entity(channel_username)
        channel_name = channel_entity.title
        time.sleep(2)
        posts = await client.get_messages(channel_entity, limit=3)
        for post in posts:
            if post.media and post.text != '':
               
                repl = re.sub(r'@\w+|#\w+|https?://\S+', '', post.text)
                repl2 = re.sub("^\s+|\n|\r|\s+$", '', repl)
                message_text = re.sub(r'[*\W_]+', ' ', repl2)
                
                link = f"https://t.me/{channel_username}/{post.id}" if not re.match(r'^https://', channel_username) else f"https://t.me/c/{str(channel_entity).replace('-', '')[3:]}/{post.id}"
                views = post.views
                
                reactions_count = sum(reaction.count for reaction in post.reactions.results) if post.reactions else 0
                timestamp1 = post.date
                timestamp2 = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
                time_difference = abs(timestamp2 - timestamp1).total_seconds()
                
                if time_difference <=time_diff:
                   
                    print(f"Прошло менее 20 минут. {link} {views} {reactions_count}")  
                    postsToInsert.append({  'channel_id': channel_username,
                                            'channel_name':channel_name,
                                            'message_text': message_text,
                                            'link': link,
                                            'reactions': reactions_count,
                                            'views': views,
                                            'created_at': timestamp1})

    linksEquals = {}

    for post in postsToInsert:
        try:
            cursor.execute("SELECT * FROM posts WHERE link = %s", (post['link'],))
            existing_post = cursor.fetchone()
            if existing_post:
                cursor.execute("UPDATE posts SET channel_name=%s, message_text=%s, reactions=%s, views=%s, created_at=%s WHERE link = %s", (post['channel_name'], post['message_text'], post['reactions'], post['views'], post['created_at'], post['link']))
            else:
                try:
                    cursor.execute("INSERT INTO posts (channel_id, channel_name, message_text, link, reactions, views, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s)", (post['channel_id'], post['channel_name'], post['message_text'], post['link'], post['reactions'], post['views'], post['created_at']))
                except mysql.connector.Error as err:
                    print("Ошибка при выполнении запроса:", err)
            db.commit()
        except mysql.connector.Error as err:
            print("Ошибка при выполнении запроса:", err)



async def find_similar_posts(cursor, threshold):
    similar_posts = []
    
    # Получаем все посты из базы данных за последний час
    current_time_utc = datetime.datetime.utcnow()
    time_threshold = current_time_utc - datetime.timedelta(hours=1)
    
    cursor.execute("SELECT * FROM posts WHERE created_at >= %s", (time_threshold,))
    posts_db = cursor.fetchall()

    # Сравниваем каждый пост с каждым
    for i in range(len(posts_db) - 1):
        for j in range(i + 1, len(posts_db)):
            post_i = posts_db[i]
            post_j = posts_db[j]

            if post_i[4] == post_j[4]:
                continue

            message_words_i = post_i[3].split()
            message_words_j = post_j[3].split()
            
            # Рассчитываем коэффициент сходства
            similarity_ratio = difflib.SequenceMatcher(None, message_words_i, message_words_j).ratio()

            if similarity_ratio >= threshold and post_i[1] != post_j[1]:
                similar_posts.append({
                    'post_i': post_i,
                    'post_j': post_j,
                    'similarity_ratio': similarity_ratio,
                    'views_i': post_i[6],
                    'views_j': post_j[6],
                    'reactions_i': post_i[5],
                    'reactions_j': post_j[5],
                    'message_text': post_i[3],
                })
    return similar_posts
    
async def main():
    global global_messages_to_send, time_diff

    db = mysql.connector.connect(
    host="45.9.24.153",
    user="parsertg",
    password="a23sdg@d5!kd#29",
    database="parsertg"
)

    if db.is_connected():
        print("CONNECT")

        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS posts (id INT AUTO_INCREMENT PRIMARY KEY, channel_id TEXT, channel_name TEXT, message_text TEXT, link TEXT NOT NULL, reactions INT, views INT, created_at DATETIME)")
        db.commit()
        
        cursor.execute("SELECT username FROM usernames")
        channels = [row[0] for row in cursor.fetchall()]

        keys_to_fetch = ("similar_posts", "time_diff", "similar_percent")

        # Выполнение запроса для получения значений по заданным ключам
        cursor.execute("SELECT setting_key, setting_value FROM settings WHERE setting_key IN (%s)" % ', '.join(['%s'] * len(keys_to_fetch)), keys_to_fetch)

        # Извлечение результатов
        results = cursor.fetchall()
        # Обработка результатов
        for setting_key, setting_value in results:
            if setting_key == "similar_posts":
                similar_posts_count = int(setting_value)
            elif setting_key == "time_diff":
                time_diff = int(setting_value)
            elif setting_key == "similar_percent":
                percent= float(setting_value)
        await client.start()
        await getPosts(channels, cursor, db)  
        similar_posts = await find_similar_posts(cursor, threshold=percent)
        message=""
    # Вывести результаты
        if similar_posts:
        # Инициализация словаря для объединения похожих постов
            similar_posts_dict = defaultdict(list)

            # Заполнение словаря
            for post_info in similar_posts:
                post_i = post_info['post_i']
                post_j = post_info['post_j']
                similarity_ratio = post_info['similarity_ratio']

                # Добавление в словарь
                similar_posts_dict[post_i[4]].append({
                    'post_j': post_j[4],
                    'similarity_ratio': similarity_ratio,
                    'views_j': post_info['views_j'],
                    'reactions_j': post_info['reactions_j'],
                    'views_i': post_info['views_i'],
                    'reactions_i': post_info['reactions_i'],
                    'message_text': post_info['message_text']
                })


            messages_to_send = []
            checked_messages=[]
            for post_id, similar_posts_list in similar_posts_dict.items():
                if len(similar_posts_list) > similar_posts_count:  # Количество совпадений
                    current_message = f"\n\nПост {post_id} реакций {similar_posts_list[0]['reactions_i']} просмотров {similar_posts_list[0]['views_i']}\n{similar_posts_list[0]['message_text']}\n"
                    checked_messages=f"{post_id}"
                    for similar_post in similar_posts_list:
                        post_j = similar_post['post_j']
                        similarity_ratio = similar_post['similarity_ratio']
                        current_message += f"{post_j}: Совпадение: {similarity_ratio:.2f} Реакций: {similar_post['reactions_j']} Просмотров {similar_post['views_j']}\n"
                        checked_messages+=f"{post_j}"

                    
                    if checked_messages not in global_messages_to_send:  # Проверка наличия сообщения в глобальном списке
                        global_messages_to_send.append(checked_messages)
                        messages_to_send.append(current_message)

            if messages_to_send:
                bot_token = '6241029292:AAGHM_8qMCCOqkLBBOg1tK0immbsent3wvs'
                chat_ids = ['567152294', '6320508601', '220567177'] #567152294 6320508601 220567177
                api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
                print(messages_to_send)
                for chat_id in chat_ids:
                    for message in messages_to_send:
                        params = {
                            'chat_id': chat_id,
                            'text': message,
                        }
                        try:
                            requests.get(api_url, params=params)
                            print("сообщение отправлено")
                        except Exception as e:
                            print(e)


with client:
    while True:
        client.loop.run_until_complete(main())
        time.sleep(120)
