import os
import time
import concurrent.futures
import argparse
import logging
from py2neo import Graph, Node, Relationship

from GetVkInfo import GetVkInfo

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,  # Уровень логирования
    format='%(asctime)s - %(levelname)s - %(message)s',  # Формат сообщений
    handlers=[
        logging.FileHandler("vk_info.log", encoding="utf-8"),  # Логирование в файл с кодировкой utf-8
        logging.StreamHandler()  # Логирование в консоль
    ]
)


def save_user_to_neo4j(graph, user_data):
    if not user_data.get("id"):
        logging.error("Ошибка: поле 'id' отсутствует или пустое: %s", user_data)
        return None

    # Создаем узел с дополнительными атрибутами, как указано в задании
    user_node = Node(
        "User",
        id=user_data.get('id'),
        screen_name=user_data.get('screen_name'),
        name=user_data.get('name'),
        sex="Female" if user_data.get('sex') == 1 else "Male",  # Используем тернарный оператор для выбора пола
        home_town=user_data.get('home_town'),
        followers_count=user_data.get('followers_count')
    )
    graph.merge(user_node, "User", "id")
    logging.info("Пользователь %s успешно сохранен в Neo4j.", user_data.get('id'))
    return user_node


def save_group_to_neo4j(graph, group_data):
    if not group_data.get("id"):
        logging.error("Ошибка: поле 'id' отсутствует или пустое для группы: %s", group_data)
        return None

    # Сохранение группы с дополнительными атрибутами
    group_node = Node(
        "Group",
        id=group_data.get('id'),
        name=group_data.get('name', 'Unnamed Group'),
        screen_name=group_data.get('screen_name')
    )
    graph.merge(group_node, "Group", "id")
    logging.info("Группа %s успешно сохранена в Neo4j.", group_data.get('id'))
    return group_node


def create_relationship(graph, user_node, target_node, rel_type="Follow"):
    if user_node is None or target_node is None:
        logging.error("Ошибка при создании связи: один из узлов не существует.")
        return None

    relationship = Relationship(user_node, rel_type, target_node)
    graph.merge(relationship)
    logging.info("Связь типа '%s' между %s и %s успешно сохранена.", rel_type, user_node["id"], target_node["id"])
    return relationship


def execute_query(graph, query, **kwargs):
    try:
        result = graph.run(query, **kwargs).data()
        logging.info("Запрос выполнен успешно: %s", query)
        return result
    except Exception as e:
        logging.error("Ошибка при выполнении запроса: %s", e)
        return None


def get_user_info_recursive(vk_info_instance, user_id, depth=2):
    """
    Рекурсивная функция для получения информации о пользователе и его фолловерах и подписках.

    :param vk_info_instance: Экземпляр класса GetVkInfo, который используется для доступа к VK API.
    :param user_id: ID пользователя, с которого начинается процесс.
    :param depth: Глубина рекурсии для обхода фолловеров и подписок.
    """
    if depth < 1:
        return

    logging.info("Начало обработки пользователя ID %s на глубине %d", user_id, depth)

    # Получение информации о пользователе и сохранение его данных в Neo4j
    user_data = vk_info_instance.get_user_info(user_id)
    user_node = save_user_to_neo4j(graph, user_data)
    if user_node is None:
        logging.error("Не удалось сохранить пользователя ID %s в Neo4j.", user_id)
        return

    # Получение друзей и подписок пользователя
    friends_and_requests = vk_info_instance.get_friends_and_followers(user_id)
    groups = vk_info_instance.get_groups(user_id)

    # Функция для обработки друзей
    def process_friend(friend_id):
        time.sleep(4)  # Задержка для предотвращения частых запросов
        friend_vk_info = GetVkInfo(friend_id, vk_info_instance.token)
        friend_data = friend_vk_info.get_user_info(friend_id)
        friend_node = save_user_to_neo4j(graph, friend_data)
        if friend_node:
            create_relationship(graph, user_node, friend_node, "Follow")
            # Рекурсивный вызов для фолловеров на следующем уровне
            get_user_info_recursive(friend_vk_info, friend_id, depth - 1)

    # Функция для обработки групп
    def process_group(group):
        time.sleep(4)
        group_node = save_group_to_neo4j(graph, group)
        if group_node:
            create_relationship(graph, user_node, group_node, "Subscribe")

    # Запуск задач для обработки друзей и подписок параллельно
    with concurrent.futures.ThreadPoolExecutor() as executor:
        friend_futures = [executor.submit(process_friend, friend_id) for friend_id in friends_and_requests]
        group_futures = [executor.submit(process_group, group) for group in groups]

        # Ожидание завершения всех задач
        for future in concurrent.futures.as_completed(friend_futures + group_futures):
            try:
                future.result()
            except Exception as e:
                logging.error("Ошибка при обработке: %s", e)

    logging.info("Обработка пользователя с ID %s завершена на глубине %d.", user_id, depth)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK User Info Fetcher with Neo4j Integration")
    parser.add_argument('--user_id', type=str, default=None, help='VK User ID')
    args = parser.parse_args()

    # Ваш токен доступа
    token = os.getenv('VK_ACCESS_TOKEN')

    if not token:
        raise ValueError("Переменная VK_ACCESS_TOKEN не найдена")

    user_id = args.user_id or '326621197'

    # Инициализация подключения к Neo4j
    try:
        graph = Graph("bolt://localhost:7687", auth=("neo4j", "123"))
        logging.info("Подключение к Neo4j успешно.")
    except Exception as e:
        logging.error("Ошибка подключения к Neo4j: %s", e)
        raise

    # Инициализация доступа к VK API
    getVkInfo = GetVkInfo(user_id=user_id, vk_token=token)

    # Запуск рекурсивного сбора информации с заданной глубиной
    get_user_info_recursive(getVkInfo, user_id, depth=2)
