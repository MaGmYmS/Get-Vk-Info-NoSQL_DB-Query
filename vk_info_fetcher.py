import os
import time
import concurrent.futures
import json
import argparse
import logging
from py2neo import Graph, Node, Relationship

from GetVkInfo import GetVkInfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def save_user_to_neo4j(graph, user_data):
    if not user_data.get("id"):
        logging.error("Ошибка: поле 'id' отсутствует или пустое: %s", user_data)
        return None  # Пропускаем создание узла для некорректных данных

    user_node = Node(
        "User",
        id=user_data.get('id'),
        name=f"{user_data.get('first_name', '')} {user_data.get('last_name', '')}",
    )
    graph.merge(user_node, "User", "id")
    return user_node


def save_group_to_neo4j(graph, group_data):
    group_node = Node(
        "Group",
        id=group_data.get('id'),
        name=group_data.get('name', 'Unnamed Group')  # Присваиваем имя группе
    )
    graph.merge(group_node, "Group", "id")
    return group_node


def create_relationship(graph, user_node, target_node, rel_type="Follow"):
    relationship = Relationship(user_node, rel_type, target_node)
    graph.merge(relationship)


def execute_query(graph, query, **kwargs):
    return graph.run(query, **kwargs).data()


def get_user_info_recursive(vk_info_instance, user_id, depth=2):
    """
    Рекурсивная функция для получения информации о пользователе и его фолловерах и подписках.

    :param vk_info_instance: Экземпляр класса GetVkInfo, который используется для доступа к VK API.
    :param user_id: ID пользователя, с которого начинается процесс.
    :param depth: Глубина рекурсии для обхода фолловеров и подписок.
    """
    if depth < 1:
        return

    # Получение информации о пользователе и сохранение его данных в Neo4j
    user_data = vk_info_instance.get_user_info()
    user_node = save_user_to_neo4j(graph, user_data)

    # Получение друзей и подписок пользователя
    friends_and_requests = vk_info_instance.get_friends_and_requests()
    groups = vk_info_instance.get_groups()

    # Функция для обработки друзей
    def process_friend(friend_id):
        time.sleep(1)
        friend_vk_info = GetVkInfo(friend_id, vk_info_instance.token)
        friend_data = friend_vk_info.get_user_info()
        friend_node = save_user_to_neo4j(graph, friend_data)
        create_relationship(graph, user_node, friend_node, "Follow")
        # Рекурсивный вызов для фолловеров на следующем уровне
        get_user_info_recursive(friend_vk_info, friend_id, depth - 1)

    # Функция для обработки групп
    def process_group(group):
        time.sleep(1)
        group_node = save_group_to_neo4j(graph, group)
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

    logging.info("Обработка пользователя с ID %s завершена.", user_id)


def save_info_to_json(graph, user_info, followers, subscriptions, group_details, output_file):
    logging.info("Запросы к базе данных")
    top_5_users = execute_query(graph,
                                "MATCH (u:User)-[:Follow]->() RETURN u, count(*) AS followers ORDER BY followers DESC LIMIT 5")
    top_5_groups = execute_query(graph,
                                 "MATCH (g:Group)<-[:Subscribe]-() RETURN g, count(*) AS members ORDER BY members DESC LIMIT 5")
    mutual_followers = execute_query(graph, "MATCH (u1:User)-[:Follow]->(u2:User), (u2)-[:Follow]->(u1) RETURN u1, u2")

    # Сохраняем результат в JSON-файл
    result = {
        'user_info': user_info,
        'followers': followers,
        'subscriptions': subscriptions,
        'groups': group_details,
        'top_5_users': top_5_users,
        'top_5_groups': top_5_groups,
        'mutual_followers': mutual_followers
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

    logging.info(f"Данные успешно сохранены в {output_file}")


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
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "123"))

    getVkInfo = GetVkInfo(user_id=user_id, vk_token=token)

    # Запуск рекурсивного сбора информации с заданной глубиной
    get_user_info_recursive(getVkInfo, user_id, depth=2)
