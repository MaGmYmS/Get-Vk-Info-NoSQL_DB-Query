import os
import requests


class GetVkInfo:
    def __init__(self, user_id, vk_token):
        """
        Инициализация класса GetVkInfo с токеном доступа к VK API,
        идентификатором пользователя и базовыми параметрами.

        :param user_id: Идентификатор пользователя ВК (строка или число).
        :param vk_token: Токен доступа к VK API (строка).
        """
        self.user_id = user_id
        self.token = vk_token
        self.base_params = {
            'access_token': self.token,
            'v': '5.131'
        }

    def get_user_info(self, user_id=None):
        """
        Получает информацию о пользователе ВКонтакте, включая screen_name, sex, home_town, city.

        :param user_id: Идентификатор пользователя ВК (строка или число), необязательный.
        :return: Словарь с информацией о пользователе.
        """
        params = {
            **self.base_params,
            'user_ids': user_id or self.user_id,
            'fields': 'followers_count,sex,city,screen_name,home_town'
        }
        response = requests.get('https://api.vk.com/method/users.get', params=params)
        user_info = response.json().get('response', [{}])[0]

        return {
            'id': user_info.get('id'),
            'screen_name': user_info.get('screen_name'),
            'name': f"{user_info.get('first_name')} {user_info.get('last_name')}",
            'sex': user_info.get('sex'),
            'home_town': user_info.get('city', {}).get('title', user_info.get('home_town')),
            'followers_count': user_info.get('followers_count')
        }

    def get_followers(self, user_id=None):
        """
        Получает список фолловеров пользователя ВКонтакте.

        :param user_id: Идентификатор пользователя ВК (строка или число), необязательный.
        :return: Список фолловеров (ID пользователей).
        """
        params = {**self.base_params, 'user_id': user_id or self.user_id, 'count': 1000}
        response = requests.get('https://api.vk.com/method/users.getFollowers', params=params)
        return response.json().get('response', {}).get('items', [])

    def get_friends(self, user_id=None):
        """
        Получает список подписок пользователя ВКонтакте.

        :param user_id: Идентификатор пользователя ВК (строка или число), необязательный.
        :return: Список подписок (ID групп и пользователей).
        """
        params = {**self.base_params, 'user_id': user_id or self.user_id}
        response = requests.get('https://api.vk.com/method/friends.get', params=params)
        return response.json().get('response', {}).get('items', [])

    def get_friends_and_followers(self, user_id=None):
        """
        Получает объединённый список друзей и подписчиков пользователя ВКонтакте.

        :param user_id: Идентификатор пользователя ВК (строка или число), необязательный.
        :return: Уникальный список ID пользователей, включающий и друзей, и подписчиков.
        """
        followers = self.get_followers(user_id)
        friends = self.get_friends(user_id)

        # Объединение списков и удаление дубликатов
        all_connections = list(set(followers + friends))

        return all_connections

    def get_groups(self, user_id=None):
        """
        Получает список групп, в которых состоит пользователь ВКонтакте.

        :param user_id: Идентификатор пользователя ВК (строка или число), необязательный.
        :return: Список словарей с идентификаторами и именами групп.
        """
        params = {**self.base_params, 'user_id': user_id or self.user_id}
        response = requests.get('https://api.vk.com/method/groups.get', params=params)
        group_ids = response.json().get('response', {}).get('items', [])
        return self.__get_group_details(group_ids)

    def __get_group_details(self, group_ids):
        """
        Получает подробную информацию о группах по их идентификаторам, включая имя и screen_name.

        :param group_ids: Список идентификаторов групп.
        :return: Список словарей с данными групп (id, name, screen_name).
        """
        if not group_ids:
            return []

        params = {**self.base_params, 'group_ids': ','.join(map(str, group_ids)), 'fields': 'name,screen_name'}
        response = requests.get('https://api.vk.com/method/groups.getById', params=params)
        group_details = response.json().get('response', [])

        return [{'id': group.get('id'), 'name': group.get('name'), 'screen_name': group.get('screen_name')} for group in
                group_details]


if __name__ == "__main__":
    token = os.getenv('VK_ACCESS_TOKEN')
    if not token:
        raise ValueError("Переменная VK_ACCESS_TOKEN не найдена")

    getVkInfo = GetVkInfo(user_id='326621197', vk_token=token)

    # Предположим, что экземпляр класса GetVkInfo инициализирован как getVkInfo
    user_info = getVkInfo.get_user_info()
    followers = getVkInfo.get_followers()
    subscriptions = getVkInfo.get_friends()
    groups = getVkInfo.get_groups()

    # Вывод информации о пользователе
    print("=== User Information ===")
    print(f"ID: {user_info['id']}")
    print(f"Screen Name: {user_info['screen_name']}")
    print(f"Name: {user_info['name']}")
    print(f"Sex: {'Male' if user_info['sex'] == 2 else 'Female' if user_info['sex'] == 1 else 'Unknown'}")
    print(f"Home Town: {user_info['home_town']}")
    print(f"Followers Count: {user_info['followers_count']}")

    print("=== Follower Information ===")
    print(len(followers))

    print("=== Subscription Information ===")
    print(len(subscriptions))

    # Вывод информации о группах
    print("\n=== Groups ===")
    print(f"Total Groups: {len(groups)}")
    for idx, group in enumerate(groups, start=1):
        print(f"Group {idx}: ID = {group['id']}, Name = {group['name']}, Screen Name = {group['screen_name']}")
