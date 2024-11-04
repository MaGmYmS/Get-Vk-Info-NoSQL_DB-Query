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
            'v': '5.131',
            'user_id': self.user_id
        }

    def get_user_info(self):
        """
        Получает информацию о пользователе ВКонтакте.

        :return: Словарь с информацией о пользователе, включая количество фолловеров и подписок.
        """
        params = {**self.base_params, 'fields': 'followers_count,subscriptions'}
        response = requests.get('https://api.vk.com/method/users.get', params=params)
        user_info = response.json().get('response', [{}])[0]
        return {
            'id': user_info.get('id'),
            'first_name': user_info.get('first_name'),
            'last_name': user_info.get('last_name'),
            'followers_count': user_info.get('followers_count'),
            'subscriptions': user_info.get('subscriptions')
        }

    def get_friends_and_requests(self):
        import requests
        """
        Получает список друзей и подписчиков пользователя ВКонтакте.

        :return: Словарь с объединенным списком 'friends_and_requests'.
        """
        friends_response = requests.get('https://api.vk.com/method/friends.get', params=self.base_params)
        friends = friends_response.json().get('response', {}).get('items', [])

        requests_response = requests.get('https://api.vk.com/method/friends.getRequests', params=self.base_params)
        requests = requests_response.json().get('response', {}).get('items', [])

        # Объединяем списки
        combined_list = friends + requests

        return combined_list

    def get_groups(self):
        """
        Получает список групп, в которых состоит пользователь ВКонтакте.

        :return: Список словарей с идентификаторами и именами групп.
        """
        response = requests.get('https://api.vk.com/method/groups.get', params=self.base_params)
        group_ids = response.json().get('response', {}).get('items', [])
        return self.__get_group_details(group_ids)

    def __get_group_details(self, group_ids):
        """
        Получает подробную информацию о группах по их идентификаторам, включая имя.

        :param group_ids: Список идентификаторов групп.
        :return: Список словарей с данными групп (идентификаторы и имена).
        """
        if not group_ids:
            return []

        params = {**self.base_params, 'group_ids': ','.join(map(str, group_ids)), 'fields': 'name'}
        response = requests.get('https://api.vk.com/method/groups.getById', params=params)
        group_details = response.json().get('response', [])

        # Извлекаем нужные данные и формируем список словарей
        return [{'id': group.get('id'), 'name': group.get('name')} for group in group_details]


if __name__ == "__main__":
    token = os.getenv('VK_ACCESS_TOKEN')
    if not token:
        raise ValueError("Переменная VK_ACCESS_TOKEN не найдена")

    getVkInfo = GetVkInfo(user_id='326621197', vk_token=token)

    user_info = getVkInfo.get_user_info()
    followers = getVkInfo.get_friends_and_requests()
    groups = getVkInfo.get_groups()

    print(user_info)
    print(len(followers))
    print(len(groups))
