import requests
import asyncio

import aiohttp

from config import AppConfig

url = 'https://api.vk.com/method/users.get'
params = {
    'user_ids': 'taxi8308,akozhedub,agekalo',
    'v': '5.199'
}
headers = {
    'Authorization': f'Bearer {AppConfig.get_vk_access_token()}',
    'Content-Type': 'multipart/form-data'
}

# res = requests.post(url=url, params=params, headers=headers)
# print(res.json()['response'])


async def get_users():
    async with aiohttp.ClientSession() as session:
        async with session.post(url=url, params=params, headers=headers) as resp:
            data = await resp.json()
            print(data['response'])


async def main():
    foo_list = [get_users(), get_users(), get_users(), get_users(), get_users(), get_users(), get_users(), get_users()]
    await asyncio.gather(*foo_list)


asyncio.run(main())