from datetime import datetime
from queue import Queue
import asyncio
from time import sleep
from threading import Thread

from config import AppConfig
from models import Base, Query, Owner, Post, Link, Photo, Video
from executors import SearchOwner, ApiParser, DataExecutor
from logger import ManagerLogger


class AppManager:
    """

    """

    def __init__(self, api_v: str, token: str, user_ids: list, group_ids: list, query: str):
        self.logger = ManagerLogger()
        self.queue = Queue()
        self.parser = ApiParser(api_v, token, query, self.queue)
        self.executor = DataExecutor()
        self.user_ids = user_ids
        self.group_ids = group_ids
        self.owners = []

    def update_exp_owners(self, ext_owners: list[Owner]) -> None:
        """

        """
        for owner in ext_owners:
            self.add_ext_posts(owner)

    def add_ext_posts(self, owner: Owner) -> None:
        """

        """
        for post in self.parser.ext_posts:
            if post.owner_id == owner.id:
                owner.posts.append(post)

    def export_result(self):
        ext_owners = self.parser.create_owners(self.parser.ext_user_ids, self.parser.ext_group_ids)
        self.update_exp_owners(ext_owners)
        insert_data = self.owners + ext_owners + self.parser.posts
        self.executor.export_data(insert_data)

    def parser_thread(self):
        _offset = 0
        stop_dt = datetime(2023, 1, 1, 0, 0, 0)
        unix_timestamp = stop_dt.timestamp()

        iter_cnt = 1
        while self.parser.active_owners > 0:
            self.logger.info(f'\n\nIter: {iter_cnt}  Active owners: {self.parser.active_owners}\n')
            asyncio.run(self.parser.search_owner_wall(100, _offset))
            _offset += 100
            iter_cnt += 1

    def listen_queue(self, stop_date=None):
        while self.parser.active_owners > 0:
            wall = self.queue.get()
            if wall is None:
                break
            self.parser.check_wall(wall['owner'], wall['wall_json'], 100)
            self.parser.create_owner_posts(wall['owner'], wall['wall_json']['items'], stop_date)

    def run(self):
        """

        """
        self.owners = self.parser.create_owners(self.user_ids, self.group_ids)
        self.logger.info(f'Search owners: {len(self.parser.search_owners)}')
        Thread(target=self.parser_thread).start()
        self.listen_queue()


        # for wall in list(self.queue.queue):
        #     print(wall)
        # for owner in self.parser.search_owners:
        #     print(owner)

        self.export_result()


users = ['taxi8308', 'akozhedub', 'agekalo']
# users = []
groups = ['pva_anapa', 'anapavputi']
# groups = ['pva_anapa']
QUERY = ['видео']

parser = AppManager(AppConfig.get_vk_api_version(), AppConfig.get_vk_access_token(), users, groups, 'Путин')



parser.run()

# Base.metadata.drop_all(engine)
# Base.metadata.create_all(engine)

    # def filter_owners(self):
    #     """
    #
    #     """
    #     self.filter_collection(self.owners, self.select_ids(Owner))
    #     for owner in self.owners:
    #         self.filter_collection(owner.posts, self.select_ids(Post))
    #
    #
    # @staticmethod
    # def filter_collection(collection: list, exists_ids: list):
    #     """
    #
    #     """
    #     for el in collection:
    #         if el.id in exists_ids:
    #             collection.pop(el)




# _iter = 50


# parser.search_owner_wall(count=100)

# Base.metadata.drop_all(engine)
# Base.metadata.create_all(engine)
#


# parser.set_all_ids()
# print(f'{parser.owner_ids=}')
# print(f'{parser.post_ids=}')
# print(f'{parser.photo_ids=}')
# print(f'{parser.video_ids=}')
