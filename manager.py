from queue import Queue
from threading import Thread
from datetime import datetime

from config import AppConfig
from executors import ApiParser, DataExecutor
from logger import ManagerLogger


class AppManager:
    """

    """
    def __init__(self, api_v: str, token: str, query: str):
        self.logger = ManagerLogger()
        self.queue = Queue()
        self.parser = ApiParser(api_v, token, query, self.queue)
        self.executor = DataExecutor()

    def listen_queue(self, stop_date=None):
        while True:
            wall = self.queue.get()
            if wall is None:
                break
            self.parser.check_wall(wall['owner'], wall['wall_json'], 100)
            self.parser.create_owner_posts(wall['owner'], wall['wall_json']['items'], stop_date)
        self.parser.create_owners()

    def export_result(self):
        insert_data = self.parser.owners + self.parser.posts
        self.executor.export_data(insert_data)

    def run(self):
        """

        """
        start = datetime.now()
        Thread(target=self.parser.run, args=(users, groups)).start()
        self.listen_queue()
        self.export_result()
        self.logger.info(f"\n\n   Total time: {datetime.now() - start}\n\n")


users = ['taxi8308', 'akozhedub', 'agekalo']
# users = []
groups = ['pva_anapa', 'anapavputi']
# groups = [
#     "anapagorod",
#     "anapavputi",
#     "pva_anapa",
#     "be.smart",
#     "coub",
#     "spb.news.rayoni",
#     "club31819673",
#     "kuban24_tv",
#     "ruopp"
# ]
# groups = ['pva_anapa']
QUERY = ['видео']


def start_service():
    app_manager = AppManager(AppConfig.get_vk_api_version(), AppConfig.get_vk_access_token(), 'Путин')
    app_manager.run()


if __name__ == '__main__':
    start_service()
