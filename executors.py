from datetime import datetime
from queue import Queue
from threading import Thread, Lock
import asyncio
import aiohttp

from vk_api.exceptions import VkApiError
import vk_api
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from config import AppConfig
from models import Query, Owner, Post, Link, Photo, Video
from logger import ParserLogger


class SearchOwner:
    """

    """
    def __init__(self, _id: int, domain: str):
        self.id = _id
        self.domain = domain
        self.is_active = True

    def __repr__(self):
        return f'Owner: {self.id}_{self.domain}'


class ApiParser:
    """

    """

    def __init__(self, version: str, token: str, query: str, queue: Queue):
        self.logger = ParserLogger()
        self.domain = 'https://vk.com/'
        self.api_url = 'https://api.vk.com/method/wall.search'
        self.headers = self.get_headers()
        self.queue = queue
        self.lock = Lock()

        self.version = version
        self.token = token

        self.api = vk_api.VkApi(token=token, api_version=version)

        self.loader = DataExecutor()
        self.query = query
        self.query_id = self.loader.get_query_id(self.query)
        self.search_owners = self.loader.get_search_owners()
        self.post_ids = self.loader.select_ids(Post)
        self.photo_ids = self.loader.select_ids(Photo)
        self.video_ids = self.loader.select_ids(Video)
        self.active_owners = len(self.search_owners)
        self.posts = []
        self.ext_user_ids = []
        self.ext_group_ids = []
        self.ext_posts = []

    @staticmethod
    def get_headers():
        """

        """
        headers = {
            'Authorization': 'Bearer ' + AppConfig.get_vk_access_token(),
            'Content-Type': 'multipart/form-data'
        }
        return headers

    @staticmethod
    def convert_list_to_str(source: list) -> str:
        """

        """
        source = [str(el) for el in source]
        return ', '.join(source)

    def create_owners(self, user_ids=None, group_ids=None) -> list[Owner]:
        """

        """
        owners_obj_list = []
        if user_ids:
            owners_obj_list.extend(self.create_users(user_ids))
        if group_ids:
            owners_obj_list.extend(self.create_groups(group_ids))
        return owners_obj_list

    def check_owner_exists(self, owner_id) -> bool:
        """

        """
        for owner in self.search_owners:
            if owner_id == owner.id:
                return True
        return False

    def get_error_message(self, owner: dict, owner_type: str, exists: bool) -> None:
        """

        """
        domain = owner['domain'] if owner_type == 'User' else owner['screen_name']
        deactivated = owner['deactivated'] if 'deactivated' in owner.keys() else None
        message = f"{owner_type} {owner['id']}_{domain}: "
        message = message + ' already exists.' if exists else message
        message = message + ' is closed.' if bool(owner['is_closed']) else message
        message = message + f' deactivated: {deactivated}' if deactivated is not None else message
        self.logger.info(message)

    def create_users(self, user_ids: list) -> list[Owner]:
        """

        """
        users_json = self.api.method(
            method='users.get', values={'user_ids': self.convert_list_to_str(user_ids), 'fields': 'domain'}
        )
        user_obj_list = []
        for user in users_json:
            is_exists = self.check_owner_exists(user['id'])
            if any([
                is_exists,
                bool(user['is_closed']),
                'deactivated' in user.keys()
            ]):
                self.get_error_message(user, 'User', is_exists)
            else:
                user_obj_list.append(
                    Owner(
                        id=user['id'],
                        domain=user['domain'],
                        type='user',
                        url=self.domain + user['domain'],
                        first_name=user['first_name'],
                        last_name=user['last_name']
                    )
                )
                owner = SearchOwner(user['id'], user['domain'])
                self.search_owners.append(owner)
                self.active_owners += 1
        return user_obj_list

    def create_groups(self, group_ids: list) -> list[Owner]:
        """

        """
        groups_json = self.api.method(
            method='groups.getById', values={'group_ids': self.convert_list_to_str(group_ids)}
        )['groups']
        group_obj_list = []
        for group in groups_json:
            is_exists = self.check_owner_exists(-group['id'])
            if any([
                is_exists,
                bool(group['is_closed']),
                'deactivated' in group.keys()
            ]):
                self.get_error_message(group, 'Group', is_exists)
            else:
                group_obj_list.append(
                    Owner(
                        id=-group['id'],
                        domain=group['screen_name'],
                        type='group',
                        url=self.domain + group['screen_name'],
                        name=group['name'],
                    )
                )
                owner = SearchOwner(group['id'], group['screen_name'])
                self.search_owners.append(owner)
                self.active_owners += 1
        return group_obj_list

    async def search_owner_posts(self, owner: SearchOwner, count: int, offset: int) -> None:
        """

        """
        try:
            self.logger.info(f'Search - {owner}')
            param = {
                'domain': owner.domain,
                'query': self.query,
                'count': count,
                'offset': offset,
                'v': AppConfig.get_vk_api_version()
            }
            async with aiohttp.ClientSession() as session:
                async with session.post(url=self.api_url, params=param, headers=self.headers) as response:
                    wall_json = await response.json()
            self.logger.info(f"Post count - {owner}: {len(wall_json['response']['items'])}")
            self.queue.put({'owner': owner, 'wall_json': wall_json['response']})
        except VkApiError as e:
            with self.lock:
                owner.is_active = False
                self.active_owners -= 1
                self.logger.error(f"Deactivate - {owner}: {str(e)}")

    async def search_owner_wall(self, count=None, offset=None) -> None:
        """

        """
        if self.active_owners == 0:
            self.queue.put(None)
        foo_list = []
        for owner in self.search_owners:
            if owner.is_active:
                foo_list.append(self.search_owner_posts(owner, count, offset))
        await asyncio.gather(*foo_list)

    def check_wall(self, owner: SearchOwner, wall: dict, count: int):
        """

        """
        if wall['count'] == 0:
            owner.is_active = False
            self.active_owners -= 1
            self.logger.info(f"Deactivate - {owner}: no posts")
        else:
            if wall['count'] < count:
                owner.is_active = False
                self.active_owners -= 1
                self.logger.info(f"Deactivate - {owner}: post count < {count}")

    def prepare_text(self, source: str) -> str:
        """

        """
        if source is not None:
            query_fragment = self.get_query_fragment(source)
            return query_fragment if query_fragment is not None else self.get_text_slice(source)

    @staticmethod
    def get_text_slice(text: str) -> str:
        """

        """
        if len(text) > 30:
            return text[:27] + "..."
        return text

    def get_query_fragment(self, text: str) -> str:
        """

        """
        query = self.query
        start_index = text.find(query)
        if start_index != -1:
            start_slice = max(0, start_index - 15)
            end_slice = min(len(text), start_index + len(query) + 15)
            return text[start_slice:end_slice]

    def create_owner_posts(self, owner: SearchOwner, post_list: list, stop_date) -> None:
        """

        """
        if post_list:
            post_count = 0
            for post in post_list:
                if stop_date is not None and post['date'] < stop_date:
                    owner.is_active = False
                    self.active_owners -= 1
                    self.logger.info(
                        f"Deactivate - {owner}: post out of date {str(datetime.utcfromtimestamp(post['date']))}"
                    )
                else:
                    post_obj = self.create_post(post)
                    if post_obj is not None:
                        if 'attachments' in post.keys():
                            self.set_post_attachments(post_obj, post['attachments'])
                        if 'copy_history' in post.keys():
                            self.post_history_process(post['copy_history'])
                        self.posts.append(post_obj)
                        post_count += 1
            self.logger.info(f"Created posts - {owner}: {post_count}")

    def create_post(self, post: dict) -> Post:
        """

        """
        if post['id'] not in self.post_ids:
            views_cnt = post['views']['count'] if 'views' in post.keys() else None
            likes_cnt = post['likes']['count'] if 'likes' in post.keys() else None
            comments_cnt = post['comments']['count'] if 'comments' in post.keys() else None
            reposts_cnt = post['reposts']['count'] if 'reposts' in post.keys() else None
            post_obj = Post(
                id=post['id'],
                query_id=self.query_id,
                type=post.get('post_type', None),
                date=datetime.utcfromtimestamp(post['date']),
                from_id=post['from_id'],
                views_cnt=views_cnt,
                likes_cnt=likes_cnt,
                comments_cnt=comments_cnt,
                reposts_cnt=reposts_cnt,
                text=self.prepare_text(post['text']),
                owner_id=post['owner_id'],
                url=f"{self.domain}wall{post['owner_id']}_{post['id']}"
            )
            self.post_ids.append(post['id'])
            return post_obj

    def set_post_attachments(self, post: object, attachments: list) -> None:
        """

        """
        if attachments:
            # links_count = 0
            # photos_count = 0
            # videos_count = 0
            for attachment in attachments:
                if attachment['type'] == 'link':
                    post.links.append(self.create_link_obj(attachment['link']))
                    # links_count += 1
                elif attachment['type'] == 'photo':
                    photo = self.create_photo_obj(attachment['photo'])
                    if photo is not None:
                        post.photos.append(photo)
                        # photos_count += 1
                elif attachment['type'] == 'video':
                    video = self.create_video_obj(attachment['video'])
                    if video is not None:
                        post.videos.append(video)
                        # videos_count += 1
            # print('Created links: ', links_count)
            # print('Created photos: ', photos_count)
            # print('Created videos: ', videos_count)

    def create_link_obj(self, attachment: dict) -> Link:
        """

        """
        link = Link(
            title=self.prepare_text(attachment['title']),
            url=attachment['url'],
            caption=attachment.get('caption', None),
            description=self.prepare_text(attachment.get('description', None))
        )
        return link

    def create_photo_obj(self, attachment: dict) -> Photo:
        """

        """
        url = attachment['orig_photo']['url'] if 'orig_photo' in attachment.keys() \
            else self.choose_photo_max_size(attachment['sizes'])
        if attachment['id'] not in self.photo_ids:
            photo = Photo(
                id=attachment['id'],
                date=datetime.utcfromtimestamp(attachment['date']),
                url=url,
                text=self.prepare_text(attachment['text']),
                owner_id=attachment['owner_id']
            )
            self.photo_ids.append(photo.id)
            return photo

    def create_video_obj(self, attachment: dict) -> Video:
        """

        """
        if attachment['id'] not in self.video_ids:
            video = Video(
                id=attachment['id'],
                date=datetime.utcfromtimestamp(attachment['date']),
                title=self.prepare_text(attachment['title']),
                description=self.prepare_text(attachment.get('description', None)),
                views_cnt=attachment.get('views', None),
                comments_cnt=attachment.get('comments', None),
                duration=attachment.get('duration', None),
                owner_id=attachment['owner_id']
            )
            self.video_ids.append(video.id)
            return video

    def post_history_process(self, posts: list):
        """

        """
        for post in posts:
            post_obj = self.create_post(post)
            if post_obj is not None:
                post_obj.owner_id = post['owner_id']
                if not self.check_owner_exists(post['owner_id']):
                    self.add_owner_id(post['owner_id'])
                if 'attachments' in post.keys():
                    self.set_post_attachments(post_obj, post['attachments'])
                self.ext_posts.append(post_obj)

    def add_owner_id(self, owner_id: int) -> None:
        """

        """
        if owner_id >= 0:
            self.ext_user_ids.append(owner_id)
        else:
            self.ext_group_ids.append(abs(owner_id))

    @staticmethod
    def choose_photo_max_size(sizes: dict):
        """

        """
        max_size = max(sizes, key=lambda x: x['width'])
        return max_size['url']


class DataExecutor:
    """

    """
    __con_str = (f'{AppConfig.get_base_provider_name()}:/'
                 f'/{AppConfig.get_user_id()}:{AppConfig.get_user_pwd()}@{AppConfig.get_db_host()}:'
                 f'{AppConfig.get_db_port()}/{AppConfig.get_db_name()}')

    def __init__(self):
        self.engine = create_engine(url=self.__con_str, echo=False)
        self.factory = sessionmaker(self.engine, expire_on_commit=True)

    def get_query_id(self, query_str: str) -> int:
        """

        """
        with self.factory() as session:
            stmt = select(Query.id, Query.text)
            result = session.execute(stmt).all()
            for tpl in result:
                if tpl[1] == query_str:
                    return tpl[0]
            query = Query(text=query_str)
            session.add(query)
            session.commit()
            return query.id

    def select_ids(self, model) -> list[int]:
        """

        """
        with self.factory() as session:
            query = select(model.id)
            result = session.execute(query).all()
            if result:
                return [tpl[0] for tpl in result]
            return []

    def get_search_owners(self) -> list[SearchOwner]:
        """

        """
        with self.factory() as session:
            query = select(Owner.id, Owner.domain)
            result = session.execute(query).all()
            owners = []
            if result:
                for tpl in result:
                    owner = SearchOwner(tpl[0], tpl[1])
                    owners.append(owner)
            return owners

    def export_data(self, data: list) -> None:
        """

        """
        with self.factory() as session:
            session.add_all(data)
            session.commit()
