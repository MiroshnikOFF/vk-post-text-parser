from datetime import datetime
from queue import Queue

from vk_api.exceptions import ApiError, ApiHttpError
import vk_api
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from config import AppConfig
from models import Query, Owner, Post, Link, Photo, Video
from logger import ParserLogger


class SearchOwner:
    """

    """
    def __init__(self, _id: int, domain: str, _type: str, first_name: str, is_closed: bool, last_name=None):
        self.id = _id
        self.domain = domain
        self.type = _type,
        self.first_name = first_name,
        self.last_name = last_name
        self.is_closed = is_closed
        self.is_active = not is_closed

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
        self.version = version
        self.token = token
        self.api = vk_api.VkApi(token=token, api_version=version)
        self.loader = DataExecutor()
        self.query = query
        self.query_id = self.loader.get_query_id(self.query)
        self.search_owners = []
        self.post_ids = self.loader.select_ids(Post)
        self.photo_ids = self.loader.select_ids(Photo)
        self.video_ids = self.loader.select_ids(Video)
        self.active_owners = 0
        self.posts = []
        self.ext_user_ids = []
        self.ext_group_ids = []
        self.owners = []
        self.owner_ids = []
        # self.posts_count = 0

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

    def get_vk_owners(self, user_ids: list, group_ids: list) -> dict:
        """

        """
        code = ('return [API.users.get({"user_ids": ' +
                str(user_ids) +
                ', "fields": "domain"}), API.groups.getById({"group_ids": ' +
                str(group_ids) +
                '})];')
        owners_json = self.api.method(method='execute', values={'code': code})
        return owners_json

    def add_search_owners(self, user_ids=None, group_ids=None) -> None:
        """

        """
        owners_exists = self.loader.get_owners()
        if user_ids is None and group_ids is None:
            self.search_owners = owners_exists
        else:
            vk_owners_json = self.get_vk_owners(user_ids, group_ids)
            self.search_owners = list(set(self.create_users(vk_owners_json[0]) +
                                          self.create_groups(vk_owners_json[1]['groups']) +
                                          owners_exists))
        for owner in self.search_owners:
            if owner.is_active:
                self.active_owners += 1

    def create_users(self, users_json: dict) -> list[SearchOwner]:
        """

        """
        search_users = []
        for user in users_json:
            is_exists = self.check_owner_exists(user['id'])
            is_closed = bool(user['is_closed']) or 'deactivated' in user.keys()
            if not is_exists:
                search_user = SearchOwner(
                        _id=user['id'],
                        domain=user['domain'],
                        _type='user',
                        first_name=user['first_name'],
                        last_name=user['last_name'],
                        is_closed=is_closed
                    )
                if is_closed:
                    self.get_error_message(user, 'User', is_exists)
                    search_user.is_active = False
                search_users.append(search_user)
        return search_users

    def create_groups(self, groups_json: dict) -> list[SearchOwner]:
        """

        """
        search_groups = []
        for group in groups_json:
            is_exists = self.check_owner_exists(-group['id'])
            is_closed = bool(group['is_closed']) or 'deactivated' in group.keys()
            if not is_exists:
                search_group = SearchOwner(
                        _id=-group['id'],
                        domain=group['screen_name'],
                        _type='group',
                        first_name=group['name'],
                        is_closed=is_closed
                    )
                if is_closed:
                    self.get_error_message(group, 'Group', is_exists)
                    search_group.is_active = False
                search_groups.append(search_group)
        return search_groups

    def create_owners(self) -> None:
        """

        """
        exists_owner_ids = self.loader.get_owner_ids()
        for owner in self.search_owners:
            if owner.id not in exists_owner_ids and owner.id not in self.owner_ids:
                self.owners.append(
                    Owner(
                        id=owner.id,
                        domain=owner.domain,
                        type=owner.type,
                        url=self.domain + owner.domain,
                        name=owner.first_name if owner.last_name is None else None,
                        first_name=owner.first_name if owner.last_name is not None else None,
                        last_name=owner.last_name,
                        is_closed=owner.is_closed
                    )
                )
                self.owner_ids.append(owner.id)

    def ran_search_owner_wall(self, count=None, offset=None) -> None:
        """

        """
        attempts = 1
        stack = [self.search_owners]
        while stack:
            current_list = stack.pop()
            try:
                if self.active_owners > 0:
                    self.search_owner_wall(current_list, count, offset)
                else:
                    self.queue.put(None)
                    break
            except ApiError as e:
                attempts += 1
                self.logger.error(f"Api error: {e}. Try: {attempts}")
                mid_index = len(current_list) // 2
                stack.append(current_list[:mid_index])
                stack.append(current_list[mid_index:])

    def search_owner_wall(self, search_owners: list[SearchOwner], count, offset) -> None:
        """

        """
        owner_walls = {}
        code_list = []
        for owner in search_owners:
            if owner.is_active:
                code_list.append(
                    ('API.wall.search({"domain": "' + owner.domain +
                     '", "query": "' + self.query +
                     '", "count": ' + str(count) +
                     ', "offset": ' + str(offset) +
                     '})')
                )
                owner_walls[owner] = None
        code = 'return ' + str(code_list).replace("'", "") + ';'
        attempts = 1
        while True:
            try:
                walls_json = self.api.method(method='execute', values={'code': code})
                owner_walls.update(dict(zip(owner_walls.keys(), walls_json)))
                self.put_owner_posts(owner_walls)
                break
            except ApiHttpError as e:
                attempts += 1
                self.logger.error(f"ApiHttpError: {e}. Try: {attempts}")

    def put_owner_posts(self, owner_walls: dict) -> None:
        """

        """
        for owner, wall in owner_walls.items():
            self.logger.info(f'Search - {owner}')
            if isinstance(wall, dict):
                if 'error' in wall.keys():
                    error = wall['error']
                    self.logger.error(f"Error {error['error_code']}: {error['error_msg']}")
                    owner.is_active = False
                    self.active_owners -= 1
                else:
                    self.logger.info(f"Post count - {owner}: {wall['count']}")
                    self.queue.put({'owner': owner, 'wall_json': wall})

    def check_wall(self, owner: SearchOwner, wall: dict, count: int):
        """

        """
        if wall['count'] == 0:
            owner.is_active = False
            self.active_owners -= 1
            self.logger.info(f"Deactivate - {owner}: no posts")
        elif wall['count'] < count:
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
            for attachment in attachments:
                if attachment['type'] == 'link':
                    post.links.append(self.create_link_obj(attachment['link']))
                elif attachment['type'] == 'photo':
                    photo = self.create_photo_obj(attachment['photo'])
                    if photo is not None:
                        post.photos.append(photo)
                elif attachment['type'] == 'video':
                    video = self.create_video_obj(attachment['video'])
                    if video is not None:
                        post.videos.append(video)

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
                if not self.check_owner_exists(post['owner_id']):
                    self.add_owner_id(post['owner_id'])
                if 'attachments' in post.keys():
                    self.set_post_attachments(post_obj, post['attachments'])
                self.posts.append(post_obj)

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

    def run(self, user_ids=None, group_ids=None):
        self.add_search_owners(user_ids, group_ids)
        _offset = 0
        while True:
            iter_cnt = 1
            while self.active_owners > 0:
                self.logger.info(f'\n\nIter: {iter_cnt}  Active owners: {self.active_owners}\n')
                self.ran_search_owner_wall(100, _offset)
                _offset += 100
                iter_cnt += 1
                self.create_owners()
            if self.ext_user_ids or self.ext_group_ids:
                # print(f"{list(set(self.ext_user_ids))=}")
                # print(f"{list(set(self.ext_group_ids))=}")
                self.add_search_owners(list(set(self.ext_user_ids)), list(set(self.ext_group_ids)))
                self.ext_group_ids = []
                self.ext_user_ids = []
            else:
                self.queue.put(None)
                break


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

    def get_owner_ids(self) -> list[int]:
        """

        """
        with self.factory() as session:
            query = select(Owner.id)
            result = session.execute(query).all()
            if result:
                return [row[0] for row in result]
            return []

    def get_owners(self) -> list[SearchOwner]:
        """

        """
        with self.factory() as session:
            query = select(Owner)
            result = session.execute(query).all()
            owners = []
            if result:
                for row in result:
                    owners.append(
                        SearchOwner(
                            _id=row[0].id,
                            domain=row[0].domain,
                            _type=row[0].type,
                            first_name=row[0].first_name,
                            last_name=row[0].last_name,
                            is_closed=row[0].is_closed
                        )
                    )
            return owners

    def export_data(self, data: list) -> None:
        """

        """
        with self.factory() as session:
            session.add_all(data)
            session.commit()
