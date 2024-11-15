from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Session, sessionmaker, DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)
    pass


class Query(Base):
    __tablename__ = 'tQueries'
    text: Mapped[str]
    posts: Mapped[list['Post']] = relationship(back_populates='query', uselist=True)


class Owner(Base):
    __tablename__ = 'tOwners'
    domain: Mapped[str]
    type: Mapped[str]
    url: Mapped[str]
    name: Mapped[str | None]
    first_name: Mapped[str | None]
    last_name: Mapped[str | None]
    posts: Mapped[list['Post']] = relationship(back_populates='owner', uselist=True)

    def __repr__(self) -> str:
        if self.type == 'user':
            return f'User:{self.id}:{self.last_name}_{self.first_name}'
        return f'Group:{self.id}:{self.name}'


class Post(Base):
    __tablename__ = 'tPosts'
    type: Mapped[str | None]
    date: Mapped[datetime]
    from_id: Mapped[int]
    views_cnt: Mapped[int | None]
    likes_cnt: Mapped[int | None]
    comments_cnt: Mapped[int | None]
    reposts_cnt: Mapped[int | None]
    text: Mapped[str]
    url: Mapped[str]
    query: Mapped['Query'] = relationship(back_populates='posts', uselist=False)
    query_id: Mapped[int] = mapped_column(ForeignKey('tQueries.id'))
    owner: Mapped['Owner'] = relationship(back_populates='posts', uselist=False)
    owner_id: Mapped[int] = mapped_column(ForeignKey('tOwners.id'))
    links: Mapped[list['Link']] = relationship(back_populates='post', uselist=True)
    photos: Mapped[list['Photo']] = relationship(back_populates='post', uselist=True)
    videos: Mapped[list['Video']] = relationship(back_populates='post', uselist=True)

    def __repr__(self) -> str:
        return f'Post:{self.id}:From_{self.from_id}:Date_{self.date}'


class Link(Base):
    __tablename__ = 'tLinks'
    title: Mapped[str]
    url: Mapped[str]
    caption: Mapped[str | None]
    description: Mapped[str | None]
    post: Mapped['Post'] = relationship(back_populates='links', uselist=False)
    post_id: Mapped[int] = mapped_column(ForeignKey('tPosts.id'))


class Photo(Base):
    __tablename__ = 'tPhotos'
    date: Mapped[datetime]
    url: Mapped[str]
    text: Mapped[str]
    owner_id: Mapped[int]
    post: Mapped['Post'] = relationship(back_populates='photos', uselist=False)
    post_id: Mapped[int] = mapped_column(ForeignKey('tPosts.id'))


class Video(Base):
    __tablename__ = 'tVideos'
    date: Mapped[datetime]
    title: Mapped[str]
    description: Mapped[str | None]
    views_cnt: Mapped[int | None]
    comments_cnt: Mapped[int | None]
    duration: Mapped[int | None]
    owner_id: Mapped[int]
    post: Mapped['Post'] = relationship(back_populates='videos', uselist=False)
    post_id: Mapped[int] = mapped_column(ForeignKey('tPosts.id'))
