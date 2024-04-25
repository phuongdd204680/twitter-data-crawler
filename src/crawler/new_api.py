import asyncio
import time
from typing import AsyncGenerator, TypeVar

from twscrape import API, parse_users
from twscrape.api import OP_Followers

from utils.logger_utils import get_logger

T = TypeVar("T")
logger = get_logger("New API Twitter GraphQl")

class NewAPi(API):
    async def followers_raw(self, uid: int, limit=-1, kv=None):
        op = OP_Followers
        kv = {"userId": str(uid), "count": 20, "includePromotedContent": False, **(kv or {})}
        ft = {"responsive_web_twitter_article_notes_tab_enabled": False}
        async for x in self._gql_items(op, kv, limit=limit, ft=ft):
            yield x

    async def followers(self, uid: int, limit=-1, kv=None):
        async for rep in self.followers_raw(uid, limit=limit, kv=kv):
            for x in parse_users(rep.json(), limit):
                yield x

    async def gather(self, gen: AsyncGenerator[T, None], time_sleep: int = 1, n_items: int = 1000) -> list[T]:
        items = []
        async for x in gen:
            items.append(x)
            total_n_items = len(items)
            if total_n_items and not (total_n_items % n_items):

                time.sleep(time_sleep)
        return items
