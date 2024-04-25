import asyncio
import time
import json
from typing import AsyncGenerator, TypeVar

import pycountry

from twscrape import User, Tweet, gather

from constants.config import AccountConfig
from constants.mongo_constant import MongoCollection
from constants.time_constant import TimeConstants
from constants.twitter import TwitterUser, Follow, Tweets, Projects
from src.crawler.new_api import NewAPi
from databases.mongodb import MongoDB
from src.jobs.cli_job import CLIJob
from utils.logger_utils import get_logger
from utils.time_utils import round_timestamp

T = TypeVar("T")
logger = get_logger('Twitter Project Crawling Job')


class TwitterProjectCrawlingJob(CLIJob):
    def __init__(
            self,
            interval: int,
            period: int,
            limit: int,
            projects: list,
            projects_file: str,
            exporter: MongoDB,
            collection: str,
            user_name: str = AccountConfig.USERNAME,
            password: str = AccountConfig.PASSWORD,
            email: str = AccountConfig.EMAIL,
            email_password: str = AccountConfig.EMAIL_PASSWORD,
            key: str = AccountConfig.KEY,
            session_id: str = 'twitter',
            stream_types: list = ["projects", "tweets", "followers"]
    ):
        super().__init__(interval, period, limit, retry=False)
        self.period = period
        self.limit = limit
        self.collection = collection
        self.stream_types = stream_types
        self.session_id = session_id
        self.key = key
        self.email_password = email_password
        self.email = email
        self.password = password
        self.user_name = user_name
        self.api = None
        self.exporter = exporter
        self.projects = projects
        self.projects_file = self.load_projects_from_file(projects_file) if self.load_projects_from_file(projects_file) is [] else projects

    @staticmethod
    def load_projects_from_file(projects_file: str) -> list:
        if projects_file is not None:
            with open(projects_file, 'r') as file:
                projects_data = json.load(file)
            return projects_data
        else:
            return []

    @staticmethod
    def convert_user_to_dict(user: User) -> dict:
        text = user.location
        country_name = ""
        for country in pycountry.countries:
            if country.name.lower() in text.lower():
                country_name = country.name
                break

        return {
            TwitterUser.id_: str(user.id),
            TwitterUser.user_name: user.username,
            TwitterUser.display_name: user.displayname,
            TwitterUser.url: user.url,
            TwitterUser.blue: user.blue,
            TwitterUser.blue_type: user.blueType,
            TwitterUser.created_at: str(user.created),
            TwitterUser.timestamp: int(user.created.timestamp()),
            TwitterUser.description_links: [str(i.url) for i in user.descriptionLinks],
            TwitterUser.favourites_count: user.favouritesCount,
            TwitterUser.friends_count: user.friendsCount,
            TwitterUser.listed_count: user.listedCount,
            TwitterUser.media_count: user.mediaCount,
            TwitterUser.followers_count: user.followersCount,
            TwitterUser.statuses_count: user.statusesCount,
            TwitterUser.raw_description: user.rawDescription,
            TwitterUser.verified: user.verified,
            TwitterUser.profile_image_url: user.profileImageUrl,
            TwitterUser.profile_banner_url: user.profileBannerUrl,
            TwitterUser.protected: user.protected,
            TwitterUser.location: user.location,
            TwitterUser.country: country_name,
            TwitterUser.count_logs: {
                round_timestamp(time.time()): {
                    TwitterUser.favourites_count: user.favouritesCount,
                    TwitterUser.friends_count: user.friendsCount,
                    TwitterUser.listed_count: user.listedCount,
                    TwitterUser.media_count: user.mediaCount,
                    TwitterUser.followers_count: user.followersCount,
                    TwitterUser.statuses_count: user.statusesCount,
                }
            }
        }

    def convert_tweets_to_dict(self, tweet: Tweet) -> dict:
        if not tweet:
            return {}
        result = {
            Tweets.id_: str(tweet.id),
            Tweets.author: str(tweet.user.id),
            Tweets.author_name: tweet.user.username,
            Tweets.created_at: str(tweet.date),
            Tweets.timestamp: tweet.date.timestamp(),
            Tweets.url: str(tweet.url),
            Tweets.user_mentions: {
                str(user.id): user.username for user in tweet.mentionedUsers
            },
            Tweets.views: tweet.viewCount,
            Tweets.likes: tweet.likeCount,
            Tweets.hash_tags: tweet.hashtags,
            Tweets.reply_counts: tweet.replyCount,
            Tweets.retweet_counts: tweet.retweetCount,
            Tweets.retweeted_tweet: self.convert_tweets_to_dict(tweet.retweetedTweet),
            Tweets.text: tweet.rawContent,
            Tweets.quoted_tweet: self.convert_tweets_to_dict(tweet.quotedTweet),
        }
        if time.time() - result.get(Tweets.timestamp) < self.period:
            result[Tweets.impression_logs] = {
                str(int(time.time())): {
                    Tweets.views: tweet.viewCount,
                    Tweets.likes: tweet.likeCount,
                    Tweets.reply_counts: tweet.replyCount,
                    Tweets.retweet_counts: tweet.retweetCount,
                }
            }

        return result

    @staticmethod
    def get_relationship(project, user):
        return {
            Follow.id_: f'{user}_{project}',
            Follow.from_: str(user),
            Follow.to: str(project)
        }

    async def gather(self, gen: AsyncGenerator[T, None], project, time_sleep: int = 1, n_items: int = 1000) -> int:
        tmp = 0
        async for x in gen:
            self.exporter.update_docs(MongoCollection.twitter_users, [self.convert_user_to_dict(x)])
            self.exporter.update_docs(MongoCollection.twitter_follows, [self.get_relationship(project, x.id)])
            tmp += 1
            if tmp and not (tmp % n_items):
                time.sleep(time_sleep)
        return tmp

    async def execute(self):
        api = NewAPi()
        await api.pool.add_account(
            self.user_name,
            self.password,
            self.email,
            self.email_password
        )
        await api.pool.login_all()

        # for project in self.projects:
        for project in self.projects_file:
            begin = time.time()
            # if project not in Projects.mapping:
            #     continue

            if "projects" in self.stream_types:
                logger.info(f"Crawling {project} project info")
                project_info = await api.user_by_login(Projects.mapping.get(project))
                self.exporter.update_docs(MongoCollection.twitter_users, [self.convert_user_to_dict(project_info)])
                logger.info(f"Crawl {project} projects info in {time.time() - begin}s")

            if "tweets" in self.stream_types:
                begin = time.time()
                logger.info(f"Crawling {project} tweets info")
                project_info = await api.user_by_login(project)
                if project_info is None:
                    continue
                if self.limit is None:
                    # Get all tweet
                    tweets = await gather(api.user_tweets(project_info.id, limit=project_info.statusesCount))
                else:
                    tweets = await gather(api.user_tweets(project_info.id, limit=self.limit))

                for tweet in tweets:
                    self.exporter.update_docs(self.collection, [self.convert_tweets_to_dict(tweet)])

                logger.info(f"Crawl {project} tweets info in {time.time() - begin}s")

            if "followers" in self.stream_types:
                logger.info(f"Crawling {project} followers info")
                follower_info = await api.user_by_login(Projects.mapping.get(project))
                await self.gather(api.followers(follower_info.id), follower_info.id, time_sleep=20)
                logger.info(f"Crawl {project} followers info in {time.time() - begin}s")

    def _execute(self, *args, **kwargs):
        begin = time.time()
        logger.info("Start execute twitter crawler")
        asyncio.run(self.execute())
        logger.info(f"Execute all streams in {time.time() - begin}s")
