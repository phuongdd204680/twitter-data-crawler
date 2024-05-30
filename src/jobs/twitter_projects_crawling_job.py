import asyncio
import time
import json
import pycountry
from constants.time_constant import TimeConstants
from typing import AsyncGenerator, TypeVar
from twscrape import User, Tweet, gather
from keyphrase_vectorizers import KeyphraseCountVectorizer
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
            kw_model,
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
        self.projects_file = self.load_projects_from_file(projects_file) if projects_file is not None else projects
        self.kw_model = kw_model

    @staticmethod
    def load_projects_from_file(projects_file: str) -> list:
        with open(projects_file, 'r') as file:
            projects_data = json.load(file)
        return projects_data

    @staticmethod
    def convert_user_to_dict(user: User, interaction_logs, interaction_change_logs) -> dict:
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
            },
            TwitterUser.interaction_logs: {
                round_timestamp(time.time(), round_time=TimeConstants.A_HOUR): interaction_logs
            },
            TwitterUser.interaction_change_logs: {
                round_timestamp(time.time(), round_time=TimeConstants.A_HOUR): interaction_change_logs
            }
        }

    def convert_tweets_to_dict(self, tweet: Tweet, key_words: bool) -> dict:
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
            Tweets.retweeted_tweet: self.convert_tweets_to_dict(tweet.retweetedTweet, False),
            Tweets.text: tweet.rawContent,
            Tweets.quoted_tweet: self.convert_tweets_to_dict(tweet.quotedTweet, False),
        }
        if key_words == True:
            tweet_txt = result[Tweets.text]
            tweet_quoted_tweet = result[Tweets.quoted_tweet]
            if 'text' in tweet_quoted_tweet:
                txt = tweet_txt + '\n\n' + tweet_quoted_tweet['text']
            else:
                txt = tweet_txt
            timmme = time.time() - result.get(Tweets.timestamp)
            if (len(txt) > 50) & (timmme <= 14 * TimeConstants.A_DAY):
                try:
                    keywords = self.kw_model.extract_keywords(txt, vectorizer=KeyphraseCountVectorizer(), top_n=23,
                                                              use_mmr=True)
                    keywords_lst = list(map(lambda x: x[0], keywords))
                    result[Tweets.key_word] = keywords_lst
                except:
                    keywords = []

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

    @staticmethod
    def get_interaction_change_logs(document, field):
        now_time = str(round_timestamp(time.time(), round_time=3600))
        previous_time = str(round_timestamp(time.time(), round_time=3600) - TimeConstants.A_HOUR)
        if "interactionLogs" in document and previous_time in document["interactionLogs"] and now_time in document["interactionLogs"]:
            change_log = document["interactionLogs"][now_time].get(field, 0) - document["interactionLogs"][previous_time].get(field, 0)
        else:
            change_log = 0
        return change_log

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
                project_info = await api.user_by_login(project)
                if project_info is None:
                    continue
                if self.limit is None:
                    # Get all tweet
                    tweets = await gather(api.user_tweets(project_info.id, limit=project_info.statusesCount))
                else:
                    tweets = await gather(api.user_tweets(project_info.id, limit=self.limit))

                views = 0
                likes = 0
                reply_counts = 0
                retweet_counts = 0
                for tweet in tweets:
                    if tweet.date.timestamp() >= round_timestamp(time.time()) - TimeConstants.DAYS_2:
                        views += tweet.viewCount
                        likes += tweet.likeCount
                        reply_counts += tweet.replyCount
                        retweet_counts += tweet.retweetCount

                views_change_log = 0
                likes_change_log = 0
                reply_change_log = 0
                retweet_change_log = 0
                list_tweets = list(self.exporter.get_docs(self.collection))
                for document in list_tweets:
                    views_change_log = self.get_interaction_change_logs(document, "views")
                    likes_change_log = self.get_interaction_change_logs(document, "likes")
                    reply_change_log = self.get_interaction_change_logs(document, "reply_counts")
                    retweet_change_log = self.get_interaction_change_logs(document, "retweet_counts")

                interaction_logs = {
                    "views": views,
                    "likes": likes,
                    "reply_counts": reply_counts,
                    "retweet_counts": retweet_counts,
                }

                interaction_change_logs = {
                    "views": views_change_log,
                    "likes": likes_change_log,
                    "reply_counts": reply_change_log,
                    "retweet_counts": retweet_change_log,
                }

                self.exporter.update_docs(self.collection, [self.convert_user_to_dict(project_info, interaction_logs, interaction_change_logs)])
                logger.info(f"Crawl {project} projects info in {time.time() - begin}s")

            if "tweets" in self.stream_types:
                begin = time.time()
                # logger.info(f"Crawling {project} tweets info")
                project_info = await api.user_by_login(project)
                if project_info is None:
                    continue
                if self.limit is None:
                    # Get all tweet
                    tweets = await gather(api.user_tweets(project_info.id, limit=project_info.statusesCount))
                else:
                    tweets = await gather(api.user_tweets(project_info.id, limit=self.limit))

                for tweet in tweets:
                    self.exporter.update_docs(self.collection, [self.convert_tweets_to_dict(tweet, True)])

                # logger.info(f"Crawl {project} tweets info in {time.time() - begin}s")
                logger.info(f"Crawled {self.projects_file.index(project) + 1}/{len(self.projects_file)} projects")

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
