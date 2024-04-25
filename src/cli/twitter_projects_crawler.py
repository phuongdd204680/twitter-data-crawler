import click

from constants.config import AccountConfig
from constants.time_constant import TimeConstants
from constants.mongo_constant import MongoCollection
from databases.mongodb import MongoDB
from src.jobs.twitter_projects_crawling_job import TwitterProjectCrawlingJob
from utils.logger_utils import get_logger

logger = get_logger('Twitter Projects Crawler')


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-i', '--interval', default=TimeConstants.A_DAY, type=int, help='Sleep time')
@click.option('-pe', '--period', default=TimeConstants.DAYS_2, type=int, help='Sleep time')
@click.option('-li', '--limit', default=None, type=int, help='Sleep time')
@click.option('-o', '--output-url', default=None, type=str, help='mongo output url')
@click.option('-col', '--collection', default=MongoCollection.tweets, type=str, help='mongo output collection')
@click.option('-p', '--projects', default=["trava_finance"], type=str, help='project name', multiple=True)
@click.option('-pf', '--projects-file', default=None, type=str, help='projects file')
@click.option('-u', '--twitter-user', default=AccountConfig.USERNAME, show_default=True,
              type=str, help='Twitter user')
@click.option('-pw', '--twitter-password', default=AccountConfig.PASSWORD, show_default=True,
              type=str, help='Telegram API hash')
@click.option('-k', '--twitter-key', default=AccountConfig.KEY, show_default=True,
              type=str, help='Telegram API key')
@click.option('-e', '--email', default=AccountConfig.EMAIL, show_default=True,
              type=str, help='Twitter email')
@click.option('-ep', '--email-password', default=AccountConfig.EMAIL_PASSWORD, show_default=True,
              type=str, help='email password')
@click.option('-st', '--stream-types', default=["projects", "tweets", "followers"], show_default=True,
              type=str, multiple=True, help='Stream types: projects, tweets, followers')
def twitter_projects_crawler(interval, period, limit, collection, output_url, projects, projects_file, twitter_user,
                             twitter_password, email, email_password, stream_types, twitter_key):
    _exporter = MongoDB(connection_url=output_url, database="cdp_database")
    job = TwitterProjectCrawlingJob(
        interval=interval,
        period=period,
        limit=limit,
        projects=projects,
        projects_file=projects_file,
        exporter=_exporter,
        user_name=twitter_user,
        password=twitter_password,
        key=twitter_key,
        email=email,
        email_password=email_password,
        stream_types=stream_types,
        collection=collection,
    )
    job.run()
