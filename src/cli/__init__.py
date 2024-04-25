import click

from src.cli.twitter_projects_crawler import twitter_projects_crawler


@click.group()
@click.version_option(version='1.0.0')
@click.pass_context
def cli(ctx):
    # Command line
    pass


# Stream
cli.add_command(twitter_projects_crawler, "twitter_projects_crawler")
