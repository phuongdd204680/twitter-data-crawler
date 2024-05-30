class TwitterUser:
    blue = "blue"
    blue_type = "blueType"
    created_at = "created"
    timestamp = "timestamp"
    description_links = "descriptionLinks"
    id_ = "_id"
    url = 'url'
    user_name = "userName"
    display_name = "displayName"
    raw_description = "rawDescription"
    followers_count = "followersCount"
    friends_count = "friendsCount"
    statuses_count = "statusesCount"
    favourites_count = "favouritesCount"
    listed_count = "listedCount"
    media_count = "mediaCount"
    location = "location"
    country = "country"
    profile_image_url = "profileImageUrl"
    profile_banner_url = "profileBannerUrl"
    protected = "protected"
    verified = "verified"
    count_logs = "countLogs"
    interaction_logs = "interactionLogs"
    interaction_change_logs = "interactionChangeLogs"


class Tweets:
    id_ = "_id"
    author = "author"
    author_name = 'authorName'
    created_at = "created"
    timestamp = "timestamp"
    hash_tags = "hashTags"
    likes = "likes"
    reply_counts = "replyCounts"
    retweet_counts = "retweetCounts"
    retweeted_tweet = "retweetedTweet"
    quoted_tweet = "quotedTweet"
    text = "text"
    tweet_body = "tweetBody"
    url = "url"
    views = "views"
    user_mentions = "userMentions"
    impression_logs = "impressionLogs"
    key_word = "keyWords"


class Follow:
    id_ = "_id"
    from_ = "from"
    to = "to"


class Projects:
    mapping = {
        "trava": "trava_finance"
    }
