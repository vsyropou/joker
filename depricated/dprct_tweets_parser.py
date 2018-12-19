
import re
import numpy as np
from text_processing import parse_multi_json_file as parse_json
from text_processing import json_tweets_length_quantile
from text_processing import remove_urls

# parse json
tweets_file_path = "/home/vsyropou/workdir/projects/bit-curves/billys.json"

tweets = parse_json(tweets_file_path)

# 90% quantile tweet length
tweets_length = json_tweets_length_quantile(tweets, .9)


# remove urls
tweets = remove_urls(tweets)

