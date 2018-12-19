
import re
import json
import numpy as np

def parse_multi_json_file(filepath):

    # read lines
    with open(filepath, 'r') as fp:
        parsed_lines = np.array([ln.rstrip() for ln in fp.readlines()])

    # iterate json objects
    parsed_tweets = []
    for json_object in parsed_lines:

        if len(json_object) == 0: continue

        try: # decode json
            parsed_tweets += [json.loads(json_object)]
        except Exception as err:
            print(err)
            ## TODO: print more infor from exception

    # filter empty tweets
    filtered_tweets = list(filter(lambda t: len(t['text'])>0, parsed_tweets))

    return np.array(filtered_tweets)


def json_tweets_length_quantile(tweets, qntile):

    splitted_tweet_text = map(lambda t: len(t['text'].split()), tweets)

    return np.quantile(list(splitted_tweet_text), qntile)

def remove_urls(tweets):

    url_filter = lambda txt: re.sub(r'^https?:\/\/.*[\r\n]*', '', txt, flags=MULTILINE)


    import pdb; pdb.set_trace()
    
    def url_filter_proxy(key,val):
        return url_filter(val) if key =='text' else val

    out_tweets = [{k:url_filter_proxy(k,v) for k,v in t.items()} for t in tweets]

    return np.array(out_tweets)
