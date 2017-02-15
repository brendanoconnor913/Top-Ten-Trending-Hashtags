import oauth2
import json
import random

# Twitter provided access key and secret
CONSUMER_KEY = "v6pdySQEJaFvJUiDAbYcDY4Qv"
CONSUMER_SECRET = "US2MtTXN2yasngF87ccmBABdWHUCllCGmH3p5iCOLWiaFMObLx"

# Twitter provided function to submit authenticated requests for data
def oauth_req(url, key, secret, http_method="GET", http_headers=None):
    consumer = oauth2.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
    token = oauth2.Token(key=key, secret=secret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, headers=http_headers)
    return content

out1 = open("output1.txt", "a") #output files
out2 = open("output2.txt", "a")
out3 = open("output3.txt", "a")
out4 = open("output4.txt", "a")
words = open("/usr/share/dict/words", "r") #dict of words to random search for
word = words.readlines()
i = 0

# Loop to collect 100k tweets if possible (typically rate limited)
while i < 100000:
    randword = random.choice(word) # random word used to search twitter for tweets
    print str(i)
    print randword

    home_timeline = oauth_req('https://api.twitter.com/1.1/search/tweets.json?q='+ randword +'&count=100',
                              '830451969345122305-5zzMdpdnH5TDI5Sk6wEj3ju9rXA4VC6',
                              '7LK2eUwasqPHlRJOmvAbCz4vDZEY9T5VOR2ZXqCo4sKBs')
    tweets = json.loads(home_timeline)  # load tweets into json parser

    # Iterate through tweets and output each tweet to a file, 1 tweet per line
    # Split output among 4 files to avoid one giant file
    for t in tweets["statuses"]:
        tweet = str(t) + "\n"
        if i % 4 == 0:
            out1.write(tweet)
        elif i % 4 == 1:
            out2.write(tweet)
        elif i % 4 == 2:
            out3.write(tweet)
        elif i % 4 == 3:
            out4.write(tweet)
        i += 1

out1.close()
out2.close()
out3.close()
out4.close()
words.close()
