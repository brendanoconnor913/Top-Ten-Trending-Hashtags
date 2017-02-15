import ast
import os
import hdfs
import operator


# Helper function to identify which hashtag category a tweet belongs to
def tagCategory(hashtags, word):
    for tup in hashtags:
        tag = tup[0]
        if tag == word:
            return word
    return "OTHER"

# Open tweet input file
fin = open("allout.txt", "r")
tweets = fin.readlines()

hashtagdict = {}

i = 0
for line in tweets:
    # t = json.loads(ast.literal_eval(line))
    tweet = ast.literal_eval(line)["text"]
    ht = ast.literal_eval(line)["entities"]['hashtags']
    print (str(i) + " : " + tweet)
    if (ht != []):
        h = 1
        for tag in ht:
            print ("hashtag #" + str(h) + " " + tag["text"])
            if (hashtagdict.has_key(tag["text"])):
                hashtagdict[tag["text"]] += 1
            else:
                hashtagdict[tag["text"]] = 1
            h += 1
    i += 1

hashtagsort = sorted(hashtagdict.items(), key=operator.itemgetter(1), reverse=True)
topten = hashtagsort[:10]

# topten = [(u'nonsenseengine', 530), (u'nonsense', 530), (u'KCAPinoyStar', 280),
#        (u'NadineLustre', 219), (u'job', 217), (u'art', 154), (u'NowPlaying', 136),
#        (u'Hiring', 129), (u'sanremo2017', 126), (u'nowplaying', 120)]

# Create connection to hadoop and create map from tags to their respective directory in hadoop
tagToHadoopDir = {}
client = hdfs.InsecureClient("http://localhost:50070", user="hduser")
for tup in topten:
    path = "/user/hduser/" + tup[0]
    client.makedirs(path)
    tagToHadoopDir[tup[0]] = path
client.makedirs("/user/hduser/Others")
client.makedirs("/user/hduser/None")

# Create a directory for convince to store category files
if not os.path.exists("tweetfiles"):
    os.makedirs("tweetfiles")

# Create output files for each tweet category
tagToOutFile = {}
for tup in topten:
    strg = "tweetfiles/"+tup[0]+"-tweets.txt"
    tagToOutFile[tup[0]] = open(strg,"w")
tagToOutFile["OTHER"] = open("tweetfiles/Other-tweets.txt","w")
tagToOutFile["NONE"] = open("tweetfiles/None-tweets.txt", "w")

# Write each tweet to their respective tweet category
# options are one of the top ten, other, or none
for line in tweets:
    ht = ast.literal_eval(line)["entities"]["hashtags"]
    if ht == []:
        tagToOutFile["NONE"].write(line)
    for tag in ht:
        tagcat = tagCategory(topten,tag["text"])
        tagToOutFile[tagcat].write(line)

# Send to hadoop and close each file
for tag, file in tagToOutFile.iteritems():
    if tag == "OTHER":
        hdir = "/user/hduser/Others"
    elif tag == "NONE":
        hdir = "/user/hduser/None"
    else:
        hdir = tagToHadoopDir[tag]
    client.upload(hdir, file.name)
    file.close()

print (client.list("/user/hduser/"))
fin.close() # close tweets input file