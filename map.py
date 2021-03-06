#!/usr/bin/python

import sys, ast

keyword = "Amazon"
attribute = "hashtags"

for line in sys.stdin:
    if line == "":
        continue

    if attribute == "text":
        try:
            newline = ast.literal_eval(line)["text"]
            newline = newline.strip()
            words = newline.split()
            for word in words:
                if word == keyword:
                    print '%s\t%s' % (word, 1)
        except:
            continue


    elif attribute == "hashtags":
        try:
            hts = ast.literal_eval(line)["entities"]["hashtags"]
            if not hts == []:
                for ht in hts:
                    if ht["text"] == keyword:
                        print '%s\t%s' % (ht["text"], 1)
        except:
            continue




