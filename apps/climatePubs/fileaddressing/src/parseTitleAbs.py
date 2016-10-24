__author__ = 'weituo'

import csv
import re
import sys
from collections import OrderedDict


def parse(sentence):
    """
    input: sentence is the paper title
    output: year when the paper is publised
            title
            url is the url by which the paper can be downloaded
            authors are the authors
    """
    titleinfo  = re.split(r"\s{3,}", sentence)
    if len(titleinfo) < 2:
        url = 'N/A'
    else:
        url = titleinfo[1]
    t = titleinfo[0]
    lst = re.split(r"\((\d{4})\).", t)
    authors, year, titles = lst[0], lst[1], lst[2]
    #print titles
    return year, titles.strip(), url, authors.strip()


def processText(file_path):
    """
    input: file_path is the file path where the output of disambiguation
    output: parseAbs.csv is the collection of the disambiguation result
    """
    data = OrderedDict()
    total = 0
    non_extracted = 0
    with open(file_path, 'r') as reader:
        lines = reader.readlines()
        total = len(lines) / 2
        for i, line in enumerate(lines):
            if i % 2 == 0:
                print "address %dth line" % (i)
                content = lines[i + 1].strip().split(":")
                if len(content[1]) == 0:
                    non_extracted += 1
                else:
                    entities = ';'.join(list(set(content[1].split('\t'))))
                    year, title, url, authors = parse(line.strip())
                    yearpapers = data.get(year, [])
                    yearpapers.append([title, entities, url, authors])
                    data[year] = yearpapers
    print "there are %d papers out of %d from which nothing are extracted" %(non_extracted, total)

    with open("parsedAbs.csv", "wb") as writefile:
        writer = csv.writer(writefile)
        for year in sorted(data.keys()):
            for paper in data[year]:
                writer.writerow([year] + paper)
        #writer.write("there are %d papers out of %d from which nothing are extracted" %(non_extracted, total))

def main():
    processText(sys.argv[1])

if __name__ == "__main__":
    main()


