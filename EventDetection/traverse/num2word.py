import sys


def get_dict(fname):
    data_dict = {}
    with open(fname, 'rb') as f:
        for line in f:
            words = line.split()
            data_dict[words[1]] = words[0]
    return data_dict


def write_walk(diction, fname):
    fw = open(fname + 'text', 'w')
    with open(fname, 'rU') as f:
        for line in f:
            words = line.strip()[1:-1]
            words = words.split(', ')
            new_words = [diction[word] for word in words]
            fw.write(str(new_words))
            fw.write('\n')
    fw.close()


if __name__ == '__main__':
    dictfname = sys.argv[1]
    walkfname = sys.argv[2]
    dictionary = get_dict(dictfname)
    write_walk(dictionary, walkfname)
