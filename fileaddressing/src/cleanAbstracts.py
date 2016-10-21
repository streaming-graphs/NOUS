

with open("ARMFY2013-FY2016June.txt", "r") as reader, open("titleAbs.txt", "w") as writer:
#with open("temp.txt", "r") as reader, open("titleAbs.txt", "w") as writer:
    l = filter(lambda x : len(x) > 10 and not x.startswith("http"), reader.readlines())
    for i in xrange(len(l)):
        if not l[i][0].isalpha():
            continue

        #print l[i], l[i].split()[-1]
        writer.write(l[i])
        if l[i].split()[-1].startswith("http"):
            if i < len(l) - 1 and l[i + 1].split()[-1].startswith("http"):
                writer.write("This is a hardcoded string to represent the absent abstract\n")



with open("titleAbs.txt", "r") as reader, open("pureAbs.txt", "w") as writer:
    for idx, line in enumerate(reader.readlines()):
        if idx % 2:
            writer.write(line)




