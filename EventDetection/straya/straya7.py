# Count the number of times different IPs appear in attack/normal files


directory = 'data/straya/'
att = 'normal'
fn = ''
ext = '.csv'

ip = {}
with open(directory + att + fn + ext, 'r') as f:
    for line in f:
        line_list = line.split(',')
        srcip, destip = line_list[0], line_list[2]
        temp = ip.get(srcip, 0)
        temp += 1
        ip[srcip] = temp

        temp = ip.get(destip, 0)
        temp += 1
        ip[destip] = temp

ip = sorted(ip.items(), key=lambda x: x[1], reverse=True)

wf = open(directory + att + fn + '_hist.txt', 'w')
for k, v in ip:
    print k, v
    wf.write(','.join(map(str, [k, v])))
    wf.write('\n')
