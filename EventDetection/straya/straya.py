# sub_cat = ['', 'SIP', 'Port Scanner', 'SCADA', "Unix 'r' Service", 'BGP', 'RDesktop', 'XINETD', 'Telnet',
# 'SunRPC Portmapper (UDP)', 'Office Document', 'VNC', 'Clientside', 'Asterisk', 'RIP', 'ISAKMP', 'Apache', 'TFTP',
# 'OSPF', 'PPTP', 'Hypervisor', 'Dameware', 'Superflow', 'SunRPC', 'LPD', 'SunRPC Portmapper (TCP) TCP Service',
# 'SCO Unix', 'SOCKS', 'POP3', 'SunRPC Portmapper (UDP) TCP Service', 'Clientside Microsoft Paint', 'LDAP', 'PHP',
# 'HTTP', 'IXIA', 'Browser FTP', 'NetBIOS/SMB', 'SunRPC Portmapper (UDP) UDP Service', 'CUPS', 'IGMP', 'BSDi',
# 'Miscellaneous', 'TCP', 'HTML', 'Evasions', 'SunRPC Portmapper (TCP) UDP Service', 'NetBIOS', 'FTP', 'HP-UX',
# 'Clientside Microsoft Office', 'ICMP', 'IMAP', 'WINS', 'Decoders', 'SSL', 'SSH', 'IIS Web Server', 'SCTP', 'NNTP',
# 'OpenBSD', 'RTSP', 'IDS', 'SCCP', 'Microsoft IIS', 'Miscellaneous Batch', 'SMB', 'Backup Appliance', 'Mac OS X',
# 'BSD', 'Cisco Skinny', 'RDP', 'Windows Explorer', 'SMTP', 'All', 'RADIUS', 'DNS', 'Oracle', 'Browser', 'ALL', 'NTP',
# 'SNMP', 'AIX', 'Spam', 'Interbase', 'NetBSD', 'NULL', 'Syslog', 'IRIX', 'Web Application', 'Webserver',
# 'Unix r Service', 'Common Unix Print System (CUPS)', 'FreeBSD', 'Cisco IOS', 'Port Scanners', 'Microsoft Office',
# 'Windows', 'DCERPC', 'IRC', 'Clientside Microsoft Media Player', 'Solaris', 'Multiple OS', 'Linux', 'MSSQL']

directory = 'data/straya/'
filegt = 'NUSW-NB15_GT'
ext = '.csv'
filen = 'UNSW-NB15_'

gtdict = {}
with open(directory + filegt + ext, 'r') as gtf:
    gtf.seek(151)  # Skip the header line
    count = 0
    for line in gtf:
        count += 1
        linelist = line.split(',')
        if len(linelist) > 5:
            # key = (stime, endtime, attack cat, srcIP)
            # val = attack sub_cat
            gtdict[(linelist[0].strip(), linelist[1].strip(), linelist[2].strip(), linelist[5].strip())] = \
                linelist[3].strip()
        # else:
            # print line
            # print count
            # exit(0)
# print len(gtdict)
# print set(gtdict.values())
# exit(0)

wf1name = directory + 'attack.csv'
wf1 = open(wf1name, 'w')

wf2name = directory + 'normal.csv'
wf2 = open(wf2name, 'w')

for u in range(1, 5):
    with open(directory + filen + str(u) + ext, 'r') as f:
        for line in f:
            linelist = line.split(',')
            if linelist[-2]:
                key = (linelist[28].strip(), linelist[29].strip(), linelist[-2].strip(), linelist[0].strip())
                sub_cat = gtdict.get(key, 'misc')
                if sub_cat == '':
                    sub_cat = 'misc'
                newline = linelist[:-1] + [sub_cat] + linelist[-1:]
                wf1.write(','.join(newline))
                # wf1.write('\n')
                # exit(0)
            else:
                linelist[-2] = 'Normal'
                newline = linelist[:-1] + ['Normal'] + linelist[-1:]
                wf2.write(','.join(newline))
                # wf2.write('\n')
# print len(gtdict)
wf1.close()
wf2.close()
