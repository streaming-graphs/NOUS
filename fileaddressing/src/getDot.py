INSTRUMENTS = ["ACSM", "AERI","AETH","AMC","AOS","AOSMET","BRS", "CAPS-PMEX", "CCN","CEIL","CLAP","CO",
             "CO2FLX","CPC","CSAPR","CSPHOT","DISDROMETER","DL","EBBR","ECOR","FLASK","GHG","GNDRAD",
             "GVR","GVRP","HSRL","HTDMA","IAP","IRSI","IRT","ISSSONDE","KASACR","KAZR","LDIS","MASC",
             "MAWS","MET","MFR","MFRIRT","MFRSR","MMCR","MPL","MWACR","MWR","MWR3C","MWRHF","MWRP","NEPHELOMETER",
             "NFOV","NIMFR","ORG","OZONE","PASS","PGS","PSAP","RAIN","RL","RSS","RWP","SASHE","SASZE",
             "SEBS","SIRS","SKYRAD","SMPS","SODAR","SONDE","SONICWIND","SP2","STAMP","SURTHREF","SWACR",
             "SWATS","SWS","TDMA","THWAPS","TRACEGAS","TSI","TWR","TWRCAM","UHSAS","VDIS","WACR","WSACR",
              "XSACR","XSAPR"]





with open("triples.txt", 'r') as reader, open("triples.dot", 'w') as writer:
    writer.write("digraph arm {\n")
    for ins in INSTRUMENTS:
        writer.write(ins + " " + "[color=red]\n")
    for line in reader.readlines():
        lst = line.split('\t')
        s, o = lst[0], lst[-1]
        if '-' in s or '-' in o:
            continue
        node1, node2 = s[1:-1], o[1:-2]
        writer.write("\"" + node1 + "\"" + "->" + "\"" + node2 + "\"" + "\n")
    writer.write("}")


