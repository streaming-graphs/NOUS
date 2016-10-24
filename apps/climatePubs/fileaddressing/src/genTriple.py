__author__ = 'weituo'

"""
knowledge relation exmpales are as follows:

represents
is    component-whole
hasInstrument
hasInstruClass
hasSite
hasFacility
hasLocation
hasType
locatesAt
hasInstru
outputs
measures
hasCategory
hasSubCategory


For site information,
The instrument XXX is deployed at YYY;
The site YYY is short for ZZZ;

For outputs,
The instrument XXX outputs YYY;

For primary_meas,
The instrument XXX, measures YYY;
The measurement YYY has category ZZZ;
The measurement YYY has subcategory SSS

"""

import json
import sys
import re
from collections import OrderedDict


def transform(dic, ins):
    """
    input: ins is the name of the instrument and dic is its corresponding dictionary
    """
    print ins
    result = list()
    instru = "<" + ins + ">" + '\t'
    arm ="<ARM>\t"
    result.append(arm+ "<hasInstrument>\t" + "<"+ins+">")
    result.append(instru + '<is>' + "\t" + "<" + "instrument" + ">")
    result.append(instru + "<represents>" + "\t" + "<"+ dic['instrument_class_name'][0] + ">")
    result.append(instru + "<hasInstruClass>\t" + "<" +dic['instrument_class'][0] + ">")

    #sites_info
    hasSite = "<hasSite>" + "\t"
    represents = "<represents>" + "\t"
    hasType = "<hasType>" + "\t"
    mf = "<Mobile Facility>"
    locatesAt = "<locatesAt>\t"

    for site_info in dic['site_info']:
        site, site_name, site_type = site_info.split('||')
        result.append(arm + hasSite + "<" + site + ">")
        result.append(instru + hasSite + "<"+site+">")
        if site_type == "M" or site_type == "T":
            result.append(instru + "<is>\t" + mf)
            s = site_name.split(";")[0]
            site_name = " ".join(s.split(","))
        result.append("<"+site+">\t" + represents + "<"+site_name+">")


    for site in dic['sites']:
        slst = site.split('||')
        site_type = slst[-1]
        site_name = "<"+slst[0]+">"
        site_facilityid = "<"+slst[0] + "-" + slst[1] + ">"
        result.append(site_name + "\t" + "<hasFacility>\t" + site_facilityid)

        #locations = re.split('; |,', slst[2])
        locations = slst[2]
        if site_type == "M" or site_type == "T":
            locations = slst[2].split(";")[0]
        locs = locations.split(",")
        loc, prov = locs[0], ' '.join(locs[1:]).strip()
        loc = "<"+ loc + ">"
        result.append(arm + "<hasLocation>\t" + loc)
        result.append(site_facilityid + "\t" + "<hasLocation>\t" + loc)
        if prov != "":
            result.append(loc + '\t' + locatesAt + "<"+prov+">")
        result.append(site_facilityid + "\t" + "<hasInstru>\t" + "<"+ins+">")

        result.append(site_name + "\t" +  "<hasLocation>\t" + loc)
        result.append(site_name + "\t" + hasType + "<"+site_type +">")

    #outputs_info
    for output in dic['outputs']:
        result.append(instru + "<outputs>" + "\t"+ "<"+output+">")

    #measurements
    measures = "<measures>\t"
    hasCategory = "<hasCategory>\t"
    meas_temp2 = "<hasSubcategory>\t"
    for measlst in dic['primary_meas']:
        result.append(instru + measures + "<"+measlst[0]+">")
        cat = measlst[1]
        if type(cat) == str:
            result.append("<"+measlst[0]+">\t" + hasCategory + "<"+cat+">")
        elif type(cat) == list:
            for ca in cat:
                c, cc = ca.split('||')
                result.append("<"+measlst[0]+">\t" + hasCategory + "<"+c+">")
                result.append("<"+c+">\t" + represents + "<"+cc+">")

        subcat = measlst[2]
        if type(subcat) == str:
            result.append("<"+ measlst[0]+">\t" + hasCategory + "<"+subcat+">")
        elif type(subcat) == list:
            for sub in subcat:
                sublst = sub.split('||')
                s, ss = sublst[0], sublst[1]
                result.append("<"+measlst[0]+">\t" + hasCategory + "<"+s+">")
                result.append("<"+s+">\t" + represents + "<"+ss+">")

    return '\n'.join(result) + '\n'


def main():
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]
    with open(filename1) as json_data1, open(filename2) as json_data2,open('../text_info/triples.txt', 'w') as trip_writer:
    #with open(filename1) as json_data1,open('triplesNew.txt', 'w') as trip_writer:
        data = json.load(json_data1, object_pairs_hook=OrderedDict)
        trip_writer.write("<ARM>\t" + "<represents>\t" + "<atmospheric radiation measurement>\n")
        trip_writer.write("<AAF>\t" + "<represents>\t" + "<ARM Aerial Facility>\n")
        trip_writer.write("<AMF>\t" + "<represents>\t" + "<ARM Mobile Facility>\n")
        for ins in data:
            trip_writer.write(transform(data[ins], ins))

        #aaf data
        aafdata = json.load(json_data2, object_pairs_hook=OrderedDict)
        for ins in aafdata:
            if len(aafdata[ins]) != 0 and ins not in data:
                trip_writer.write(transform(aafdata[ins], ins))
main()
