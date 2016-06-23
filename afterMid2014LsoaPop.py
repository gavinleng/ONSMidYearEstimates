__author__ = 'G'

import sys
sys.path.append('../harvesterlib')

import pandas as pd
import argparse
import json

import now


def download(inPath, outPath, col, keyCol, digitCheckCol, noDigitRemoveFields):
    dName = outPath

    genderArray = ["female", "male"]

    listinPath = inPath[0].split('/')
    iYear = listinPath[len(listinPath) - 1].split('-')[1]

    iPopID = "ONS-mid-" + iYear + "-lsoa-syoa-estimates"
    #iPopType = "Base"
    iPopdescription = "ONS estimate (http://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates)"

    # operate this file
    raw_data = {}
    for j in col:
        raw_data[j] = []

    for i in range(2):
        inFile = inPath[i]

        # load files
        logfile.write(str(now.now()) + ' ' + inFile + ' file loading\n')
        print(inFile + ' file loading------')

        df = pd.read_csv(inFile, dtype='unicode')
        csvcol = df.columns.tolist()
        agcol = csvcol[2:]
        lenagcol = len(agcol)

        lenrow = df.shape[0]
        #lenrow = 10
        for j in range(lenrow):
            if (j % 1000) == 0:
                print(i, j)

            raw_data[col[2]] = raw_data[col[2]] + agcol
            raw_data[col[5]] = raw_data[col[5]] + [df.iloc[j][0]] * lenagcol
            raw_data[col[6]] = raw_data[col[6]] + [df.iloc[j][1]] * lenagcol
            raw_data[col[7]] = raw_data[col[7]] + df.iloc[j][2:].tolist()

        raw_data[col[3]] = raw_data[col[3]] + [genderArray[i]] * lenagcol * lenrow

    raw_data[col[0]] = raw_data[col[0]] + [iPopID] * lenagcol * lenrow * 2
    #raw_data[col[1]] = raw_data[col[1]] + [iPopType] * lenagcol * lenrow * 2
    raw_data[col[1]] = raw_data[col[1]] + [iPopdescription] * lenagcol * lenrow * 2
    raw_data[col[4]] = raw_data[col[4]] + [iYear] * lenagcol * lenrow * 2

    raw_data[col[7]] = [int(i.replace(",", "")) for i in raw_data[col[7]]]

    df1 = pd.DataFrame(raw_data)
    strings = df1.to_json(orient="records")

    jsonString = '[{"jsondata":' + strings + '}]'

    myJson = pd.read_json(jsonString)
    myJson.index = ['mydata']

    # save to file
    myJson.to_json(path_or_buf=dName, orient="index")
    logfile.write(str(now.now()) + ' has been extracted and saved as ' + str(dName) + '\n')
    print('Requested data has been extracted and saved as ' + dName)
    logfile.write(str(now.now()) + ' finished\n')
    print("finished")


parser = argparse.ArgumentParser(
    description='Get the age group date from after mid-2014 LSOA population estimates as .json files.')
parser.add_argument("--generateConfig", "-g", help="generate a config file called config_tempafterMid2012LsoaMF.json",
                    action="store_true")
parser.add_argument("--configFile", "-c", help="path for config file")
args = parser.parse_args()

if args.generateConfig:
    obj = {
        "inPath": ["./data/mid-2014-lsoa-female-g.csv", "./data/mid-2014-lsoa-male-g.csv"],
        "outPath": "mid-2014-lsoa-mf.json",
        "colFields": ['popId', 'popId_description', "age_band", "gender", "year", "area_id", "area_name", "persons"],
        "primaryKeyCol": [],
        "digitCheckCol": [],
        "noDigitRemoveFields": []
    }

    logfile = open("log_tempafterMid2014LsoaMF.log", "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open("err_tempafterMid2014LsoaMF.err", "w")

    with open("config_tempafterMid2014LsoaMF.json", "w") as outfile:
        json.dump(obj, outfile, indent=4)
        logfile.write(str(now.now()) + ' config file generated and end\n')
        sys.exit("config file generated")

if args.configFile == None:
    args.configFile = "config_tempafterMid2014LsoaMF.json"

with open(args.configFile) as json_file:
    oConfig = json.load(json_file)

    logfile = open('log_' + oConfig["outPath"].split('.')[0] + '.log', "w")
    logfile.write(str(now.now()) + ' start\n')

    errfile = open('err_' + oConfig["outPath"].split('.')[0] + '.err', "w")

    logfile.write(str(now.now()) + ' read config file\n')
    print("read config file")

download(oConfig["inPath"], oConfig["outPath"], oConfig["colFields"], oConfig["primaryKeyCol"], oConfig["digitCheckCol"], oConfig["noDigitRemoveFields"])
