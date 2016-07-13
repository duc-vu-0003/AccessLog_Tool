import random
import os
import pickle
import pandas as pd
import numpy as np
import os
import logging
from utils import Paths
import time
import collections
import encodings
import re
from os import path
from bs4 import BeautifulSoup
import requests
from goose import Goose

def readWebContent():
    if os.path.exists(Paths.web_content_file):
        statinfo = os.stat(Paths.web_content_file)
        if statinfo.st_size > 0 :
            webContentFile = np.load(Paths.web_content_file)
            # print(webContentFile['array1'])

            for title in webContentFile['array3']:
                print(title)

            # print(webContentFile['array2'])
            # print(webContentFile['array3'])

def preprocessing():
    print 'PreProcess'
    start_time = time.time()

    if os.path.exists(Paths.cache_file):
        statinfo = os.stat(Paths.cache_file)
        if statinfo.st_size == 0 :
            readRawFolder()
    else:
        readRawFolder()

    cacheFile = np.load(Paths.cache_file)

    # print(cacheFile['array1'])
    # print(cacheFile['array2'])
    # print(cacheFile['array3'])
    # print(cacheFile['array4'])
    # print(cacheFile['array5'])
    # print(cacheFile['array6'])
    # print(cacheFile['array7'])
    # print(cacheFile['array8'])

    webLinkList = cacheFile['array4']
    refLinkList = cacheFile['array7']

    crawlerLinkList = []

    for num in range(0, 500):
        linkCrawler = ""

        if len(refLinkList[num]) > 10:
            linkCrawler = refLinkList[num]
        else:
            linkCrawler = webLinkList[num]

        crawlerLinkList.append(linkCrawler)

    crawlerLinkList = remove_duplicate(crawlerLinkList)

    print("Starting crawler ...." + str(len(crawlerLinkList)))
    #
    webTitleList = []
    webDescriptionList = []

    for link in crawlerLinkList:
        title, description = crawlerWebLinkManual(link)
        webTitleList.append(title)
        webDescriptionList.append(description)

    #write data to file here
    print("Saving data ....")
    np.savez_compressed(Paths.web_content_file, array1=crawlerLinkList, array2=webTitleList, array3=webDescriptionList)

    end_time = time.time()
    print '(Time to preprocess: %s)' % (end_time - start_time)


def readRawFolder():
    subDirs = Paths.data

    if not os.path.exists(subDirs):
        os.makedirs(subDirs)
        os.makedirs(temp_data)
        print('Input data not found, please put access log data to folder data :)')
        return

    dirs = os.listdir(subDirs)

    ipListRaw = []
    timeListRaw = []
    methodListRaw = []
    requestLinkListRaw = []
    statusCodeListRaw = []
    requestLineListRaw = []
    refererLinkListRaw = []
    userAgentListRaw = []

    # This would print all the files and directories
    for file in dirs:

        ipList = []
        timeList = []
        methodList = []
        requestLinkList = []
        statusCodeList = []
        requestLineList = []
        refererLinkList = []
        userAgentList = []

        if not file.startswith("."):
            print(file)
            if os.path.isdir(subDirs + "/" + file):
                pathName = os.listdir(subDirs + "/" + file)
                for fileSub in subDirs:
                    ipList, timeList, methodList, requestLinkList, statusCodeList, requestLineList, refererLinkList, userAgentList = readRawFile(fileSub)
            else:
                ipList, timeList, methodList, requestLinkList, statusCodeList, requestLineList, refererLinkList, userAgentList = readRawFile(file)

        ipListRaw += ipList
        timeListRaw += timeList
        methodListRaw += methodList
        requestLinkListRaw += requestLinkList
        statusCodeListRaw += statusCodeList
        requestLineListRaw += requestLineList
        refererLinkListRaw += refererLinkList
        userAgentListRaw += userAgentList

    print("Writing cache....")
    # Save cache
    # print("Writing ipListRaw....")
    # np.savez_compressed(Paths.ipList_file, array1=ipListRaw)
    # print("Writing timeListRaw....")
    # np.savez_compressed(Paths.timeList_file, array1=timeListRaw)
    # print("Writing methodListRaw....")
    # np.savez_compressed(Paths.methodList_file, array1=methodListRaw)
    # print("Writing requestLinkListRaw....")
    # np.savez_compressed(Paths.requestLinkList_file, array1=requestLinkListRaw)
    # print("Writing statusCodeListRaw....")
    # np.savez_compressed(Paths.statusCodeList_file, array1=statusCodeListRaw)
    # print("Writing requestLineListRaw....")
    # np.savez_compressed(Paths.requestLineList_file, array1=requestLineListRaw)
    # print("Writing userAgentListRaw....")
    # np.savez_compressed(Paths.userAgentList_file, array1=userAgentListRaw)
    # print("Writing refererLinkListRaw....")
    # np.savez_compressed(Paths.refererLinkList_file, array1=refererLinkListRaw)
    np.savez_compressed(Paths.cache_file, array1=ipListRaw, array2=timeListRaw, array3=methodListRaw, array4=requestLinkListRaw, array5=statusCodeListRaw, array6=requestLineListRaw, array7=refererLinkListRaw, array8=userAgentListRaw)

def readRawFile(fileName):
    if not os.path.exists(Paths.data + "/" + fileName):
        print('File Not Found!!!')
        return

    ipList = []
    timeList = []
    methodList = []
    requestLinkList = []
    statusCodeList = []
    requestLineList = []
    refererLinkList = []
    userAgentList = []

    with open(Paths.data + "/" + fileName, 'r') as logFile:
        lines = logFile.readlines()
        for line in lines:
            line = line.strip()
            line = line.replace('"', '')
            if len(line) > 0:
                attributes = line.split(" ")

                if len(attributes) < 12:
                    #Specify case
                    ipList.append("")
                    timeList.append("")
                    methodList.append("")
                    requestLinkList.append("")
                    statusCodeList.append("")
                    requestLineList.append("")
                    refererLinkList.append("")
                    userAgentList.append("")
                else:
                    ipList.append(attributes[0])
                    timeList.append(attributes[3] + attributes[4])
                    methodList.append(attributes[5])
                    requestLinkList.append(attributes[6])
                    statusCodeList.append(attributes[8])
                    requestLineList.append(attributes[7])
                    refererLinkList.append(attributes[11])
                    userAgent = " ".join(attributes[12:])
                    userAgentList.append(userAgent)


    return ipList, timeList, methodList, requestLinkList, statusCodeList, requestLineList, refererLinkList, userAgentList

def crawlerWebLinkManual(url):

    print("Crawler: " + url)
    #Title and description
    title = ""
    description = ""

    try:
        page3 = requests.get(url)
        page3.encoding = 'UTF-8'

        soup3 = BeautifulSoup(page3.text, "lxml")
        desc = soup3.find(attrs={'name':'Description'})
        if desc == None:
            desc = soup3.find(attrs={'name':'description'})

        title = soup3.title.string
        description = desc['content']
    except Exception as e:
        # print '%s (%s)' % (e.message, type(e))
        title = ""
        description = ""

    # kill all script and style elements
    # for script in soup3(["script", "style"]):
    #     script.extract()    # rip it out
    #
    # # get text
    # text = soup3.get_text()
    #
    # # break into lines and remove leading and trailing space on each
    # lines = (line.strip() for line in text.splitlines())
    # # break multi-headlines into a line each
    # chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # # drop blank lines
    # text = '\n'.join(chunk for chunk in chunks if chunk)
    # # print(text)
    return title, description

def crawlerWebLink(url):
    g = Goose()
    article = g.extract(url=url)

    print(article.title)
    print(article.meta_description)
    print(article.cleaned_text)

def remove_duplicate(alist):
    return list(set(alist))

def main():
    oper = -1
    while int(oper) != 0:
        print('**************************************')
        print('Choose one of the following: ')
        print('1 - Pre Processing Raw Data')
        print('2 - Crawler Web link')
        print('3 - Read Saved Web Content')
        print('0 - Exit')
        print('**************************************')
        oper = int(input("Enter your options: "))

        if oper == 0:
            exit()
        elif oper == 1:
            preprocessing()
        elif oper == 2:
            crawlerWebLinkManual('http://www.ebay.vn/tim-kiem/Marc+jacobs+lip.html?type=2utm_source=Email&utm_medium=Danhmuc&utm_content=EBAY_2016&utm_campaign=EM_0206_SKLD')
        elif oper == 3:
            readWebContent()

if __name__ == "__main__":
    main()
