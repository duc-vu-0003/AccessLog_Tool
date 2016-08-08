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
from urlparse import urlparse

def readWebContent(needWriteCSV):
    if os.path.exists(Paths.web_content_file):
        statinfo = os.stat(Paths.web_content_file)
        if statinfo.st_size > 0 :
            webContentFile = np.load(Paths.web_content_file)
            # print(webContentFile['array3'])

            # for title in webContentFile['array3']:
            #     print(title)

            print(webContentFile['array3'])
            # print(webContentFile['array3'])

            if needWriteCSV:
                df = pd.DataFrame(columns = ['web_link', 'title', 'description', 'category'])
                df['web_link'] = webContentFile['array1']
                df['title'] = webContentFile['array2']
                df['description'] = webContentFile['array3']

                print(df)

                df.to_csv(Paths.temp_result, sep=',', encoding='UTF-8')

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

    for num in range(0, len(refLinkList)):
        linkCrawler = ""

        if len(refLinkList[num]) > 10:
            linkCrawler = refLinkList[num]
        else:
            linkCrawler = webLinkList[num]

        crawlerLinkList.append(linkCrawler)

    crawlerLinkList = remove_duplicate(crawlerLinkList)

    webLinkList = None
    refLinkList = None

    print("Starting crawler .... " + str(len(crawlerLinkList)))
    #
    # webTitleList = []
    # webDescriptionList = []

    # Read result file to get postion to continue from last run
    countRow = 0
    if not os.path.isfile(Paths.temp_result):
        countRow = 0
    else:
        dfResult = pd.read_csv(Paths.temp_result)
        countRow = len(dfResult.index)

    i = countRow
    for link in crawlerLinkList[countRow : len(crawlerLinkList)]:
        if not link.startswith("http://huaban.com/") and not link.startswith("http://member.1688.com/") and not link.startswith("http://api.droid4x.cn/") :

            df = pd.DataFrame(columns = ['web_link', 'title', 'description', 'category'])

            if link.endswith(".jpg") or link.endswith(".css") or link.endswith(".png") or link.endswith(".gif") or link.endswith(".js") or link.endswith(".jpeg") or link.endswith(".zip") or link.endswith(".mp4"):
                item = pd.Series([link, "", "", ''], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://su.ff.avast.com/") or "kaspersky" in link or "symantecliveupdate" in link or "avast" in link:
                item = pd.Series([link, "Antivirus Update", "", 'Antivirus Update'], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://api.") or link.startswith("https://api."):
                item = pd.Series([link, "API", "", 'API'], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://w.api.mega.co.nz/"):
                item = pd.Series([link, "Mega API", "", 'API'], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://nethd.org"):
                item = pd.Series([link, "Torrent tracker", "", 'torrent'], index=['web_link', 'title', 'description', 'category'])
            elif "windowsupdate" in link:
                item = pd.Series([link, "Windos Update", "", 'windowsupdate'], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://secure.livechatinc.com"):
                item = pd.Series([link, "LiveChat - Customers choose live chat over phone or email", "LiveChat - premium live chat software and help desk software for business. Over 15,000 companies from 140 countries use LiveChat. Try for free!", 'Business'], index=['web_link', 'title', 'description', 'category'])
            elif link.startswith("http://goweatherex.3g.cn") or link.startswith("http://rts.mobula.sdk.duapps.com/"):
                item = pd.Series([link, "", "", ''], index=['web_link', 'title', 'description', 'category'])
            else:
                category = ""

                if "chodientu" in link or "alokhuyenmai" in link or "vatgia" in link or "zoomua" in link or "ebay" in link or "nguyenkim" in link or "trananh" in link:
                    category = "shopping"
                elif "kenh14" in link or "vnexpress" in link or "dantri" in link or "vietbao" in link or "danviet" in link or "24h.com.vn" in link or "yan.vn" in link:
                    category = "news"
                elif "bongda.com.vn" in link:
                    category = "news, sports, football"
                elif "stackoverflow" in link:
                    category = "IT Techniques"
                elif link.startswith("http://mp3.zing.vn/") or link.startswith("http://www.nhaccuatui.com/"):
                    category = "music, entertainment"

                title, description = crawlerWebLinkManual(link)
                if title == "" or description == "":
                    title, description = crawlerWebLinkManual(getDomainFromUrl(link))
                item = pd.Series([link, title, description, category], index=['web_link', 'title', 'description', 'category'])

            # Save data for file
            df = df.append(item, ignore_index=True)
            appendDFToCSV(df, Paths.temp_result)
            # webTitleList.append(title)
            # webDescriptionList.append(description)
            print(i)
            i+=1
            df = None

    #write data to file here
    # print("Saving data ....")
    # np.savez_compressed(Paths.web_content_file, array1=crawlerLinkList, array2=webTitleList, array3=webDescriptionList)

    end_time = time.time()
    print '(Time to preprocess: %s)' % (end_time - start_time)

def appendDFToCSV(df, csvFilePath, sep=","):
    import os
    if not os.path.isfile(csvFilePath):
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep, encoding='UTF-8')
    elif len(df.columns) != len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns):
        raise Exception("Columns do not match!! Dataframe has " + str(len(df.columns)) + " columns. CSV file has " + str(len(pd.read_csv(csvFilePath, nrows=1, sep=sep).columns)) + " columns.")
    elif not (df.columns == pd.read_csv(csvFilePath, nrows=1, sep=sep).columns).all():
        raise Exception("Columns and column order of dataframe and csv file do not match!!")
    else:
        df.to_csv(csvFilePath, mode='a', index=False, sep=sep, header=False, encoding='UTF-8')

def readRawFolder():
    subDirs = Paths.data

    if not os.path.exists(subDirs):
        os.makedirs(subDirs)
        os.makedirs(temp_data)
        os.makedirs(result)
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

    #I remove some link cannot get
    alist.remove('http://w.api.mega.co.nz/YsNZcTlk1n1JLsn33fHkXJQ_RlA')
    alist.remove('http://y7549610.ivps9x.u.avast.com/ivps9x/vps_win64-fdc-fdb.vpx')
    alist.remove('http://j1.baomoi.com/i/160531/9d/646459.jpg?preset=default&mode=crop&anchor=topcenter&scale=both&width=250&height=250')


    return list(set(alist))

def getDomainFromUrl(url):
    parsed_uri = urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain

def main():
    oper = -1
    while int(oper) != 0:
        print('**************************************')
        print('Choose one of the following: ')
        print('1 - Pre Processing Raw Data')
        print('2 - Crawler Web link')
        print('3 - Read Saved Web Content')
        print('4 - Get Domain from URL')
        print('5 - Write to CSV')
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
            readWebContent(False)
        elif oper == 4:
            getDomainFromUrl("http://p1.msg.zaloapp.com/?zpw_type=20&zpw_ver=35&zpw_sek=t0vS.138360650.a1.D6qXZdVRAeqzx1M5LjigZGpvJ-PLuGYx0-9xp32qOhy7z078FPbJoWAMRECsvXUl2gugyfFRAertsYsSJ8nal0&params=148217145418")
        elif oper == 5:
            readWebContent(True)

if __name__ == "__main__":
    main()
