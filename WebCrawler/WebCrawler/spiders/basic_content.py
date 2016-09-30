from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import scrapy
import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')

LIMIT = 10

print("Connecting to MySql...")
db = MySQLdb.connect("localhost", "root", "ngango", "FUWebLog", use_unicode=True, charset="utf8")
print("Connected to MySql - DB: FUWebLog")
# prepare a cursor object using cursor() method
cursor = db.cursor()

def getSavedPosition():
    print("FUWebLog: Check table WebProxyContent...")
    if isHaveTable():
        print("FUWebLog: Already have table WebProxyContent...")
    else:
        # Drop table if it already exist using execute() method.
        cursor.execute("DROP TABLE IF EXISTS WebProxyContent")
        # Create table as per requirement
        sql = """CREATE TABLE FUWebLog.WebProxyContent (
                    ID INT,
                    SavePosition INT)"""
        cursor.execute(sql)

        sql1 = "INSERT INTO FUWebLog.WebProxyContent(ID, SavePosition) VALUES (0, 0)"
        cursor.execute(sql1)
        print("FUWebLog: Create table WebProxyContent succesfully!!!")

    return getItemsCount()

def getItemsCount():
    sql = "SELECT * FROM FUWebLog.WebProxyContent;"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchone()
        return results[1]
    except:
        return 0

def updateSavePosition(position):
    sql = """UPDATE FUWebLog.WebProxyContent \
            SET \
            SavePosition = %d WHERE ID = 0;""" % position

    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Commit your changes in the database
        db.commit()
        print('Saved position %d' % position)
    except Exception as error:
        # Rollback in case there is any error
        print(error)
        db.rollback()

def loadUrls(limit, offset):
    print('Start get %d urls from offset %d' % (limit, offset))
    sql = "SELECT uri FROM FUWebLog.WebProxyLog \
            WHERE FUWebLog.WebProxyLog.mimetype \
            LIKE '%s' LIMIT %d OFFSET %d;" % ('%%text/html%', limit, offset)

    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        return results
    except:
        return []

def updateData(title, description, keywords, uri):
    print('Start update row(s) where uri: %s' % uri)
    sql = """UPDATE FUWebLog.WebProxyLog \
            SET \
            WebTitle = %s, \
            WebDescription = %s, \
            WebKeywords = %s \
            WHERE uri = %s;"""

    print title
    # print description
    # print keywords
    # print uri

    try:
        # Execute the SQL command
        cursor.execute(sql, (title, description, keywords, uri))
        # Commit your changes in the database
        db.commit()
        print('Updated row(s) where uri: %s' % uri)
    except Exception as error:
        # Rollback in case there is any error
        print(error)
        db.rollback()

def isHaveTable():
    result = False
    sql = "SELECT * \
            FROM information_schema.tables \
            WHERE table_schema = 'FUWebLog' \
            AND table_name = 'WebProxyContent' \
            LIMIT 1;"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        result = len(results) > 0
    except:
        result = False
    return result

class BasicContentSpider(Spider):
    name = "basic_content"

    offset = getSavedPosition()
    results = loadUrls(LIMIT, offset)
    start_urls = [tup[0] for tup in results]

    def __init__(self):
        dispatcher.connect(self.quit, signals.spider_closed)

    def quit(self, spider):
      # second param is instance of spder about to be closed.
      updateSavePosition(getItemsCount() + LIMIT)
      print("Disconnect from MySql - DB: FUWebLog")
      db.close()

    def parse(self, response):
        print(response.url)

        title = ""
        description = ""
        keywords = ""

        try:
            title = response.css('title::text').extract_first().encode('utf-8')
        except:
            title = ""

        try:
            description = response.xpath("//meta[@name='description']/@content").extract_first().encode('utf-8')
        except:
            description = ""

        try:
            keywords = response.xpath("//meta[@name='keywords']/@content").extract_first().encode('utf-8')
        except:
            try:
                keywords = response.xpath("//meta[@name='news_keywords']/@content").extract_first().encode('utf-8')
            except:
                keywords = ""

        if len(title) == 0 and len(description) == 0 and len(keywords) == 0:
            print("Cannot crawler data, don't need update data")
        else:
            updateData(title, description, keywords, response.url)
