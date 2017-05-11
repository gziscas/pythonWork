# coding:utf-8 
#import httplib2  
import urllib
import re  
import sys
import io
import os
import threading
import sqlite3
import MySQLdb
import uuid
import time
from lxml import etree  

# 2017.5.11 updated, using fixed UUID

#mysql
class mysqlVar: 
  db = None
  cursor = None


#sqllite
class sqlliteVar: 
   dbeExist = None
   cx = None
   cur = None

Video_Data = []

reload(sys)
sys.setdefaultencoding('utf-8')
g_codetype = sys.getfilesystemencoding()


def openMysql():
    # 打开数据库连接
    #db = MySQLdb.connect("127.0.0.1","root","12345","jojodb", charset='utf8')
    db = MySQLdb.connect("119.23.68.93","mess","jojo$2904^sns","mess", charset='utf8')

    # 使用cursor()方法获取操作游标 
    cursor = db.cursor()  

    # 使用execute方法执行SQL语句
    cursor.execute("SELECT VERSION()")

    mysqlVar.db = db
    mysqlVar.cursor = cursor

def closeMysql():
    mysqlVar.db.close()   

def getHtml(url):
    page = urllib.urlopen(url)
    html = page.read()
    return html

def getNews(html):
    #reg = r'<a href="(.*?)">(.*?)</a>(.*?)</span></div>'
    reg = r'[\d\D]*?data-src="(.*?)"/>[\d\D]*?<a href="(.*?)">(.*?)</a> <span>(.*?)</span></div>'
 
    newsre = re.compile(reg)
    newslist = re.findall(newsre, html)
    #抓取3个分组：主题配图、链接网址、title
    x=0
    for newurl in newslist:
      
       num = len(newurl)
       if num > 1:
          pic = newurl[0]
          url = newurl[1]
          name = newurl[2]
          #print x,  url, name

          getVideo(pic, url, name)
          x = x+1

def getVideo(picurl, new_url, url_name):
    newhtml = getHtml(new_url)

    if newhtml is None:
        return
        
    #抓取视频文件
    videoreg = r'videoUrl=(.*?)"'
    videosre = re.compile(videoreg)
    videourl = re.findall(videosre, newhtml)

    num = len(videourl)

    if num > 0:
       url = videourl[0]

       fname = os.path.basename(url)
       tmp = checkUrl(fname)
       if tmp == 0:
          Video_Data.append([picurl, url, url_name])        
          insertVideoInfo(picurl, url_name, fname)

          insertArticleInfoMysql(picurl, url_name, url, fname)


 
def getImg(html):
    reg = r'src="(.+?\.jpg)" pic_ext'
    imgre = re.compile(reg)
    imglist = re.findall(imgre,html)
    x = 0
    for imgurl in imglist:
        urllib.urlretrieve(imgurl,'%s.jpg' % x)
        x+=1

def begin_Thread():
    try:
        print("begin get video url...")
        html = getHtml("http://news.v1.cn/today_hot.shtml")
        getNews(html)  
    except Exception as e:
        raise


    thread = threading.Thread(target= myThread)
    thread.start()

def myThread():
    id = 0

    print("begin downloading...")

    num = len(Video_Data)
    re = 0
    for Data in Video_Data:

        #下载主题配图
        picname = os.path.basename(Data[0])
        re = urllib.urlretrieve(Data[0], picname)
        print id+1, picname
 
        #下载视频文件
        fname = os.path.basename(Data[1])
        re = urllib.urlretrieve(Data[1], fname)
        print id+1, fname
 
        id += 1

    if id ==0:
        print 'no videos updated...' 
       
    print id, ' videos downloaded...' 

def testpara(txt):
    print txt

def OpenSqlliteDb():
    dbeExist = os.path.exists("video_info.db")

    cx = sqlite3.connect("video_info.db")  
    cx.text_factory = str  
    cur = cx.cursor() 

    if dbeExist == False:
        cx.execute("create table videoInfo (id integer, title varchar(255), videourl varchar(255))")   

    sqlliteVar.cur = cur
    sqlliteVar.cx = cx
    sqlliteVar.dbeExist = dbeExist       


def insertVideoInfo(picurl, title, url):
    insert_sql = "insert into videoInfo (id, title, videourl) values (?,?,?)"
    sqlliteVar.cur.execute(insert_sql, (0, title, url))
    sqlliteVar.cx.commit()

def QueryVideoInfo():
    #查询记录
    select_sql = "select * from videoInfo"
    sqlliteVar.cur.execute(select_sql)

    #返回一个list，list中的对象类型为tuple（元组）
    rows = sqlliteVar.cur.fetchall()  
  
    for row in rows:  
        print row[0], row[1], row[2]

def checkUrl(fname):
    
    select_sql = "select * from videoInfo"
    sqlliteVar.cur.execute(select_sql)

    #返回一个list，list中的对象类型为tuple（元组）
    rows = sqlliteVar.cur.fetchall()  
  
    found = 0
    for row in rows:  
        if fname == row[2]:
           found = 1;

    return found

def insertArticleInfoMysql(picurl, title, url, fname):
    #sql = "insert into article_test(type, title, url, filename) values (%d, '%s', '%s','%s')" % (type, title, url, fname)
    article_id  =  str(uuid.uuid4()).replace('-','')
    article_title = title
    article_author = 'first video'
    release_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    article_summary = title
    article_column_id = '4d8a6539a888475e8a78e0c9a62d4d4d'
    is_released = 1
    release_user = 'Python Spider'
    article_type = 4
    create_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    article_url = fname
    weburl = url
    is_top = 0
    is_digest = 0
    page_title = title
    article_keyword = title
    firstimg = os.path.basename(picurl)
    taskconfigid = ''


    sql = "insert into article_info(article_id, article_title, article_author, release_time, article_summary, article_column_id, is_released, release_user, article_type, create_time, article_url, weburl, is_top, is_digest, page_title, article_keyword, firstimg, taskconfigid) values ('%s', '%s','%s', '%s', '%s','%s', %d, '%s', %d, '%s', '%s','%s', %d, '%d', '%s','%s', '%s','%s')" % (article_id, article_title,  article_author, release_time, article_summary, article_column_id, is_released, release_user, article_type, create_time, article_url, weburl, is_top, is_digest, page_title, article_keyword, firstimg, taskconfigid)
    #print sql
    mysqlVar.cursor.execute(sql)
    mysqlVar.db.commit()

def main():

    openMysql()
    OpenSqlliteDb()
    begin_Thread()
    closeMysql()

if __name__ == '__main__':
    main()

