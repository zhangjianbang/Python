#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import re
from bs4 import BeautifulSoup
import pymysql
import traceback
import os
import json
import chardet
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
import time
from multiprocessing import Process, Lock

'''
    mysql里comment是保留关键字
    获取评论jsonurl参数可能在单独的json中
'''
#global titleurl
titleurl = []
connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "tencent", charset = "utf8")
head = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36'}

def save(platform_title, category_title, column_title, subscribe, time, title, length, comment_content, where, play, content, url):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `tencent` WHERE `platform`=%s AND `category`=%s AND `column_title`=%s AND `title`=%s"
            cursor.execute(sql, (platform_title, category_title, column_title, title))
            result = cursor.fetchall()          
            if len(result) == 0:
                sql = "INSERT INTO `tencent` (`platform`, `category`, `column_title`, `subscribe`, `time`, `title`, `length`, `comment_content`, `play_{0}`, `content_{1}`,`url`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)".format(where, where)
                cursor.execute(sql, (platform_title, category_title, column_title, subscribe, time, title, length, comment_content, play, content, url))

            else:
                assert(len(result) == 1)
                sql = "UPDATE `tencent` SET `subscribe` = %s, `play_{0}` = %s, `content_{1}` = %s, `comment_content` = %s WHERE `id` = %s".format(where,where)
                cursor.execute(sql,(subscribe, play, content, comment_content, result[0]))
                #更新长度
                #sql = "update tencent set length=%s where id=%s"
                #cursor.execute(sql,(length,result[0]))
                #print(type(result[0]))
        connection.commit()
    except Exception as e:
        print("db error", e)
        
#获取文章/视频的url--栏目主页视频
def columnfile_video(category_title,href):
    try:
        
        text = bs(href)
        column_title = re.findall(r'<span class="txt" id="userInfoNick">(.*?)<',text)[0]
        #订阅量
        subscribe = re.findall(r'<span class="num j_rss_count">(.*?)<',text)[0]
        if subscribe.find('万') == -1:
            next
        else:
            subscribe = int(float(subscribe.replace('万',''))*10000)
        #global titleurl
        print(column_title+'\n'+str(subscribe))
        if column_title not in titleurl:
            titleurl.append(column_title)
            uin = re.findall(r"visited_euin.*?'(.*?)'",text)
            jsonurl = 'http://c.v.qq.com/vchannelinfo?otype=json&uin='+uin[0]+'&qm=1&pagenum=1&num=24'
            #print(jsonurl)
            jsontext = bs(jsonurl)
            page = 1
            while(len(jsontext)>500):
                titles = re.findall(r'title_s":"(.*?)",',jsontext)
                plays = re.findall(r'play_count":"(.*?)",',jsontext)
                times = re.findall(r'uploadtime":"(.*?)",',jsontext)
                urls = re.findall(r'url":"(.*?)",',jsontext)
                video_len = re.findall(r'duration":"(.*?)",',jsontext)
                #print(jsonurl)
                page  = page + 1
                jsonurl = 'http://c.v.qq.com/vchannelinfo?otype=json&uin='+uin[0]+'&qm=1&pagenum='+str(page)+'&num=24'
                jsontext = bs(jsonurl)
                #print(jsonurl)
                for i in range(len(titles)):
                    #获取评论vid
                    vid = urls[i].split('/')[-1].split('.')[0]
                    #print(vid)
                    vid_url = 'https://ncgi.video.qq.com/fcgi-bin/video_comment_id?otype=json&callback=jQuery19104520801019040641_1495288892103&op=3&vid='+vid
                    #print(vid_url)
                    #获取评论comment_id
                    vid_text = bs(vid_url)
                    comment_id = re.findall(r'comment_id":"(.*?)"',vid_text)[0]
                    #print(comment_id)
                    #获取评论内容
                    comment_url = 'https://coral.qq.com/article/'+comment_id+'/comment?'
                    #print(comment_url)
                    comment_text = bs(comment_url)
                    comment_json = json.loads(comment_text)
                    comments = comment_json['data']['commentid']
                    #print('评论量'+str(len(comments)))#评论量
                    commenttext = ''
                    if len(comments)<1:
                        #print('no comments')
                        next
                    else:
                        for c in comments:
                            comment = c['content']
                            commenttext = comment+'\n'+commenttext
                    #print(commenttext)#评论内容
                    if re.findall(r'(\d+)小时前',times[i]):
                        test = re.findall(r'(\d+)小时前',times[i])[0]
                        release_time = datetime.now() - timedelta(hours = int(test))
                        tmp = datetime.now() - release_time
                        print(release_time)
                    else:
                        release_time = datetime.strptime(times[i],'%Y-%m-%d')
                        tmp = datetime.now() - release_time
                        print(release_time)
                    where = 0
                    if tmp.days < 7:
                        where = tmp.days
                    elif tmp.days < 14:
                        where = 13
                    elif tmp.days < 21:
                        where = 20
                    elif tmp.days < 30:
                        where = 29
                    elif tmp.days < 60:
                        where = 59
                    else:
                        where = 89
                    if plays[i].find('万') == -1:
                        next
                    else:
                        plays[i] = int(float(plays[i].replace('万',''))*10000)
                    #print(plays[i])
                    #print('视频长度'+video_len[i])
                    video_len1 = int(str(video_len[i]).split(':')[0])
                    video_len2 = int(str(video_len[i]).split(':')[1])
                    #print(video_len1)
                    #print(video_len2)
                    video_length = video_len1*60+video_len2
                    #print(video_length)
                    print(titles[i])
                    save('tencent', category_title, column_title, subscribe, release_time, titles[i], video_length, commenttext, str(where), plays[i], len(comments), urls[i])
                    
    except:
        print("error")         

#获取栏目主页的url--入口在视频下方显示----大部分是这个！！！
def column_type(category_title,href):
    try:
        text = bs(href)
        soup = BeautifulSoup(text,'html.parser')
        url = soup.find('div',attrs={'class':'video_user'}).find('a').get('href')
        text = bs(url)
        if len(text)<2000:
            #print(text)
            url_video = re.findall(r'window.location.href="(.*?)"<',text)[0] + '/videos'
            columnfile_video(category_title,url_video)
            #print(url_video)
        else:
            soup = BeautifulSoup(text,'html.parser')
            url_video = 'http://v.qq.com' + soup.find('ul',attrs={'class':'tab_list'}).find_all('a')[1].get('href')
            #print(url_video)
            columnfile_video(category_title,url_video)
    except:
        print("column_error")
        
#获取栏目主页的url--入口在下拉栏目信息处
def column_type1(category_title,href):
    try:
        text = bs(href)
        soup = BeautifulSoup(text,'html.parser')
        a = soup.find('a',attrs={'class':'album_pic'})
        href = a.get('href')
        print(href)
    except:
        print("column_error")



#获取推荐入口栏目的url--顶端导航入口--不用采取，重复！！
def top_url(category_title,url):
    try:
        print("-------------------------->"+category_title)
        text = bs(url)
        soup = BeautifulSoup(text,'html.parser')
        div = soup.find_all('div',attrs={'class':'nav_area'})
        a = div[0].find_all('a')
        for i in a:
            href = i.get('href')
            #print(href)
            column_type(category_title,href)
    except:
        print("column_error")

#获取导航文字推荐入口
def daohang_url(category_title,url):
    try:
        #lock.acquire()
        print("-------------------------->"+category_title)
        text = bs(url)
        soup = BeautifulSoup(text,'html.parser')
        div = soup.find_all('div',attrs={'class':'mod_title'})
        for d in div:
            a = d.find_all('a')
            title = d.find(attrs={'class':'text_medium'})
            #print(title.string)
            href = []
            for i in a:
                allhref = i.get('href')
                if not allhref in href:
                    href.append(allhref)
            #print("导航文字入口url个数:"+len(href))
            for i in href:
                
                #print(i)
                column_type(category_title,i)
        #print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        #lock.release()
                    
    except:
        print("url_error")
        
#获取推荐入口栏目的url-全部图片推荐入口
def img_url(category_title,url):
    try:
        print("-------------------------->"+category_title)
        text = bs(url)
        soup = BeautifulSoup(text,'html.parser')
        div = soup.find_all('div',attrs={'class':'mod_bd'})
        for d in div:
            a = d.find_all('a')
            href = []
            for i in a:
                allhref = i.get('href')
                if not allhref in href:
                    href.append(allhref)
            #for i in href:
                #print(i)
                column_type(category_title,i)
    except:
        print("url_error")

#解析界面
def bs(obj):
    try:
        #coolie = {'cookie':'value'}
        #r = requests.get(obj,headers=cook)
        r = requests.get(obj,headers = head)
        r.encoding = r.apparent_encoding
        r.raise_for_status()
        text = r.text
        text = text.encode('utf-8').decode('utf-8')
        return text
    except:
        print("bs_error")

if __name__=="__main__":

    #connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "test", charset = "utf8")
    #url = "http://v.qq.com/baby/"
    #url = "http://v.qq.com/travel/"
    #url = "http://v.qq.com/life/"
    #url = "http://v.qq.com/news/"
    #url = "http://v.qq.com/child/"
    #url = "http://v.qq.com/tech/"
    #daohang_url('育儿',url)
    #print('++++++++++++++++++++++++++++++')
    #top_url('育儿',url)
    #print('++++++++++++++++++++++++++++++')
    #img_url('育儿',url)
    '''字典循环
    lists={'育儿':"http://v.qq.com/baby/", '旅游':"http://v.qq.com/travel/", '生活':"http://v.qq.com/life/" ,'新闻':"http://v.qq.com/news/",
           '少儿':"http://v.qq.com/child/", '原创':"http://v.qq.com/travel/", '财经':"http://v.qq.com/finance/" ,'搞笑':"http://v.qq.com/fun/",
           '房产':"http://v.qq.com/house/", '科技':"http://v.qq.com/vplus/zealer/videos"}
    for key,value in lists.items():
        #daohang_url('育儿',url)
        #print('++++++++++++++++++++++++++++++')
        #top_url(key,value)
        print(key+','+value)
        print('++++++++++++++++++++++++++++++')
        print(key+','+value)
        #img_url('育儿',url)
    '''
    #元祖列表循环
    
    ident = OrderedDict([('育儿',"http://v.qq.com/baby/"), ('旅游',"http://v.qq.com/travel/"), ('生活',"http://v.qq.com/life/") ,('新闻',"http://v.qq.com/news/"),
           ('少儿',"http://v.qq.com/child/"), ('原创',"http://v.qq.com/travel/"), ('财经',"http://v.qq.com/finance/") ,('搞笑',"http://v.qq.com/fun/"),
           ('房产',"http://v.qq.com/house/"), ('科技',"http://v.qq.com/tech/")])
    #t = time.time()
    jc = []
    '''
    for i in ident.keys():
        #print(i,ident[i])
        daohang_url(i,ident[i])
        #print('++++++++++++++++++++++++++++++')
        #top_url(i,ident[i])
        #print('++++++++++++++++++++++++++++++')
        #img_url(i,ident[i])
    #print(time.time()-t)
    '''
    t = time.time()
    lock = Lock()
    for i in ident.keys():
        p1 = Process(target=daohang_url, args=(i, ident[i]))
        p2 = Process(target=top_url, args=(i,ident[i]))
        p3 = Process(target=img_url, args=(i,ident[i]))
        jc.append(p1)
        jc.append(p2)
        jc.append(p3)
        p1.start()
        p2.start()
        p3.start() 
    print(time.time()-t)
