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
from urllib.request import urlopen
from datetime import datetime
from datetime import timedelta
from collections import OrderedDict
from multiprocessing import Process, Lock

'''
    1.评论url的参数：objectId 可在源码抓取、time为请求的时间、sign未知！
    2.育儿ok、测试教育频道时会出现多值为None情况！！
'''
'''全局变量'''
#检测栏目标题是否重复
urllist = []
connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "youku", charset = "utf8")


def save(platform_title, category_title, column_title, subscribe, title, time, length, where, play, href):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `youku` WHERE `category`=%s AND `column_title`=%s AND `title`=%s"
            cursor.execute(sql, (category_title, column_title, title))
            result = cursor.fetchall()
            if len(result) == 0:
                sql = "INSERT INTO `youku` (`platform`, `category`, `column_title`, `subscribe`, `title`, `time`, `length`, `play_{}`, `href`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s ,%s)".format(where)
                cursor.execute(sql, (platform_title, category_title, column_title, subscribe, title, time, length, play, href))
            else:
                assert(len(result) == 1)
                sql = "UPDATE `youku` SET `play_{}` = %s, `subscribe` = %s WHERE `id` = %s".format(where)
                cursor.execute(sql, (play, subscribe, result[0]))
        connection.commit()
    except:
        traceback.print_exc()
        
def bs(url):
    r = requests.get(url)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    text = r.text
    return text.encode('utf-8').decode('utf-8')    
def jsons(url):
    try:
        jsonr = requests.get(url)
        jsonr.raise_for_status()
        jsont = json.loads(jsonr.text)
        return jsont
    except:
        traceback.print_exc()
 
def crawl_column(category_title, column_title, column_url):
    try:
        text = bs(column_url)
        soup = BeautifulSoup(text,'html.parser')
        #订阅量
        subscribe = soup.find('li', attrs={'class':'snum'}).get('title').replace(',','')
        
        page = soup.find('ul',attrs={'class':'yk-pages'}).find_all('li')[-2].string
        for i in range(int(page)):
            pageurl = column_url+'?order=1&page='+str(i+1)
            text = bs(pageurl)
            soup = BeautifulSoup(text,'html.parser')
            #单页的数据区
            bodys = soup.find_all('div', attrs={'class':'v va'})
            now = datetime.now()
            for b in bodys:
                #通过判断有无发布时间来确定视频是否已经删除
                if not b.find_all(attrs={'class','v-publishtime'}):
                    continue
                else:
                    href = b.find('div',attrs={'class':'v-meta-title'}).find('a').get('href')
                    title = b.find('div',attrs={'class':'v-meta-title'}).find('a').get('title').strip()
                    time = b.find_all(attrs={'class','v-publishtime'})[0].string
                    play = b.find(attrs={'class','v-num'}).string
                    video_length = b.find('span',attrs={'class':'v-time'}).string
                    
                    #视频长度数据处理区
                    video_length = int(video_length.split(':')[0])*60 + int(video_length.split(':')[1])
                    
                    #播放量数据处理区
                    if '万' in play:
                        play = str(int(float(play.split('万')[0])*10000))
                    
                    #发布时间数据处理区
                    if re.findall(r'(\d+)小时前',time):
                        test = re.findall(r'(\d+)小时前',time)[0]
                        dt = now - timedelta(hours = int(test))
                        tests = now -dt
                    elif re.findall(r'(\d+)天前',time):
                        test = re.findall(r'(\d+)天前',time)[0]
                        dt = now - timedelta(days = int(test))
                        tests = now - dt             
                    elif re.findall(r'(\d+)分钟前',time):
                        test = re.findall(r'(\d+)分钟前',time)[0]
                        dt = now - timedelta(minutes = int(test))
                        tests = now - dt                    
                    elif re.findall(r'(\d+\-\d+\-\d+)',time):
                        dt = datetime.strptime(time,'%Y-%m-%d')
                        tests = now - dt                    
                    elif re.findall(r'(\d+\-\d+\s+\d+\:\d+)',time):
                        dts = '2017-'+time
                        dt = datetime.strptime(dts,'%Y-%m-%d %H:%M')
                        tests = now - dt                      
                    elif re.findall(r'昨天\s+(\d+\:\d+)',time):
                        test = re.findall(r'昨天\s+(\d+\:\d+)',time)
                        dt = now - timedelta(days=1)
                        tests = now - dt
                    elif re.findall(r'前天\s+(\d+\:\d+)',time):
                        test = re.findall(r'前天\s+(\d+\:\d+)',time)
                        dt = now - timedelta(days=2)
                        tests = now - dt
                    else:
                        print('datetime-error')
                        
                    #位置数据处理区
                    where = 0
                    if tests.days < 7:
                        where = tests.days
                    elif tests.days < 14:
                        where = 13
                    elif tests.days < 21:
                        where = 20
                    elif tests.days < 30:
                        where = 29
                    elif tests.days < 60:
                        where = 59
                    else:
                        where = 89
                    #print(title)
                    #print(title+'\n'+str(dt)+'\n'+str(where)+'\n'+play+'\n'+href)
                    #titles[i] = re.compile(r'[^\u2E80-\uFE4F]').sub('',titles[i]).strip()
                    #'youku'-'专栏标题'-'栏目标题'-'订阅量'-'视频标题'-'视频发布时间'-'视频长度'-'位置'-'播放量'-'地址'
                    save('youku', category_title, column_title, subscribe, title, dt, video_length, str(where), play, href)
                    print(title)
                    
    except:
        traceback.print_exc()

#获取推荐入口的url
def ranking(category_title, url):
    try:
        title_dh_list = []
        text = bs(url)
        soup = BeautifulSoup(text,'html.parser')
        div = soup.find_all('div',attrs={'class':'yk-box'})
        for d in div:          
            if not d.find('div',attrs={'class':'yk-title'}):
                print('未找到导航标题')
                continue
            else:    
                title_dh = d.find('div',attrs={'class':'yk-title'}).string
                if title_dh not in title_dh_list:
                    title_dh_list.append(title_dh)
                    print('#################'+title_dh)
                    div_link = d.find_all('div',attrs={'class':'v-link'})
                    #获取单个栏目的标题和url
                    for dl in div_link:
                        title = dl.find('a').get('title')
                        href = dl.find('a').get('href')
                        crawl_category(category_title, href, title)
                
                        
    except:
        traceback.print_exc()
        
'''获取栏目的栏目名和栏目主页地址'''
def crawl_category(category_title, href, title):
    '''
    no.1
    column_url--栏目主页的url
    column_title--栏目名称
    '''
    try:
        text = bs(href)
        videoOwner = re.findall(r'videoOwner:"(.*?)"',text)[0]
        videoId = re.findall(r'videoId:"(.*?)"',text)[0]
        json_url = 'http://v.youku.com/action/sub?beta&callback=jQuery1112025935302970611196_1493381890724&+vid='+videoId+'&ownerid='+videoOwner
        text = bs(json_url)
        column_url = re.findall(r'channelurl":"(.*?)",',text)[0]
        column_url = 'http:'+column_url.replace('\\','').split('==')[0]+'==/videos'
        texts = bs(column_url)
        column_title = re.findall(r'<title>(.*?)<',texts)[0]
        #crawl_column(category_title, column_title, column_url)
        #print(column_title+'\n'+title)
        if column_title not in urllist:
            urllist.append(column_title)
            #print(column_title+'\n'+column_url)
            crawl_column(category_title, column_title, column_url)
        else:
            print("标题重复！")
    except:
        traceback.print_exc()


if __name__=="__main__":
    try:
        
        jc = []
        #connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "youku", charset = "utf8")
        ident = OrderedDict([('育儿',"http://baby.youku.com/"),('教育',"http://edu.youku.com/"),('时尚',"http://fashion.youku.com/"),('旅游',"http://travel.youku.com/"),
                             ('搞笑',"http://fun.youku.com/"),('娱乐',"http://ent.youku.com/"),('财经',"http://finance.youku.com/"),('科技',"http://tech.youku.com/"),
                             ('汽车',"http://auto.youku.com/"),('公益',"http://gongyi.youku.com/"),('纪实',"http://jilupian.youku.com/"),('资讯',"http://news.youku.com/")])
        for i in ident.keys():
            p = Process(target=ranking, args=(i, ident[i]))
            jc.append(p)
            p.start()
        
        ''' 
        connections = [pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "youku", charset = "utf8") for i in ident.keys()]
        for j,i in enumerate(ident.keys()):
            p = Process(target=ranking, args=(connections[j], i, ident[i]))
            jc.append(p)
            p.start()
        
        for p in jc:
            p.join()
        for c in connections:
            c.close()
        '''
        
        print("the process was end!")
    except:
        traceback.print_exc()
