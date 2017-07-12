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
from util import *
from datetime import datetime

'''
    待解决的问题:
    1.评论内容获取
    2.微博界面下拉，无法获取json地址的参数，涉及到ajax知识
'''

def save(platform_title, category_title, column_title, time, title, length, comment_content, where, forward, comment, play):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `weibo` WHERE `platform`=%s AND 'category'=%s AND 'column'=%s AND 'title'=%s"
            cursor.execute(sql, (platform_title, category_title, column_title, title,))
            result = cursor.fetchall()
            if len(result) == 0:
                sql = "INSERT INTO `weibo` (`platform`, `category`, `column`, `time`, `title`, `length`, `comment_content`, `play_{}`, `comment_{}`, `forward_{}`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(where, where, where)
                cursor.execute(sql, (platform_title, category_title, column_title, time, title, length, comment_content, play, comment, forward))

            else:
                assert(len(result) == 1)
                sql = "UPDATE `weibo` SET `play_{}` = '{}', `commnent_{}` = '{}', `forward_{}` = '{}', `comment_content` = '{}' WHERE `id` = {}".format(where, play, where, comment, where, forward, comment_content, result[0])
                cursor.execute(sql)
        connection.commit()
    except Exception as e:
        print("db error", e)
        
def get(url):
    r = session.get(url,headers=headers)
    r.raise_for_status()
    text = r.text
    return text.encode('utf-8').decode('utf-8')

def crawl_column(category_title, column_url, column_title):
    '''
    no.2
    '''
    print(column_title)
    print(column_url)
    text = get(column_url)
    times = re.findall(r'class=\\"WB_from S_txt2\\">\\n\s+<a\s+name=.*?title=\\"(.*?)\\"',text)
    titles = re.findall(r'class=\\"WB_from S_txt2\\">.*?>\\n(.*?)<\\\/div>',text)
    forward = re.findall(r'<span class=\\"line S_line1\\"\s+node-type=\\"forward_btn_text\\">.*?<em>(.*?)<\\\/em>',text)
    forward = [int(f) if f != u'转发' else 0 for f in forward]
    comment = re.findall(r'<span class=\\"pos\\"><span class=\\"line S_line1\\"\s+node-type=\\"comment_btn_text\\">.*?<em>(.*?)<\\\/em>',text)
    comment = [int(c) if c != u'评论' else 0 for c in comment]
    zan = re.findall(r'<span class=\\"pos\\"><span class=\\"line S_line1\\">.*?<em>(.*?)<\\\/em>',text)
    zan = [int(z) if z != u'赞' else 0 for z in zan]
    comments = ['comment1; comment2; comment3...' for i in range(len(times))]
    for i in range(len(times)):
        release_time = datetime.strptime(times[i], '%Y-%m-%d %H:%M')
        #print(release_time)--将格式字符串转化为datetime对象
        tmp = datetime.now() - release_time
        #print(tmp.days)--当前时间与抓取的日期进行相减，得到整数
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
        titles[i] = re.compile(r'[^\u2E80-\uFE4F]').sub('',titles[i]).strip()
        save('weibo', category_title, column_title, release_time, titles[i], 0, comments[i], str(where), forward[i], comment[i], zan[i])
             
def crawl_category(category_title, category_url):
    '''
    no.1
    homeurls--推荐页栏目的url
    titles--栏目标题
    '''

    try:
        homeurls = []
        titles = []
        for page in range(5):
            text = get(category_url+"?page="+str(page+1))
            hoturl = re.findall(r'class=\\"mod_pic\\">\\r\\n\s+<a href=\\"(.*?)\\"',text)
            homeurls += [item.replace('\\','').split('?')[0] + "?profile_ftype=1&is_all=1#_0" for item in hoturl]
            titles += re.findall(r'class=\\"mod_pic\\">\\r\\n\s+<a href=\\".*?title=\\"(.*?)\\"',text)
        for url, title in zip(homeurls, titles):
            crawl_column(category_title, url, title)
            #print(title+'/n'+url)
    except:
        traceback.print_exc()
    

if __name__=="__main__":

    login('15192893836', '6892545bang')
    connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "weibo", charset = "utf8")
    crawl_category('育儿', "http://d.weibo.com/1087030002_2975_2018_0")
    connection.close()
