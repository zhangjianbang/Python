# -*- coding: utf-8 -*

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
import time

connection = pymysql.connect(host = "58fb51768d38f.bj.cdb.myqcloud.com", port = 7218, user = "cdb_outerroot", password = "mysql2017", db = "letv", charset = "utf8")
head = {'User-Agent':'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36'}
def save(column_title, vid, title, play, comments, commenttext):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `letv` WHERE `vid`="+vid
            cursor.execute(sql)
            result = cursor.fetchall()          
            if len(result) == 0:
                sql = "INSERT INTO `letv` (`vid`, `column_title`, `title`, `play`, `comments`, `comment_content`) VALUES (%s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (vid, column_title, title, play, comments, commenttext))

            else:
                assert(len(result) == 1)
                sql = "UPDATE `letv` SET `play` = %s, `comments` = %s, `comment_content` = %s WHERE `vid` = %s"
                cursor.execute(sql,(play, comments, commenttext, vid))
        connection.commit()
    except Exception as e:
        print("db error", e)
def column(category_title, column_title, column_url, json_url_head):
    #标题、发布时间、播放量、评论量、评论内容
    try:
        num = 1
        while num>0:
            json_url = json_url_head + "/queryvideolist?callback=jQuery19108592363288873253_1497924025137&orderType=0&currentPage="+str(num)
            json_text = bs(json_url)
            title = re.findall(r'"title":"(.*?)"',json_text)
            time = re.findall(r'uploadTime":"(.*?)"',json_text)
            play = re.findall(r'"hits":(.*?),',json_text)
            vid = re.findall(r'"vid":(.*?),',json_text)
            for i in range(len(play)):
                #获取评论数据
                comment_page = 1
                comments = 0
                commenttext = ''
                while comment_page>0:
                    comment_url = 'http://api.my.le.com/vcm/api/list?jsonp=jQuery19100517617442119056_1497936963143&cid=40&type=video&rows=20&page='+str(comment_page)+'&sort=&source=1&xid='+vid[i]+'&pid=10018724&ctype=cmt%2Cimg%2Cvote&listType=2'        
                    comment_text = bs(comment_url)
                    if len(comment_text)<200:
                        break
                    else:
                        comment = re.findall(r'content":"(.*?)"',comment_text)
                        for c in comment:
                            commenttext = c +'\n'+commenttext
                            comments = comments + len(comment)
                        comment_page = comment_page + 1
             
                print(title[i]+','+time[i]+','+play[i]+','+str(comments)+','+commenttext)
                save(column_title, vid[i], title[i], play[i], comments, commenttext)
            
            if len(json_text)<1000:
                break
            else:
                num  = num +1       
    except:
        traceback.print_exc()

#获取栏目标题和视频页URL
def ranking(category_title, url):
    try:
        title_list = []
        text = bs(url)
        soup = BeautifulSoup(text,'html.parser')
        d_img = soup.find_all('dd',attrs={'class':'d_img'})
        for d in range(len(d_img)):
            href = re.findall(r'<a href="(.*?)"',str(d_img[d]))[0]
            column_text = bs(href)
            userId = re.findall(r'userId:"(.*?)"',column_text)[0]
            column_url = 'http://chuang.le.com/u/'+userId+'/videolist'
            json_url_head = 'http://chuang.le.com/u/'+userId
            print(column_url)
            column_text = bs(column_url)
            #column标题和总播放量
            column_title = re.findall(r'<title>(.*?)</title>',column_text)[0]
            column_play = re.findall(r'<i class="bf_num">(.*?)</i>',column_text)[0]
            if column_title not in title_list:
                title_list.append(column_title)
                column(category_title, column_title, column_url, json_url_head)
                #print('title ok!')
            else:
                print('title error')
            
    except:
        traceback.print_exc()

        
#解析html
def bs(url):
    try:
        r = requests.get(url, headers = head)
        r.encoding = r.apparent_encoding
        r.raise_for_status
        return r.text
    except:
        traceback.print_exc()

        
if __name__=="__main__":
    try:
        t = time.time()
        jc = []
        ident = OrderedDict([('生活',"http://life.le.com/"),('资讯',"http://news.le.com/"),('风尚',"http://fashion.le.com/"),
                             ('旅游',"http://travel.le.com/"),('汽车',"http://auto.le.com/"),('亲子',"http://qinzi.le.com/"),
                             ('宠物',"http://pets.le.com/"),('科技',"http://tech.le.com/"),('教育',"http://edu.le.com/")])
        for i in ident.keys():
            p = Process(target=ranking, args=(i, ident[i]))
            jc.append(p)
            p.start()
            
        print("the process was end!")
        print(time.time()-t)
    except:
        traceback.print_exc()

