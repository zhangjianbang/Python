import re
import requests
import json
import time
import pymysql
import traceback
from multiprocessing import Process
import time
from collections import OrderedDict


connection = pymysql.connect(host = "localhost", port = 3306, user = "root", password = "root", db = "data", charset = "utf8")

def category(category_title, category_name):
    for page in range(5):
        url = "http://www.toutiao.com/api/pc/feed/?category="+category_name+"&utm_source=toutiao&widen="+str(page+1)+"&max_behot_time=0&max_behot_time_tmp=0&tadrequire=true&as=A1750944C9302AB&cp=594970627A1B3E1"
        category_text = bs(url)
        category_json = json.loads(category_text)
        data = category_json['data']
        for i in range(len(data)):
            try:
                if not category_json['data'][i]['source']:
                    continue
                elif category_json['data'][i]['source'] == "头条问答":
                    continue
                elif not category_json['data'][i]['media_url']:
                    continue
                else:
                    user_id = category_json['data'][i]['media_url'].split('/')[-2]
                    fensi_url = 'http://www.toutiao.com'+category_json['data'][i]['media_url']
                    fensi_text = bs(fensi_url)
                    fensi = re.findall(r'fensi:\'(.*?)\'',fensi_text)[0]#粉丝数量
                    column_title = re.findall(r'<title>(.*?)</title>',fensi_text)[0]#栏目标题
                    column(category_title, user_id, fensi, column_title)

            except Exception as e:
                print('cateroty error', e)
                
def column(category_title, user_id, fensi, column_title):
    #最大取500条文章数据
    column_url = "http://www.toutiao.com/c/user/article/?page_type=1&user_id="+user_id+"&max_behot_time=1495209598&count=500"
    #获取粉丝数量
    #fensi = re.findall(r'fensi:(\d+),',column_text)
    column_json = json.loads(bs(column_url))
    #文章数量
    data = column_json['data']
    for d in range(len(data)):
        try:
            title = data[d]['title']
            play = data[d]['go_detail_count']
            group_id = data[d]['group_id']
            item_id = data[d]['item_id']
            #comments = data[d]['comments_count']
            comments, commenttext = comment(group_id, item_id)
            save(category_title, column_title, int(fensi), title, play, comments, group_id, item_id, commenttext)
            #print('\n'+category_title+'//'+column_title+'//'+fensi+'//'+title+'//'+str(play)+'//'+str(comments)+'//'+group_id+'//'+item_id+'//'+commenttext)
            print(title)
        except Exception as e:
            print('column error', e)
        
def comment(group_id, item_id):
    try:
        #最大取500条评论
        comment_url = "http://www.toutiao.com/api/comment/list/?group_id="+group_id+"&item_id="+item_id+"&offset=0&count=100"
        comment_json = json.loads(bs(comment_url))
        data_comment = comment_json['data']['comments']
        comments = comment_json['data']['total']
        commenttext = ''
        for c in range(len(data_comment)):
            comment = data_comment[c]['text']
            commentz  = re.compile(r'[^\u4e00-\u9fa5]')
            comment = str(commentz.sub('',comment))
            commenttext = comment+'\n'+commenttext
        return comments,commenttext
    except Exception as e:
        print('comment error', e)
        
def bs(url):
    try:
        s = requests.Session()
        cookies = {'cookie':'uuid="w:3d7709dcd2c9439d9127e12d5cca1bf3"; UM_distinctid=15b62972acc8b9-0d22f81a89c45c-4e47072e-100200-15b62972acd510; __utma=24953151.782722717.1492007726.1492282139.1492282139.1; __utmz=24953151.1492282139.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _ba=BA0.2-20170415-51d9e-dhuuLROF2jPp5AhDgYsf; tt_webid=6408129065186133505; csrftoken=051adc4da60b723c5bac0d154f44b63a; __tasessionId=dcg41bxs91498037972995; CNZZDATA1259612802=932514416-1492312948-%7C1498032884; _ga=GA1.2.782722717.1492007726; _gid=GA1.2.1527931367.1497956805'}
        s.get(url, headers = cookies)
        r = s.get(url, headers = cookies, timeout = 5)
        r.encoding = r.apparent_encoding
        r.raise_for_status
        return r.text
    except Exception as e:
        print('bs error', e)
def save(category_title, column_title, fensi, title, play, comments, group_id, item_id, commenttext):
    try:
        with connection.cursor() as cursor:
            sql = "SELECT `id` FROM `toutiao` WHERE `group_id`="+group_id
            cursor.execute(sql)
            result = cursor.fetchall()          
            if len(result) == 0:
                sql = "INSERT INTO `toutiao` (`category_title`, `column_title`, `fensi`, `title`, `play`, `comments`, `group_id`, `item_id`, `commenttext`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (category_title, column_title, fensi, title, play, comments, group_id, item_id, commenttext))
            else:
                assert(len(result) == 1)
                sql = "UPDATE `toutiao` SET `fensi` = %s, `play` = %s, `comments` = %s, `commenttext` = %s WHERE `group_id` = %s"
                cursor.execute(sql,(fensi, play, comments, commenttext, group_id))
        connection.commit()
    except Exception as e:
        print("db error", e)
if __name__=="__main__":
    try:
        t = time.time()
        jc = []
        ident = OrderedDict([('热点','news_hot'),('视频','video'),('社会','news_society'),('娱乐','news_entertainment'),('科技','news_tech'),('体育','news_sports'),('汽车','news_car'),
                             ('财经','news_finance'),('搞笑','funny'),('国际','news_world'),('时尚','news_fashion'),('旅游','news_travel'),('育儿','news_baby')])
        for i in ident.keys():
            p = Process(target=category, args=(i, ident[i]))
            jc.append(p)
            p.start()
        print(time.time()-t)
    except Exception as e:
        print('main error', e)
