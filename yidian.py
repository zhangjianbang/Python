import requests
import re
import json
import pymysql

column_title_list = []#判断栏目标题是否重复

def category(url):
    try:
        category_text = bs(url)
        div_list = re.findall(r'<div class="list">(.*?)</div>',category_text)[0]
        a = re.findall(r'<a(.*?)/a>',div_list)
        del a[0], a[1]#删除首页和一点号
        for i in range(len(a)):
            category_title = re.findall(r'>(.*?)<',a[i])[0]#要闻、视频、段子、汽车、社会、娱乐、军事、体育、NBA、财经、科技、数码、美女、健康、时尚、搞笑、电台、旅游、
            data_channelid = re.findall(r'data-channelid="(.*?)"',a[i])[0]
            #抓取推荐页50个栏目
            column_urls = "http://www.yidianzixun.com/home/q/news_list_for_channel?channel_id="+data_channelid+"&cstart=10&cend=50&infinite=true&refresh=1&__from__=pc&multi=5&appid=web_yidian&_=1498115238527"
            column(category_title, column_urls)
            #print(column_urls)
    except Exception as e:
        print(e)

def column(category_title, column_urls):
    try:
        column_json = json.loads(bs(column_urls))
        json_num = column_json['result']
        for n in json_num:
            column_urls = n['url']
            if "www.yidianzixun.com" in column_urls:
                channel_id = re.findall(r'"channel_id":"(.*?)"',bs(column_urls))[0]
                column_url = "http://www.yidianzixun.com/channel/"+channel_id#栏目主页
                column_data(category_title, column_url, channel_id)
            else:
                continue

    except Exception as e:
        print(e)

def column_data(category_title, column_url, channel_id):
    try:
        url="http://www.yidianzixun.com/home/q/news_list_for_channel?channel_id="+channel_id+"&cstart=10&cend=500&infinite=true&refresh=1&__from__=pc&multi=5&appid=web_yidian&_=1498118614073"
        num = json.loads(bs(url))
        column_title =num['channel_name']#栏目标题
        bookcount = num['bookcount']#订阅量
        print(column_title)
        print(url)
        if column_title not in column_title_list:
            column_title_list.append(column_title)
            print(len(num['result']))
            for r in num['result']:
                title = r['title']#文章标题
                itemid = r['itemid']#文章id
                url = "http://www.yidianzixun.com/article/"+itemid#文章地址
                comments,commenttext = comment(itemid)
                #save(category_title, column_title, itemid, title, comments, commenttext, url)
                #print(category_title+'\n'+column_title+'\n'+itemid+'\n'+title+'\n'+str(comments)+'\n'+commenttext+'\n'+url)
                #print('------------------------')
                #print(title)
    except Exception as e:
        print(e)

def comment(itemid):
    try:
        comment_url = "http://www.yidianzixun.com/home/q/getcomments?_=1498120177106&docid="+itemid+"&s=&count=300&last_comment_id=&appid=web_yidian"
        comments = json.loads(bs_json(comment_url))['comments']#评论量
        commenttext = ''
        for c in comments:
            commenttext = c['comment'] +'\n'+ commenttext
        return len(comments),commenttext
    except Exception as e:
        print(e)
def save(category_title, column_title, itemid, title, comments, commenttext, url):
    try:
        with conn.cursor() as cursor:
            sql = "select `id` from `yidian` where `itemid`= %s"
            cursor.execute(sql,(itemid))
            result = cursor.fetchall()
            if len(result) == 0:
                sql = "insert into `yidian` (`category_title`, `column_title`, `itemid`, `title`, `comments`, `commenttext`, `url`) values(%s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql, (category_title, column_title, itemid, title, comments, commenttext, url))
            else:
                assert(len(result) == 1)
                sql = "update `yidian` set `comments` = %s, `commenttext` = %s where `itemid` = %s"
                cursor.execute(sql, (comments, commenttext, itemid))
        conn.commit()
    except Exception as e:
        print(e)

def bs(url):
    try:
        #cookies = "wuid=116931988803417; wuid_createAt=2017-04-28 20:54:13; JSESSIONID=1e04a2faa74730d08a40658f1debca1ea0e77b942079b1c52a73257b5e45cdf4; weather_auth=2; captcha=s%3A810f69693351945480735db2621450a5.Ri8d2PbhR%2B%2FV0weHIon87u%2FI1sDIonIxBB%2F86R8%2F%2BnE; Hm_lvt_15fafbae2b9b11d280c79eff3b840e45=1498109667,1498387518,1498390926; Hm_lpvt_15fafbae2b9b11d280c79eff3b840e45=1498390926; CNZZDATA1255169715=1825314600-1493381792-%7C1498386319; cn_9a154edda337ag57c050_dplus=%7B%22distinct_id%22%3A%20%2215bb4a045d69e8-05c69f38d3b3a9-4e47072e-100200-15bb4a045d7473%22%2C%22%24_sessionid%22%3A%200%2C%22%24_sessionTime%22%3A%201498390931%2C%22%24dp%22%3A%200%2C%22%24_sessionPVTime%22%3A%201498390931%7D; UM_distinctid=15bb4a045d69e8-05c69f38d3b3a9-4e47072e-100200-15bb4a045d7473"
        cookies = {'Cookie':'wuid=116931988803417; wuid_createAt=2017-04-28 20:54:13; JSESSIONID=1e04a2faa74730d08a40658f1debca1ea0e77b942079b1c52a73257b5e45cdf4; weather_auth=2; captcha=s%3A810f69693351945480735db2621450a5.Ri8d2PbhR%2B%2FV0weHIon87u%2FI1sDIonIxBB%2F86R8%2F%2BnE; Hm_lvt_15fafbae2b9b11d280c79eff3b840e45=1498109667,1498387518,1498390926; Hm_lpvt_15fafbae2b9b11d280c79eff3b840e45=1498390926; CNZZDATA1255169715=1825314600-1493381792-%7C1498386319; cn_9a154edda337ag57c050_dplus=%7B%22distinct_id%22%3A%20%2215bb4a045d69e8-05c69f38d3b3a9-4e47072e-100200-15bb4a045d7473%22%2C%22%24_sessionid%22%3A%200%2C%22%24_sessionTime%22%3A%201498390931%2C%22%24dp%22%3A%200%2C%22%24_sessionPVTime%22%3A%201498390931%7D; UM_distinctid=15bb4a045d69e8-05c69f38d3b3a9-4e47072e-100200-15bb4a045d7473'}
        s = requests.Session()
        s.get(url, headers=cookies)
        r = s.get(url, headers=cookies)
        r.encoding = r.apparent_encoding
        r.raise_for_status()
        return r.text
    except Exception as e:
        print('bs error', e)
def bs_json(url):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(e)

if __name__=="__main__":
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="root",
        password="root",
        db="data",
        charset="utf8")
    category("http://www.yidianzixun.com/channel/u13746")

