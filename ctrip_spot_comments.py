from fake_useragent import UserAgent
import requests
import json

import os
import csv
import queue
from retrying import retry
import threading
import random
import datetime


def retry_if_Conn_error(exception):
    return isinstance(exception, ConnectionError)


def get_ip_list():
    root_dir = os.path.abspath('.')
    ip_config = os.path.join(root_dir, "ip.txt")
    ip_list = []
    with open(ip_config) as f:
        lines = f.readlines()
    for line in lines:

        li_dic = {'http': line.strip('\n')}
        ip_list.append(li_dic)
    return ip_list


@retry(retry_on_exception=retry_if_Conn_error)
def spider(cityName, spotName, spotId, pageno):
    url = "https://sec-m.ctrip.com/restapi/soa2/12530/json/viewCommentList?_fxpcqlniredt=09031150210510028466"

    data = {"pageid": 10650000804,
            "viewid": spotId,
            "tagid": 0,
            "pagenum": pageno,
            "pagesize": 100,
            "contentType": "json",
            "head": {
                "appid":
                "100013776",
                "cid": "09031150210510028466",
                "ctok": "",
                "lang": "01",
                "sid": "8888",
                "syscode": "09",
                "auth": "",
                "extension": []}
            }
    data = json.dumps(data).encode(encoding='utf-8')
    header = {
        'User-Agent': UserAgent().random,
    }
    proxies = random.choice(get_ip_list())
    resp = requests.post(url=url, data=data, proxies=proxies, headers=header)

    # print(resp.text)
    # return resp.text
    if resp.status_code == 200:
        comoment_list = get_spot_comments(cityName, spotName, resp)
        write_to_excel(comoment_list)
        print("写入{} {} 第 {} 页".format(cityName, spotName, pageno))
    else:
        print("请求失败")
        header['Connection'] = 'close'
        pass


def get_spot_comments(cityName, spotName, resp):
    data = json.loads(resp.text)
    comments_temp = data["data"]["comments"]
    comments_list = []
    for comments in comments_temp:
        temp = [cityName,
                spotName,
                comments['id'],
                comments['uid'],
                comments['memberLevel'],
                comments['memberName'],
                comments['date'],
                comments['score'],
                comments['sightStar'],
                comments['interestStar'],
                comments['costPerformanceStar'],
                comments['content']
                ]
        comments_list.append(temp)
    return comments_list


def write_to_excel(list):
    with open(csv_file, 'at', encoding='utf-8-sig', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerows(list)


def init_csv():
    if os.path.exists(csv_file):
        os.remove(csv_file)
    headrow = [['城市', '景点名称', '用户id', '用户名称', '成员等级', '会员等级', '评论日期', '分数',
                '游玩指数', '兴趣指数', '消费指数', '评论内容']]
    write_to_excel(headrow)


def run_all_spots():
    '''多线程运行 目的是获取每一个实例的所有日志列表'''
    threadPools = []
    #
    task_size = 300
    for i in range(task_size):
        thread_name = 'Thread-spots-'+str(i)
        t = threading.Thread(
            target=get_all_comments, name=thread_name, kwargs={'comment_queue': comment_queue})
        threadPools.append(t)
    for t in threadPools:
        t.start()
    for t in threadPools:
        t.join()


def get_all_comments(comment_queue):
    while not comment_queue.empty():
        cityName, spotName, spotId, pageno = comment_queue.get()
        spider(cityName, spotName, spotId, pageno)


def enqueue_data(data, pageSum):
    for cityName in data:
        spotsInfo = data[cityName]
        for spot in spotsInfo:
            for pageNo in range(1, pageSum+1):
                comment_queue.put([cityName, spot[0], spot[1], pageNo])


if __name__ == '__main__':
    pageSum = 300
    start = datetime.datetime.now().replace(microsecond=0)
    print(start)
    root_dir = os.path.abspath('.')
    csv_file = os.path.join(root_dir, '携程景点评论.csv')
    init_csv()
    comment_queue = queue.Queue()
    data = {
        "北京": [["故宫", 229], ["北京野生动物园", 107469], ["八达岭长城", 230]],
        "苏州": [["拙政园", 47072], ["周庄", 109861], ["虎丘", 3763]],
        "广州": [["广州塔", 107540], ["长隆野生动物世界", 6802], ["广州岭南印象园", 110338]],
        "昆明": [["石林", 44950], ["滇池", 2967], ["七彩云南欢乐世界", 4682346]],
        "三亚": [["亚龙湾热带天堂森林公园", 74247], ["蜈支洲岛旅游风景区", 3244], ["天涯海角", 3222]]
    }
    enqueue_data(data, pageSum)
    run_all_spots()
    end = datetime.datetime.now().replace(microsecond=0)
    print(end)
    print('一共用时{}秒'.format(end-start))
