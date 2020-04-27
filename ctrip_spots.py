import requests
import json
from fake_useragent import UserAgent
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
def spider(cityNo, pageNo):
    url = "https://sec-m.ctrip.com/restapi/soa2/12530/json/ticketSpotSearch?_fxpcqlniredt=09031150210510028466"

    data = {
        "pageid": 10320662472,
        "searchtype": 1,
        "districtid": cityNo,

        "sort": 1,
        "pidx": pageNo,
        "psize": 100,
        "reltype": 7,
        "contentType": "json",
        "head": {
            "cid": "09031150210510028466",
            "ctok": "",
            "cver": "1.0",
            "lang": "01",
            "sid": "8888",
            "syscode": "09",
            "extension": []
        }
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
        spot_list = get_spot_list(resp)
        if spot_list:
            write_to_excel(spot_list)
            # csv_file = os.path.join(root_dir, cityNo+'_携程景区列表.csv')
            print("{}写入景区 第 {} 页".format(cityNo, pageNo))
    else:
        print("请求失败")
        headers['Connection'] = 'close'
        pass


def get_spot_list(resp):
    data = json.loads(resp.text)
    spots_temp = data["data"]["viewspots"]
    spots_list = []
    if spots_temp:

        for spots in spots_temp:
            temp = [data["data"]['title'],
                    spots['name'],
                    spots['star'],
                    spots['cmtscore'],
                    spots['cmttag'],
                    spots['feature'],
                    spots['commentCount']
                    ]
            spots_list.append(temp)

    else:
        pass
    return spots_list


def write_to_excel(list):
    with open(csv_file, 'at', encoding='utf-8-sig', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerows(list)


def init_csv():
    if os.path.exists(csv_file):
        os.remove(csv_file)
    headrow = [['城市', '景点名称', '星级', '评分', '评价', '特点', '订购数']]
    write_to_excel(headrow)


def run_all_spots():
    '''多线程运行 目的是获取每一个实例的所有日志列表'''
    threadPools = []
    #
    task_size = 100
    for i in range(task_size):
        thread_name = 'Thread-spots-'+str(i)
        t = threading.Thread(
            target=get_all_spots, name=thread_name, kwargs={'spot_queue': spot_queue})
        threadPools.append(t)
    for t in threadPools:
        t.start()
    for t in threadPools:
        t.join()


def get_all_spots(spot_queue):
    while not spot_queue.empty():
        cityNo, pageNo = spot_queue.get()
        spider(cityNo, pageNo)


def enqueue_data(data, pageSum):
    for cityNo in data:
        for pageNo in range(1, pageSum+1):
            spot_queue.put([cityNo, pageNo])


if __name__ == '__main__':
    pageSum = 15
    start = datetime.datetime.now().replace(microsecond=0)
    print(start)
    root_dir = os.path.abspath('.')
    csv_file = os.path.join(root_dir, '携程景点列表.csv')
    init_csv()
    spot_queue = queue.Queue()
    cityInfo = ['1', '11', '152', '29', '61']

    enqueue_data(cityInfo, pageSum)
    run_all_spots()
    end = datetime.datetime.now().replace(microsecond=0)
    print(end)
    print('一共用时{}秒'.format(end-start))
