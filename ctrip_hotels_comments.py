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
import time


class SpiderComments():
    def retry_if_Conn_error(exception):
        print("重试")
        return isinstance(exception, ConnectionError)

    def get_ip_list(self):
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
    def spider(self, cityName, hotelId, pageNo):
        url = "https://m.ctrip.com/restapi/soa2/16765/gethotelcomment?&_fxpcqlniredt=09031150210510028466"

        data = {"hotelId": hotelId,
                "pageIndex": pageNo,
                "tagId": 0, "pageSize": 20,
                "groupTypeBitMap": 2,
                "needStatisticInfo": 0,
                "order": 0,
                "basicRoomName": "",
                "travelType": -1,
                "head": {"cid": "09031150210510028466", "ctok": "",
                         "cver": "1.0", "lang": "01", "sid": "8888",
                         "syscode": "09", "auth": "", "xsid": "", "extension": []}
                }
        header = {
            'User-Agent': UserAgent().random,
        }
        data = json.dumps(data).encode(encoding='utf-8')
        attempts = 0
        success = False
        # 请求失败的话重试
        while attempts < 5 and not success:
            try:
                proxies = random.choice(self.get_ip_list())
                resp = requests.post(url=url, data=data, proxies=proxies,
                                     headers=header)
                if resp.status_code == 200:
                    comoment_list = self.get_spot_comments(cityName, resp)
                    self.write_to_excel(comoment_list)
                    print("写入{} {} 第 {} 页".format(cityName, hotelId, pageNo+1))
                    success = True
                    time.sleep(3)
                if resp.status_code == 431:
                    print(resp.text)
                    time.sleep(2)
            except:
                attempts += 1
                print(attempts)
                time.sleep(3)
                if attempts == 5:
                    print("请求失败")
                    header['Connection'] = 'close'
                    break

    def get_spot_comments(self, cityName, resp):
        data = json.loads(resp.text)
        comments_temp = data["othersCommentList"]
        comments_list = []
        for comments in comments_temp:
            temp = [
                cityName,
                data['hotelName'],
                comments['id'],
                comments['userNickName'],
                comments['baseRoomName'],
                comments['checkInDate'],
                comments['postDate'],
                comments['ratingPoint'],
                comments['ratingPointDesc'],
                comments['travelType'],
                comments['content']]
            comments_list.append(temp)
        return comments_list

    def write_to_excel(self, list):
        with open(self.csv_file, 'at', encoding='utf-8-sig', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerows(list)

    def init_csv(self):
        if os.path.exists(self.csv_file):
            os.remove(self.csv_file)
        headrow = [['城市', '酒店名称', '用户id', '用户名称', '房间类型',
                    '入住日期', '评论日期', '评分', '评价', '出行类型', '评论内容']]
        self.write_to_excel(headrow)

    def run_all_hotels(self):
        '''多线程运行 目的是获取每一个实例的所有日志列表'''
        threadPools = []
        #
        task_size = 20
        for i in range(task_size):
            thread_name = 'Thread-spots-'+str(i)
            t = threading.Thread(
                target=self.get_all_comments, name=thread_name, kwargs={'comment_queue': self.comment_queue})
            threadPools.append(t)
        for t in threadPools:
            t.start()
        for t in threadPools:
            t.join()

    def get_all_comments(self, comment_queue):
        while not self.comment_queue.empty():
            cityName, hotelId, page_no = self.comment_queue.get()
            self.spider(cityName, hotelId, page_no)

    def enqueue_data(self, hotel_list):
        for hotel in hotel_list:
            cityName = hotel[0]
            hotelId = hotel[2]
            subsribeSum = int(hotel[3].replace(',', ''))
            # 每页20个  计算总页数
            pageSum = int(subsribeSum / 20)
            # if pageSum>20:
            #     pageSum =20
            # pageSum = 100

        # for cityName in data:
        #     spotsInfo = data[cityName]
        #     for spot in spotsInfo:
            for pageNo in range(0, pageSum+1):
                self.comment_queue.put([cityName, hotelId, pageNo])

    def __init__(self):

        root_dir = os.path.abspath('.')
        self.csv_file = os.path.join(root_dir, '携程全酒店评论.csv')

        self.comment_queue = queue.Queue()

    def main(self, hotel_list):
        self.enqueue_data(hotel_list)
        self.run_all_hotels()


if __name__ == '__main__':
    SpiderComments().init_csv()
    start = datetime.datetime.now().replace(microsecond=0)
    cityName = '北京'
    hotelId = 1563509
    hotelName = '大饭店'
    subscribe = '1'
    hotel_list = [cityName, hotelName, hotelId, subscribe]
    SpiderComments().main([hotel_list])
    end = datetime.datetime.now().replace(microsecond=0)
    print(end)
    print('一共用时{}秒'.format(end-start))
