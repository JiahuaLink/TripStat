# -*- encoding: utf-8 -*-
'''
@File    :   ctrip.py
@Time    :   2020/04/25 19:42:54
@Author  :   JiahuaLink
@Version :   1.0
@Contact :   840132699@qq.com
@License :   (C)Copyright 2020, Liugroup-NLPR-CASIA
@Desc    :   携程酒店爬取
'''

# here put the import lib

import csv
import re
import time
from retrying import retry
from multiprocessing import Process
from fake_useragent import UserAgent
import multiprocessing
from lxml import etree
import requests
import pandas as pd
import json
import os
import random
import queue
import datetime
import threading
from ctrip_hotels_comments import SpiderComments


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


def retry_if_Conn_error(exception):
    return isinstance(exception, ConnectionError)


# 请求参数
# 注意https://hotels.ctrip.com/hotels/listPage?city=2&countryId=0&checkin=2020/04/22&checkout=2020/04/23&optionId=2&optionType=City&display=%E4%B8%8A%E6%B5%B7&crn=2&adult=3&children=4&searchBoxArg=t&travelPurpose=0&ctm_ref=ix_sb_dl&domestic=1&pageNo=3
# pageNo是分页参数
# 此函数可以控制爬取页数


def getPageParm(pageNum, cityeName, cityId):
    pas = []
    for i in range(1, pageNum+1):
        param = {
            'cityename': cityeName,
            'city': cityId,
            'checkin': '2020/04/25',
            'checkout': '2020/04/26',
            'crn': '1',
            'adult': '2',
            # 'children': '1',
            # 'ages': '1,2,3,4',
            'pageNo': i
        }
        pas.append(param)
    return pas

# 爬虫函数
@retry(retry_on_exception=retry_if_Conn_error)
def spider(parm):
    # 请求头文件
    headers = {
        'pragma': 'no-cache',
        'dnt': '1',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN,zh;q=0.9',
        'upgrade-insecure-requests': '1',
        'user-agent': UserAgent().random,
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'cache-control': 'no-cache',
        'authority': 'hotels.ctrip.com',
    }

    proxies = random.choice(get_ip_list())
    # print("代理 ip：{}".format(proxies))

    resp = requests.get(base_url, headers=headers,
                         params=parm)
    if resp.status_code == 200:
        hotel_list = get_hotel_list(resp)
        if hotel_list:
            # csv_file = os.path.join(root_dir, cityeName+'_携程酒店.csv')
            write_to_excel(hotel_list)
            SpiderComments().main(hotel_list)
        
        
    else:
        print("请求失败")
        headers['Connection'] = 'close'
        pass


def get_hotel_list(resp):
    hotel_list = []
    full_list = []
    # print(resp.text)
    # 文本解析，根据响应头提供的编码进行文本处理
    text = resp.text
# 利用lxml进一步解析成xpath可识别的格式
    html = etree.HTML(text)
    item = {}
# 利用xpath获取到json数据，获取到的是一个列表
    context = html.xpath('//body//script[2]/text()')[0]
    str1 = 'window.IBU_HOTEL = '
    # str2里的字符串1.23后面的参数经常变动，所以建议直接切片处理如f2
    # str2='__webpack_public_path__ = "//webresource.c-ctrip.com/ares2/hotel/smart/1.23.{}/default/";'
    # str3=str2.format(len(str2))
# 进行json数据处理，strip去掉两边的空格；replace替换文本函数，为了用python将json转换为字典格式
    f1 = context.strip().replace(str1, '')
    #
    k = re.compile(r"\n__webpack_public_path__.*?.*")
    str2 = k.findall(f1)[0]
    f2 = f1.replace(str2, '')
    # print(f2)

# 利用loads函数将json转换为字典格式

    data = json.loads(f2)

    # full_list = data['initData']['firstPageList']['hotelList']['list']
    try:
        full_list = data['initData']['firstPageList']['hotelList']['list']
    except:
        pass
    if full_list:
        for fl in full_list:
            # 报错处理，用抛出异常解决
            try:
                item['name'] = fl['base']['hotelName']
            except:
                item['name'] = '暂无'
            try:
                item['enname'] = fl['base']['hotelEnName']
            except:
                item['enname'] = '暂无'
            try:
                item['score'] = fl['score']['number']
            except:
                item['score'] = '暂无'
            try:
                item['star'] = fl['base']['star']
            except:
                item['star'] = '暂无'
            try:
                item['tags'] = fl['base']['tags']
            except:
                item['tags'] = '暂无'
            try:
                item['isfull'] = fl['base']['isFullRoom']
            except:
                item['isfull'] = ''
            try:
                item['city'] = fl['position']['cityName']
            except:
                item['city'] = ''
            try:
                item['address'] = fl['position']['address']
            except:
                item['address'] = ''
            try:
                item['content'] = fl['comment']['content'].replace('条点评', '')
            except:
                item['content'] = '0'
            try:
                item['quality'] = fl['comment']['quality']
            except:
                item['quality'] = '暂无评分'
            try:
                item['price'] = fl['ctripTrace']['listPrice_cx']
            except:
                item['price'] = ''
            try:
                item['url'] = "https://hotels.ctrip.com/hotels/detail/?hotelId=" + \
                    fl['base']['hotelId']
            except:
                item['url'] = ''
            # 利用pandas写入csv
            # col不是列名，是索引名，控制列位置
            hotel_temp = [item['city'], item['name'],fl['base']['hotelId'], item['content'], item['score'], item['star'], item['tags'], item['isfull'],
                    item['address'], item['quality'], item['url']]
            # comments_temp = [item['city'],fl['base']['hotelId'], item['name'],int(item['content'])]
            hotel_list.append(hotel_temp)
    return hotel_list


def write_to_excel(list):
    
    with open(csv_file, 'at', encoding='utf-8-sig', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerows(list)


def init_csv():
    if os.path.exists(csv_file):
        os.remove(csv_file)
    headrow = [['城市', '酒店名称', '酒店ID','订购数', '评分', '星级', '标签', '是否满员',
                '地址', '内容', '链接']]
    write_to_excel(headrow)


def enqueue_city(cityInfo):
    for city in cityInfo:
        parms = getPageParm(pageNum, city[0], city[1])
        if parms:
            for pas in parms:
                hotel_queue.put(pas)


def run_all_hotels():
    '''多线程运行 目的是获取每一个实例的所有日志列表'''
    threadPools = []
    # 每次批跑任务个数
    task_size = 10
    for i in range(task_size):
        thread_name = 'Thread-hotel-'+str(i)
        t = threading.Thread(
            target=get_all_hotels, name=thread_name, kwargs={'hotel_queue': hotel_queue})
        threadPools.append(t)
    for t in threadPools:
        t.start()
    for t in threadPools:
        t.join()


def get_all_hotels(hotel_queue):
    while not hotel_queue.empty():
        pas = hotel_queue.get()
        spider(pas)


if __name__ == '__main__':
    start = datetime.datetime.now().replace(microsecond=0)
    print(start)
    base_url = 'https://hotels.ctrip.com/hotels/listPage'
    root_dir = os.path.abspath('.')
    ip_config = os.path.join(root_dir, "ip.txt")
    csv_file = os.path.join(root_dir, '携程酒店信息.csv')
    hotel_queue = queue.Queue()
    init_csv()
    # 爬取页数
    pageNum = 10
    cityInfo = [['beijing', '1'],
                ['suzhou', '14'],
                ['guangzhou', '32'],
                ['kunming', '34'],
                ['sanya', '43']
                ]
    enqueue_city(cityInfo)
    run_all_hotels()
    end = datetime.datetime.now().replace(microsecond=0)
    print(end)
    print('一共用时{}秒'.format(end-start))
