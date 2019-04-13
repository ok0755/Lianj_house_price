#coding=utf-8
#python3.7
#author:zhoub  ok0755@126.com
import requests
import re
from lxml import etree
import csv
from fake_useragent import UserAgent
from time import sleep

import gevent
from gevent import monkey,pool
monkey.patch_socket()

class Lianj(object):
    def __init__(self):
        self.ua = UserAgent()
        self.position = re.compile("resblockPosition:\'(.*?)\',",re.S)

    def sh_position(self):
        ##ar = ['luohuqu','futianqu','nanshanqu','yantianqu','baoanqu','longhuaqu','guangmingqu','pingshanqu','dapengxinqu']  ## 深圳各区
        ar = ['luohuqu','futianqu','nanshanqu','yantianqu']
        return ar

    def pages(self,u,i,num_retry=2):
        sleep(2)
        self.headers = {'User-Agent':self.ua.random}
        url = 'https://sz.lianjia.com/xiaoqu/{}/pg{}/'.format(u,i)
        try:
            res=requests.get(url,headers = self.headers)
            page_source=res.text
            selectors=etree.HTML(page_source)
            res.close
            urls = selectors.xpath('.//div[@class="title"]/a/@href')
            return urls
        except:
            if num_retry > 0:
                sleep(2)
                self.pages(u,i,num_retry-1)
            elif num_retry == 0:
                with open(r'd:\lianj_sz_error.txt','a+') as f:
                    f.write('Pageerror:{}\n'.format(url))

    def total_pages(self,a):
        self.headers = {'User-Agent':self.ua.random}
        url = 'https://sz.lianjia.com/xiaoqu/{}/pg1/'.format(a)
        res = requests.get(url,headers=self.headers,timeout=10)
        page_source = res.text
        selectors=etree.HTML(page_source)
        res.close
        totalpages = selectors.xpath('.//div/@page-data')[0]
        total = eval(totalpages)['totalPage']
        print('{}共{}页'.format(a,total))
        return total

    def target_url(self,page,a,num_retry=2):
        print(page)
        self.headers = {'User-Agent':self.ua.random}
        try:
            res=requests.get(page,headers=self.headers,timeout=5)
            res.encoding='utf-8'
            page_source=res.text
            res.close
            selectors=etree.HTML(page_source)
            address = selectors.xpath('.//h1[@class="detailTitle"]/text()')[0]
            price = selectors.xpath('.//span[@class="xiaoquUnitPrice"]/text()')[0]
            info = selectors.xpath('normalize-space(.//div[@class="xiaoquInfo"])')

            pos = re.findall(self.position,page_source)
            ar = [a,pos[0],address,price,info]
            with open(r'd:\lianj_sz.csv','a+',newline='') as f:
                wf=csv.writer(f)
                wf.writerow(ar)
        except:
            if num_retry > 0:
                self.target_url(page,a,num_retry-1)
            elif num_retry == 0:
                with open(r'd:\lianj_sz_error.txt','a+') as f:
                    f.write('{},{}\n'.format(a,page))

if __name__=='__main__':
    lj=Lianj()
    p = pool.Pool(30)
    th = []
    ar = lj.sh_position()
    for a in ar:
        k = lj.total_pages(a)
        for i in range(1,int(k)+1):
            lists = lj.pages(a,i)
            print(a,i)
            for l in lists:
                th.append(p.spawn(lj.target_url,l,a))
            gevent.joinall(th)