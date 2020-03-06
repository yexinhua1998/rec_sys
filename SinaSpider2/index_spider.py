import sys
import requests
import datetime as dt
import threading
from utils import get_db_conn,get_url,get_time_num,get_lid_list,num2date
import requests
import sys
import re

def parse_index(index):
	'''
	解析index的内容
	index(str):index的内容
	return url_list(list):对应url的列表
	'''
	data=eval(index)['result']['data']
	url_list=[item['url'].replace('\\','') for item in data]
	del data
	return url_list


def crawl_l(lid,start_date,start_page,end_date):
	'''
	爬取一个栏目的index
	lid(int):栏目id
	start_date(dt.datetime):开始的日期
	start_page(int):开始的页数
	end_date(dt.datetime):结束的时间
	'''
	conn=get_db_conn()
	cursor=conn.cursor()
	link_matcher=re.compile('https?://(.*)')
	page=start_page
	date=start_date
	while True:
		url=get_url(date,lid,page)
		print('crawling,date=%s,lid=%d,page=%d'%(str(date)[:10],lid,page))
		r=requests.get(url)
		r.raise_for_status()
		url_list=parse_index(r.text)
		#插入到数据库
		for content_url in url_list:
			link=link_matcher.match(content_url).group(1)
			cursor.execute('SELECT insert_url_to_be_get(\'%s\');'%link)
			result=cursor.fetchall()[0][0]
			if not result:
				print('已存在，url=%s'%link)

		if len(url_list)==0:
			#爬取下一天的index
			date=date+dt.timedelta(days=1)
			page=1
			if date>=end_date:
				break
		else:
			#爬取下一页的数据
			page=page+1

		#更改数据库状态
		etime=get_time_num(date)
		cursor.execute('UPDATE index_to_be_get SET page=%d,etime=%d WHERE lid=%d;'%(page,etime,lid))
		conn.commit()
	conn.close()


def crawl_index(mode='start',start_date=dt.datetime(2018,1,1),end_date=dt.datetime.now()):
	'''
	mode(str):使用的模式,有'start','continue'。'start'代表爬虫重新启动，'continue'代表使用原有数据进行爬取
	start_date(dt.datetime):开始日期，当mode为start的时候，作为爬取新闻开始的日期
	'''
	tlist=[]
	conn=get_db_conn()
	cursor=conn.cursor()
	if mode=='start':
		lid_list=get_lid_list()
		etime=get_time_num(start_date)
		for lid in lid_list:
			cursor.execute('INSERT INTO index_to_be_get VALUES(%d,%d,%d);'%(etime,lid,1))
			tlist.append(threading.Thread(target=crawl_l,args=(lid,start_date,1,end_date)))
			conn.commit()
	elif mode=='continue':
		cursor.execute('SELECT * FROM index_to_be_get;')
		data=cursor.fetchall()
		for etime,lid,page in data:
			s=num2date(etime)
			tlist.append(threading.Thread(target=crawl_l,args=(lid,s,page,end_date)))
	conn.close()

	for t in tlist:
		t.start()
	for t in tlist:
		t.join()

if __name__=='__main__':
	crawl_index(mode=sys.argv[1])