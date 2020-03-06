import psycopg2
import datetime as dt
import requests

def get_db_conn():
	return psycopg2.connect(user='postgres',password='Ye25554160',database='sina_page',host='127.0.0.1',port=5432)

def get_time_num(datetime):
	'''
	给定一个datetime，返回对应的整数
	datetime:datetime.datetime对象，代表需要转换的时间
	return time_num:int 新浪新闻网对应的时间数字
	'''
	anchor_datetime=dt.datetime(2020,2,18)
	anchor_time_num=1581955200
	delta=datetime-anchor_datetime
	time_num=anchor_time_num+delta.total_seconds()
	return  int(time_num)

def num2date(num):
	'''
	给定时间整数，返回日期
	'''
	anchor_datetime=dt.datetime(2020,2,18)
	anchor_time_num=1581955200
	sec_delta=anchor_time_num-num
	result=anchor_datetime-dt.timedelta(seconds=sec_delta)
	return result


def get_url(datetime,lid,page):
	'''
	给定日期、lid、页号，返回url
	datetime: 日期，datetime.datetime实例
	lid:栏目id int
	page:页号 int
	return url:对应url str
	'''
	etime=get_time_num(datetime)
	stime=get_time_num(datetime+dt.timedelta(days=1))
	url='https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=%d&etime=%d&stime=%d&num=50&page=%d'%(lid,etime,stime,page)
	return url

def get_lid_list():
	'''
	获取lid的列表
	'''
	return [2510,2511,2669,2512,2513,2514,2515,2516,2517,2518]

def get_page(url):
	'''
	给定url，返回page的内容
	'''
	r=requests.get(url)
	r.encoding='utf-8'
	return r.text