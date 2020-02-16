import scrapy
import re
from ..utils import get_db_conn
import numpy as np

class SinaPageItem(scrapy.Item):
	url=scrapy.Field()
	content=scrapy.Field()


class SinaSpider(scrapy.Spider):
	name='sina'
	start_urls=['http://www.sina.com.cn']

	def __init__(self):
		scrapy.Spider.__init__(self)
		self.conn=get_db_conn()

	def parse(self,response):
		#返回当前页面的内容
		if isinstance(response,scrapy.http.TextResponse):
			print('getted page:%s'%response.url)
			yield SinaPageItem(url=response.url,content=response.text)

			matcher=re.compile('http://(.*)sina.com.cn(.*)')
			cursor=self.conn.cursor()

			linklist=response.xpath('//a/@href').getall()
			#np.random.shuffle(linklist)#随机排列
			for link in linklist:
				if matcher.match(link) is None:
					#不是新浪网的链接
					continue
				cursor.execute('SELECT is_url_getted($url$%s$url$);'%link)
				data=cursor.fetchall()
				if data[0][0]==True:
					#数据库已经有该页面的数据
					continue
				yield scrapy.Request(url=link,callback=self.parse)
			cursor.close()
		else:
			return []


