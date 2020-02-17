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
			link_matcher=re.compile('^https?://(.*)$')#提取地址，忽略协议
			sina_matcher=re.compile('^(.*)sina.com.cn(.*)$')#判断是否新浪网的链接
			index_matcher=re.compile('^(.*)sina.com.cn(/[^/]+/)?$')#判断是否是索引页的页面


			link=link_matcher.match(response.url).group(1)
			if index_matcher.match(link) is None:
				#不是索引页面，应该存储
				yield SinaPageItem(url=link,content=response.text)

			cursor=self.conn.cursor()

			hreflist=response.xpath('//a/@href').getall()
			for href in hreflist:
				#去掉协议头
				match=link_matcher.match(href)
				if match is None:
					continue
				link=match.group(1)

				#根据地址类型进行处理
				if sina_matcher.match(link) is None:
					#不是新浪网的链接
					continue
				if not index_matcher.match(link) is None:
					#是索引页，直接返回
					yield scrapy.Request(url='http://'+link,callback=self.parse)
				else:
					#不是索引页，根据是否已经爬取进行返回
					cursor.execute('SELECT is_url_getted($url$%s$url$);'%link)
					data=cursor.fetchall()
					if data[0][0]==True:
						#数据库已经有该页面的数据
						continue
					yield scrapy.Request(url='http://'+link,callback=self.parse)
			cursor.close()
		else:
			return []


