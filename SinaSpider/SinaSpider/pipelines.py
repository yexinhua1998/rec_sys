# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from .utils import get_db_conn
import psycopg2
from scrapy.exceptions import DropItem

class db_save_pipeline:
	def open_spider(self,spider):
		self.conn=get_db_conn()

	def close_spider(self,spider):
		self.conn.commit()
		self.conn.close()

	def process_item(self,item,spider):
		cursor=self.conn.cursor()
		try:
			cursor.execute(
				'SELECT insert_page_content($url$%s$url$,$content$%s$content$);'%(item['url'],item['content']))
			self.conn.commit()
			return item
		except Exception as e:
			print('error=%s'%str(e))
			self.db_reconnect()
			print('drop item.')
			raise DropItem(str(e))

	def db_reconnect(self):
		#重置数据库连接
		self.conn.commit()
		self.conn.close()
		self.conn=get_db_conn()