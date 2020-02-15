import scrapy

class SinaSpider(scrapy.Spider):
	name='sina'
	def start_requests(self):
		yield scrapy.Request(url='http://sina.com.cn',callback=self.parse)

	def parse(self,response):
		with open('./test.html','w') as f:
			f.write(response.text)
		pass