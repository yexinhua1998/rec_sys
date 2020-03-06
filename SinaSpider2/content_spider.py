from utils import get_page,get_db_conn
import threading
from lxml import etree
from queue import Queue,Empty,Full

THREAD_NUM=128

def get_result(result):
	'''
	从result中返回单个元素
	'''
	if len(result)==0:
		return ''
	else:
		return result[0].strip()

def collect_content(line_list):
	'''
	从每一行的文字列表中整理出一个content
	'''
	content=''
	for line in line_list:
		content+=line.strip()+'\n'
	return content

def parse_content(text):
	'''
	给定content页面的text，返回row(dict)
	row包括title,content,keywords,tags等key
	'''
	empty_row=dict(title='',content='',keywords='',tags='')

	html=etree.HTML(text)
	
	result=html.xpath('/html/body/div[@class="main-content w1240"]'+
			'/h1[@class="main-title"]/text()')
	title=get_result(result)

	result=html.xpath('/html/body/div[@class="main-content w1240"]'+
			'/div[@id="article_content"]/div[@class="article-content-left"]/div[@class="article"]')
	if(len(result)!=1):
		print('len(result)=%d'%len(result))
		return empty_row
	else:
		article=result[0]
	line_list=article.xpath('./p//text()|./div/p//text()|./div/text()')
	content=collect_content(line_list)

	result=html.xpath('/html/head/meta[@name="keywords"]/@content')
	keywords=get_result(result)
	
	result=html.xpath('/html/head/meta[@name="tags"]/@content')
	tags=get_result(result)
	
	return dict(title=title,content=content,keywords=keywords,tags=tags)

def crawl_content_in_queue(q):
	'''
	爬取queue中url对应的content
	'''
	conn=get_db_conn()
	cursor=conn.cursor()
	while True:
		try:
			url=q.get(block=True,timeout=3)
		except Empty:
			#队列空，重试
			print('queue empty.retry...')
			continue 
		if url is None:
			#任务结束
			break
			print('done')
		else:
			print('url=%s'%url)
		page=get_page('http://'+url)
		row=parse_content(page)
		#存入数据库
		if row['title']!='' and row['content']!='':
			command='SELECT insert_page_content($PGDATA$%s$PGDATA$,'+\
			'$PGDATA$%s$PGDATA$,$PGDATA$%s$PGDATA$,$PGDATA$%s$PGDATA$,$PGDATA$%s$PGDATA$);'
			cursor.execute(command%(url,row['title'],row['content'],row['keywords'],row['tags']))
			is_duplicate=not cursor.fetchall()[0][0]
			if is_duplicate:
				print('已存在.url=%s'%url)
			conn.commit()
		else:
			print('empty.url=%s'%url)
	conn.close()
	pass 

def crawl_content_in_db():
	'''
	爬取db中的url对应的content
	'''
	conn=get_db_conn()
	cursor=conn.cursor()
	cursor.execute('SELECT * FROM url_to_be_get;')
	url_table=cursor.fetchall()
	q=Queue()
	for row in url_table:
		q.put(row[0])
	for i in range(THREAD_NUM):
		q.put(None)
	print('queue init done.')

	tlist=[threading.Thread(target=crawl_content_in_queue,args=(q,)) for i in range(THREAD_NUM)]
	print('start thread')
	for t in tlist:
		t.start()
	print('wait for thread')
	for t in tlist:
		t.join()
	pass

if __name__=='__main__':
	crawl_content_in_db()