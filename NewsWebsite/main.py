# -*- coding: utf-8 -*-
import web

urls=(
	'/','index',
	'/news_list','news_list',
	'/news','news'
)
db=web.database(dbn="postgres", host="10.130.26.200", port=5432, db="sina_page", user="postgres", pw="Ye25554160")
render=web.template.render('./templates',globals={'str':str})

class index:
	def GET(self):
		raise web.seeother('/news_list?page=1')

class news_list:
	def GET(self):
		page_size=100#一页装下的新闻标题数量
		jump_size=10#向前或向后跳转的新闻标题数量
		page=web.input(page=None).page 
		if page!=None and page.isdecimal() and int(page)!=0:
			page=int(page)
			results=db.select('page_content',what='docid,title',order='docid',limit=page_size,offset=(page-1)*page_size)
			jump_list=[i for i in range(page-jump_size,page+jump_size+1) if i>0]
			return render.news_list(results,page,jump_list)
		else:
			raise web.notfound('invalid page:'+str(page))

class news:
	def GET(self):
		docid=web.input(docid=None).docid
		if not docid or not docid.isdecimal():
			raise web.notfound('invalid docid:'+docid)
		else:
			myvars={'docid':docid}
			results=list(db.select('page_content',where='docid=$docid',what='docid,title,content',vars=myvars))
			if len(results)!=1:
				raise web.notfound(('have %d news with docid:'%len(results))+str(docid))
			else:
				item=results[0]
				recommended=db.select('recommended_title',what='docid2,title,sim',
					where='docid1=$docid',vars={'docid':item['docid']},order='sim DESC')
				return render.news(item['docid'],item['title'],item['content'],recommended)




app=web.application(urls,globals())

if __name__=='__main__':
	app.run()