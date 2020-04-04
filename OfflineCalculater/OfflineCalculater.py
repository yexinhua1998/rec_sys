from pyspark.sql import SparkSession,Row
from pyspark.ml.feature import IDF
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import IDF
from pyspark import StorageLevel
import psycopg2
import numpy as np
import pandas as pd 
import jieba
import gc

class Const:
	APP_NAME='OfflineCalculater'
	DB_CONN_ARG={
		'database':'sina_page',
		'user':'postgres',
		'password':'Ye25554160',
		'host':'127.0.0.1',
		'port':'5432'
	}
	DOC_FILE_PATH='./data.xlsx'
	WORD2ID_PATH='./word2id.txt'
	ID2WORD_PATH='./id2word.txt'
	WORD_MATRIX_PATH='./word_matrix.npy'
	WORD_NUM=635963



def get_doc_from_file():
	'''
	从文件中获取文档信息
	return list of (url,content)。url(str)为这个内容的url，content为该url对应的内容
	'''
	data=pd.read_excel(Const.DOC_FILE_PATH)
	return [(data['url'].iloc[i],data['content'].iloc[i]) for i in range(len(data))]

def cutted_doc2bag_of_word(element):
	'''
	将已经分词的doc转换成bag_of_word向量
	element:(url,cutted_content)
	url(str)，cutted_content对应的url
	cutted_content(list of str):对应已经分词的content
	return (url,vector)，即url和对应的bag_of_word向量
	'''
	url,cutted_content=element
	vec=np.zeros(len(word2id.value))
	for word in cutted_content:
		wordid=word2id.value.get(word)
		if not wordid is None:
			vec[wordid]+=1
			#print('word=%s,wordid=%d,vec=%d'%(word,wordid,int(vec[wordid])))
	return (url,DenseVector(vec))


def tf_idf_and_vec2product(x):
	'''
	将(Row(url,tf_idf),(col_id,col_vec))映射成为(url,(col_id,product))
	'''
	row,id_vec=x
	col_id,vec=id_vec 
	url=row['url']
	tf_idf=row['tf_idf'] 
	return (url,(col_id,tf_idf.dot(vec)))

def aggregate_product_seq(x,y):
	'''
	聚集product中的seqOp
	将(vec(WORD_NUM),(col_id,product))映射成为vec(WORD_NUM)
	'''
	z=x.copy()
	col_id,product=y 
	z[col_id]=product
	return z


def get_doc_from_db():
	'''
	从db中获取文档数据
	return doc_iter(iterator) 可以db中文档的迭代器
	'''
	conn=psycopg2.connect(**Const.DB_CONN_ARG)
	cursor=conn.cursor()
	cursor.execute('SELECT url,content FROM page_content;')
	data=cursor.fetchall()
	conn.close()
	del cursor
	del conn
	return data

def get_doc_from_db_by_gen(buf_size=10000):
	'''
	从db中获取文档数据，使用生成器以减少内存损耗
	return doc_iter(iterator) 可以db中文档的迭代器
	'''
	conn=psycopg2.connect(**Const.DB_CONN_ARG)
	cursor=conn.cursor()
	cnt=0
	while True:
		cursor.execute('SELECT url,content FROM page_content LIMIT %d OFFSET %d;'%(buf_size,cnt))
		data=cursor.fetchall()
		if data==[]:
			break
		else:
			cnt+=buf_size
			for item in data:
				yield item 
			print('loaded %d'%cnt)
			del data
			gc.collect()
	pass

def put_url_vec2db(item):
	'''
	将(url,vec)推到数据库
	item:url,vec组成的元组
	'''
	conn=psycopg2.connect(**Const.DB_CONN_ARG)
	cursor=conn.cursor()
	url,vec=item
	cursor.execute('INSERT INTO doc_vec VALUES(%s,%s);'%('$URL$'+url+'$URL$',
		'\'{'+(','.join(map(str,vec)))+'}\''))
	conn.commit()
	conn.close()


def put_vec_rdd2db(rdd):
	'''
	将计算完成的向量推到数据库
	rdd:pyspark.RDD对象 为(url,vec)组成的RDD url(str) vec(DenseVector(300))
	'''
	for item in rdd.collect():
		put_url_vec2db(item)
	pass


#配置函数接口
get_doc=get_doc_from_db_by_gen
put_doc=put_vec_rdd2db

if __name__=='__main__':
	spark=SparkSession.builder.appName(Const.APP_NAME)\
	.config('spark.default.parallelism',10000)\
	.master('local[1]')\
	.config('spark.driver.memory','4g')\
	.getOrCreate()
	sc=spark.sparkContext

	#加载数据
	print('\n\n\nloading data\n\n\n')
	doc=get_doc()#(url,content)
	doc_rdd=sc.parallelize(doc)
	del doc

	#分词
	print('\n\n\ncutting word\n\n\n')
	cutted_doc_rdd=doc_rdd.map(lambda x:(x[0],list(jieba.cut(x[1]))))#(url,cutted_content(list))
	cutted_doc_rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
	#doc_rdd.unpersist()

	#转换成bag of word
	print('\n\n\ntransforming to bag of word\n\n\n')
	with open(Const.WORD2ID_PATH,'r',encoding='utf-8') as f:
		word2id=sc.broadcast(eval(f.read()))

	bagofword_rdd=cutted_doc_rdd.map(cutted_doc2bag_of_word)#=>(url,vector)
	#bagofword_rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
	#cutted_doc_rdd.unpersist()
	#word2id.destroy()

	#计算tf_idf
	print('\n\n\ncalculating tf_idf\n\n\n')
	tf_df=spark.createDataFrame(bagofword_rdd,['url','tf'])
	idf=IDF(inputCol='tf',outputCol='tf_idf')
	model=idf.fit(tf_df)
	tf_idf_df=model.transform(tf_df)
	#tf_idf_df.rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
	#tf_df.rdd.unpersist()
	#bagofword_rdd.unpersist()

	print('\n\n\ncalculating document vector\n\n\n')
	#计算文章向量
	'''
	np_matrix=np.load(Const.WORD_MATRIX_PATH).T
	vec_list=[(i,np_matrix[i]) for i in range(np_matrix.shape[0])]
	vec_rdd=sc.parallelize(vec_list,12)
	product_rdd=tf_idf_df.rdd.cartesian(vec_rdd).map(tf_idf_and_vec2product).\
	aggregateByKey(np.zeros(Const.WORD_NUM),aggregate_product_seq,lambda x,y:x+y,12)
	'''
	np_matrix=np.load(Const.WORD_MATRIX_PATH)
	matrix=sc.broadcast(np_matrix)
	del np_matrix
	product_rdd=tf_idf_df.rdd.map(lambda row:(row['url'],DenseVector(row['tf_idf'].dot(matrix.value))))
	#product_rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
	#tf_idf_df.rdd.unpersist()
	#matrix.destroy()


	#输出
	print('\n\n\noutputing...\n\n\n')
	#str_product_rdd=product_rdd.map(lambda x:Row(url=x[0],vec=str(x[1])))
	#pdf=spark.createDataFrame(str_product_rdd).toPandas().to_excel('vec.xlsx')
	put_doc(product_rdd)