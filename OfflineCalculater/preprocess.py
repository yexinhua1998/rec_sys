from pyspark.sql import SparkSession,Row
import numpy as np

WORD_VEC_PATH='./sgns.target.word-word.dynwin5.thr10.neg5.dim300.iter5'

def get_word_vec_bank():
	'''
	获取词向量和词库
	return:
	word2id:dict str=>int 表示词到词id的映射
	id2word:list int=>str 表示词id到词的映射
	vec_matrix:np.array(2,2) 表示由词向量拼接而成的矩阵
	'''
	vec_list=[]
	word2id={}
	id2word=[]
	with open(WORD_VEC_PATH,'r') as f:
		num,dim=list(map(int,f.readline().split(' ')))
		for i in range(num):
			data=f.readline().strip().split(' ')
			if len(data)!= 301:
				print(data)
				raise Exception('dimention error:len of list=%d,expected%d'%(len(data),301))
			word=data[0]
			vec_list.append(list(map(float,data[1:])))
			del data
			word2id[word]=i 
			id2word.append(word)
	vec_matrix=np.array(vec_list)
	return word2id,id2word,vec_matrix

def get_word_id_map(path):
	'''
	获取词到id的映射和id到词的映射
	path:str 词库文件路径
	return:word2id dict word(str)=>word_id(int)的映射
	return:id2word list(str) id2word[i]表示i号词
	'''
	with open(path,'r') as f:
		line=f.readline()
		num=int(line.split(' ')[0])
		print(num)
		word2id={}
		id2word=[]
		cnt=0
		for line in f:
			word=line.split(' ')[0]
			if not word in word2id:
				word2id[word]=cnt
				id2word.append(word)
				cnt+=1
	return word2id,id2word

def vec_file2matrix(path,word2id):
	'''
	将词向量文件转换为矩阵
	path(str):文件路径
	word2id(dict str=>int):词到词id的映射
	return matrix(numpy.array):由词向量组成的矩阵，矩阵第i行的行向量代表第i个词的向量
	'''
	with open(path,'r') as f:
		line=f.readline().strip()
		num,dim=list(map(int,line.split(' ')))
		matrix=np.zeros((len(word2id),dim))
		for i in range(num):
			items=f.readline().strip().split(' ')
			word=items[0]
			wid=word2id[word]
			for j in range(dim):
				matrix[wid,j]=float(items[j+1])
	return matrix

