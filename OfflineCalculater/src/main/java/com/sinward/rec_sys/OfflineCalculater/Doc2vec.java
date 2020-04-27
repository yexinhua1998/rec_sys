package com.sinward.rec_sys.OfflineCalculater;
/*java自带*/
import java.util.*;
import java.io.*;
import java.lang.Math;
/*Spark RDD模块*/
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.Function2;
/*spark SQL模块*/
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.SaveMode;
/*结巴分词*/
import com.huaban.analysis.jieba.JiebaSegmenter;
/*矩阵*/
import org.ejml.simple.SimpleMatrix;
/*项目内的类*/
import com.sinward.rec_sys.OfflineCalculater.*;

class Cut implements MapPartitionsFunction<Row,CuttedContent>{
	Cut(){}
	public Iterator<CuttedContent> call(Iterator<Row> input){
		JiebaSegmenter segmenter=new JiebaSegmenter();
		ArrayList<CuttedContent> results=new ArrayList<CuttedContent>();
		Row row;
		String url,content;
		List<String> words=null;
		CuttedContent cutted=null;
		while(input.hasNext()) {
			row=input.next();
			url=row.<String>getAs("url");
			content=row.<String>getAs("content");
			words=segmenter.sentenceProcess(content);
			cutted=new CuttedContent();
			cutted.setUrl(url);
			cutted.setWords(words);
			results.add(cutted);
		}
		return results.iterator();
	}
}

class Cutted2BagOfWord implements MapPartitionsFunction<CuttedContent,BagOfWord>{
	public Broadcast< HashMap<String,Integer> > bc_word2id;
	Cutted2BagOfWord(Broadcast< HashMap<String,Integer> > word2id){this.bc_word2id=word2id;}
	public Iterator<BagOfWord> call(Iterator<CuttedContent> input_it) throws Exception{
		
		BagOfWord bow=null;
		CuttedContent cutted=null;
		String url=null;
		List<String> words=null;
		int wordid,i;
		List<BagOfWord> result=new ArrayList<BagOfWord>();
		HashMap<String,Integer> word2id=bc_word2id.value();
		int[] values=null;
		
		while(input_it.hasNext()) {
			
			cutted=input_it.next();
			url=cutted.getUrl();
			words=cutted.getWords();

			bow=new BagOfWord();
			bow.setUrl(url);
			
			values=new int[word2id.size()];
			
			for(String word:words) {
				if(word2id.containsKey(word)) {
					wordid=word2id.get(word);
					values[wordid]++;
				}
			}
			bow.setTf(values);
			result.add(bow);
		}
		/*for(BagOfWord t:result) {
			System.out.println(t.getTf());
		}*/
		
		return result.iterator();
	}
}

class DF_plus_TF implements Function2<int[],BagOfWord,int[]>{

	public int[] call(int[] df, BagOfWord bow) throws Exception {
		//将DF向量和某个文章的bow向量合并，产生新的DF向量
		int i;
		int[] tf=bow.getTf();
		for(i=0;i<df.length;i++) {
			if(tf[i]>0) df[i]++;
		}
		return df;
	}
}

class DF_plus_DF implements Function2<int[],int[],int[]>{

	public int[] call(int[] v1, int[] v2) throws Exception {
		//将DF向量和另外的DF向量合并，产生新的DF向量
		int i;
		for(i=0;i<v1.length;i++) v1[i]+=v2[i];
		return v1;
	}
	
	
}

class Bow2TF_IDF implements MapPartitionsFunction<BagOfWord,TF_IDF>{
	private Broadcast<double[]> bc_idf;
	public Bow2TF_IDF(Broadcast<double[]> idf) {
		this.bc_idf=idf;
	}
	public Iterator<TF_IDF> call(Iterator<BagOfWord> input) throws Exception {
		// TODO 将Bow映射成TF_IDF
		List<TF_IDF> results=new ArrayList<TF_IDF>();
		BagOfWord bow=null;
		TF_IDF tfidf=null;
		double[] tfidf_vec=null,idf=bc_idf.value();
		int[] bow_vec=null;
		String url;
		int i;
		
		while(input.hasNext()) {
			bow=input.next();
			url=bow.getUrl();
			bow_vec=bow.getTf();
			tfidf_vec=new double[bow_vec.length];
			for(i=0;i<bow_vec.length;i++) {
				tfidf_vec[i]=idf[i]*bow_vec[i];
			}
			tfidf=new TF_IDF();
			tfidf.setUrl(url);
			tfidf.setData(tfidf_vec);
			results.add(tfidf);
		}
		return results.iterator();
	}
	
}

class TfIdf2vec implements MapPartitionsFunction<TF_IDF,DocVec>{
	Broadcast<SimpleMatrix> bc_wordmatrix;
	int word_num;
	public TfIdf2vec(Broadcast<SimpleMatrix> wordMatrix,int _word_num) {
		this.bc_wordmatrix=wordMatrix;
		this.word_num=_word_num;
	}
	
	public Iterator<DocVec> call(Iterator<TF_IDF> input) throws Exception {
		// TODO 将TF_IDF映射到DocVec
		DocVec docvec=null;
		TF_IDF tfidf=null;
		String url;
		double[] tfIdfVec=null,data=null;
		SimpleMatrix m1=new SimpleMatrix(1,word_num),wordMatrix=bc_wordmatrix.value(),result=null;
		int i;
		List<DocVec> resultList=new ArrayList<DocVec>();
		
		while(input.hasNext()) {
			tfidf=input.next();
			url=tfidf.getUrl();
			tfIdfVec=tfidf.getData();
			for(i=0;i<tfIdfVec.length;i++) m1.set(0,i,tfIdfVec[i]);
			result=m1.mult(wordMatrix);
			data=new double[Config.WORD_VEC_DIM];
			for(i=0;i<Config.WORD_VEC_DIM;i++) data[i]=result.get(0, i);
			docvec=new DocVec();
			docvec.setUrl(url);
			docvec.setVec(data);
			resultList.add(docvec);
		}
		return resultList.iterator();
	}
	
}

public class Doc2vec {
	
	public static void main(String[] args) throws IOException,ClassNotFoundException{
		int i;
		
    	SparkSession spark = SparkSession
  			  .builder()
  			  .master("local[4]")
  			  .appName("OfflineCalculater")
  			  .config("spark.driver.maxResultSize","0")
  			  .getOrCreate();
    	SparkContext sc=spark.sparkContext();
    	JavaSparkContext jsc=new JavaSparkContext(sc);
    	
	  	Dataset<Row> page_content = spark.read()
	  			  .format("jdbc")
	  			  .option("url", Config.DB.JDBC_URL)
	  			  .option("user", Config.DB.USER)
	  			  .option("password", Config.DB.PASSWORD)
	  			  .option("dbtable", "(SELECT url,content,docid FROM page_content) AS temptable")
	  			  .option("numPartitions",Config.DataSource.NUM_OF_PARTITIONS)
	  			  .option("partitionColumn", "docid")
	  			  .option("lowerBound", Config.DataSource.LOWER_BOUND)
	  			  .option("upperBound",Config.DataSource.UPPER_BOUND)
	  			  .load();
	  	
	  	Encoder<CuttedContent> cuttedEncoder=Encoders.bean(CuttedContent.class);
	  	Dataset<CuttedContent> cutted=page_content.mapPartitions(new Cut(), cuttedEncoder);
	  	//载入word2id并广播
	  	HashMap<String,Integer> local_word2id=(HashMap<String, Integer>) loadObject(Config.File.WORD2ID_PATH);
	  	Broadcast< HashMap<String,Integer> > bc_word2id=jsc.broadcast(local_word2id);
	  	
	  	//转换成为bow
	  	Encoder<BagOfWord> bowEncoder=Encoders.bean(BagOfWord.class);
	  	Dataset<BagOfWord> bow=cutted.mapPartitions(new Cutted2BagOfWord(bc_word2id), bowEncoder);
	  	
	  	//计算df(Document Freqence)和IDF
	  	int word_num=local_word2id.size();
	  	int[] df=bow.javaRDD().aggregate(new int[word_num],new DF_plus_TF(), new DF_plus_DF());
	  	System.out.println("df.length="+df.length);
	  	
	  	//计算IDF
	  	int doc_num=Config.DOC_NUM;
	  	double[] idf=new double[df.length];
	  	for(i=0;i<df.length;i++) {
	  		idf[i]=Math.log((double)(doc_num+1)/(df[i]+1));
	  	}
	  	System.out.println("idf.length="+idf.length);
	  	
	  	//计算TF_IDF
	  	Broadcast<double[]> bc_idf=jsc.broadcast(idf);
	  	Encoder<TF_IDF> tfidfEncoder=Encoders.bean(TF_IDF.class);
	  	Dataset<TF_IDF> tfidf=bow.mapPartitions(new Bow2TF_IDF(bc_idf), tfidfEncoder);
	  	
	  	//计算TF_IDF_weighted word2vec
	  	Broadcast<SimpleMatrix> bc_wordmatrix=jsc.broadcast((SimpleMatrix)loadObject(Config.File.WORD_MATRIX_PATH));
	  	Encoder<DocVec> docVecEncoder=Encoders.bean(DocVec.class);
	  	Dataset<DocVec> docVecDataSet=tfidf.mapPartitions(new TfIdf2vec(bc_wordmatrix,word_num),docVecEncoder);
	  	
	  	//保存到数据库
	  	docVecDataSet.write().format("jdbc").mode(SaveMode.Overwrite)
	  	.option("url",Config.DB.JDBC_URL)
	  	.option("user",Config.DB.USER)
	  	.option("password",Config.DB.PASSWORD)
	  	.option("dbtable", "doc_vec")
	  	.save();
	  	
	}
	public static Object loadObject(String path) throws IOException,ClassNotFoundException{
		ObjectInputStream ois=new ObjectInputStream(new FileInputStream(path));
		return ois.readObject();
	}
}
