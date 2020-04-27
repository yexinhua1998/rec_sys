package com.sinward.rec_sys.OfflineCalculater;
/*Java原生部分*/
import java.util.*;
import java.io.*;
import java.lang.Object;
import com.sinward.rec_sys.OfflineCalculater.Config;
/*ejml*/
import org.ejml.simple.SimpleMatrix;


public class WordVecPreProcesser {
	public static void main(String args[]) throws IOException,ClassNotFoundException{
		String VEC_PATH=Config.File.RAW_VEC_PATH;
		//VEC_PATH="/home/sinward/rec_sys/OfflineCalculater/testwordvec";//测试时用的目录
		//由于创建大对象会引起大量GC，导致性能变为原来的1%左右，因此采用两次遍历文件，避免采用ArrayList以节省内存
		//运行时应该设置java虚拟机的堆内存最大为6g
		int i,j,word_num,dim,colid;
		String word,line;
		long start=System.currentTimeMillis(),last,now;
		
		Scanner scanner=new Scanner(new FileInputStream(VEC_PATH));
		
		word_num=scanner.nextInt();
		dim=scanner.nextInt();
		
		System.out.println("word_num="+word_num);
		System.out.println("dim="+dim);
		Map<String,Integer> word2id=new HashMap<String,Integer>(word_num);
		scanner.nextLine();
		
		double []data=new double[dim];
		last=System.currentTimeMillis();
		for(i=0;i<word_num;i++) {
			line=scanner.nextLine();
			word=line.split(" ")[0];
			//System.out.println("word="+word);
			if(!word2id.containsKey(word)) {
				word2id.put(word,word2id.size());
			}
			//System.out.println("get word:"+word+" value="+word2id.get(word));
			if(i%1000==0) {
				now=System.currentTimeMillis();
				System.out.println("HashMap building:i="+i+" percentage="+(double)100*i/word_num+"%");
				System.out.println("done 1000 task in "+(double)(now-last)/1000+" seconds");
				System.out.println("total cost:"+(double)(now-start)/1000+" seconds");
				last=now;
			}
		}
		System.out.println("hash map build success!");
		
		last=System.currentTimeMillis();
		scanner=new Scanner(new FileInputStream(VEC_PATH));
		
		word_num=scanner.nextInt();
		dim=scanner.nextInt();
		scanner.nextLine();
		
		colid=0;
		
		SimpleMatrix matrix=new SimpleMatrix(word2id.size(),dim);
		
		for(i=0;i<word_num;i++) {
			word=scanner.next();
			//System.out.println("now word="+word);
			colid=word2id.get(word);
			if(matrix.get(colid, 0)!=0.0d) {
				scanner.nextLine();
				continue;
			}
			for(j=0;j<dim;j++) {
				matrix.set(colid,j,scanner.nextDouble());
			}
			if(i%1000==0) {
				now=System.currentTimeMillis();
				System.out.println("MatrixBuilding:i="+i+" percentage="+(double)100*i/word_num+"%");
				System.out.println("done 1000 task in "+(double)(now-last)/1000+" seconds");
				System.out.println("total cost:"+(double)(now-start)/1000+" seconds");
				last=now;
			}
		}
		
		
		//保存对象
		System.out.println("saving object");
		Object[] obj2save={word2id,matrix};
		String[] path= {Config.File.WORD2ID_PATH,
				Config.File.WORD_MATRIX_PATH};
		ObjectOutputStream oos=null;
		for(i=0;i<obj2save.length;i++) {
			oos=new ObjectOutputStream(new FileOutputStream(path[i]));
			oos.writeObject(obj2save[i]);
			oos.flush();
		}
	}
}
