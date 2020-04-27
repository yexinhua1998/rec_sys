package com.sinward.rec_sys.OfflineCalculater;

import java.io.Serializable;
import java.lang.Math;

public class SimItem implements Serializable,Comparable<SimItem>{
	//相似度数据项的类，以javaBean的形式完成
	private int docid1;
	private int docid2;
	private double sim;
	SimItem(){}
	
	
	
	public int getDocid1() {
		return docid1;
	}



	public void setDocid1(int docid1) {
		this.docid1 = docid1;
	}



	public int getDocid2() {
		return docid2;
	}



	public void setDocid2(int docid2) {
		this.docid2 = docid2;
	}



	public double getSim() {
		return sim;
	}



	public void setSim(double sim) {
		this.sim = sim;
	}



	public int compareTo(SimItem o) {
		// TODO 比较相似度
		double x=this.getSim()-o.getSim();
		if(x>0) return 1;
		else if(x<0) return -1;
		else return 0;
	}
	
}
