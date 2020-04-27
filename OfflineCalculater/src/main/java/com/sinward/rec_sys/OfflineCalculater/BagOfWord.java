package com.sinward.rec_sys.OfflineCalculater;
import java.io.*;

public class BagOfWord implements Serializable{
	private String url;
	private int[] tf;
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int[] getTf() {
		return tf;
	}
	public void setTf(int[] tf) {
		this.tf = tf;
	}
	
}
