package com.sinward.rec_sys.OfflineCalculater;

import java.io.Serializable;
import java.util.List;

public class CuttedContent implements Serializable{
	private String url;
	private List<String> words;
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public List<String> getWords() {
		return words;
	}
	public void setWords(List<String> words) {
		this.words = words;
	}
	
}