package com.dataman.webservice.io;

public class ArticlID {
	
	private Integer articleid;
	private Integer appid;
	
	public ArticlID(int articleid,int appid){
		this.articleid = articleid;
		this.appid = appid;
	}
	
	public Integer getArticleid() {
		return articleid;
	}
	public void setArticleid(Integer articleid) {
		this.articleid = articleid;
	}
	public Integer getAppid() {
		return appid;
	}
	public void setAppid(Integer appid) {
		this.appid = appid;
	}
	
}
