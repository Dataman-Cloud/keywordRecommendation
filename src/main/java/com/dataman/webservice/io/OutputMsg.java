package com.dataman.webservice.io;

import java.util.List;

public class OutputMsg {
	
	private Integer articleid;
	
	private Integer appid;
	
	private String keywords; 
	
	private String related_arts;
	
	private String detail; //记录错误信息exp.
	
	public String getDetail() {
		return detail;
	}

	public void setDetail(String detail) {
		this.detail = detail;
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

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public String getRelated_arts() {
		return related_arts;
	}

	public void setRelated_arts(String related_arts) {
		this.related_arts = related_arts;
	}
}
