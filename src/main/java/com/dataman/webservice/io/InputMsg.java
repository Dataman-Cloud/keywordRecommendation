package com.dataman.webservice.io;

public class InputMsg {
	private Integer articleid;
	
	private String title;
	
	private String subcontent;
	
	private String content;
	
	private Integer appid;
	
	private String keywords;


	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSubcontent() {
		return subcontent;
	}

	public void setSubcontent(String subcontent) {
		this.subcontent = subcontent;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}


	public String getKeywords() {
		return keywords;
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

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}
	
	public String toInsertSql(){
		StringBuilder sb = new StringBuilder("insert into art4test(");
		sb.append("`articleid`,").append("`title`,").append("`subcontent`,").append("`content`,").append("`appid`,").append("`keywords`")
		.append(") values(")
		.append("").append(this.getArticleid()).append(",")
		.append("'").append(this.getTitle()).append("',")
		.append("'").append(this.getSubcontent()).append("',")
		.append("'").append(this.getContent().replaceAll("'", "''")).append("',")
		.append("").append(this.getAppid()).append(",")
		.append("'").append(this.getKeywords()).append("')");
		return sb.toString();
	}
	
	public static void main(String args[]){
		InputMsg msg = new InputMsg();
		System.out.print(msg.toInsertSql());
	}
}
