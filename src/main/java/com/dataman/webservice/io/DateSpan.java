package com.dataman.webservice.io;

import java.util.Date;

public class DateSpan {
    private Date startDate;
    
    private Date stopDate;
    
    private Integer appid;


    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getStopDate() {
        return stopDate;
    }

    public void setStopDate(Date stopDate) {
        this.stopDate = stopDate;
    }
    
    public Integer getAppid() {
        return appid;
    }

    public void setAppid(Integer appid) {
        this.appid = appid;
    }
    
    public static void main(String args[]){
        DateSpan msg = new DateSpan();
    }

}

