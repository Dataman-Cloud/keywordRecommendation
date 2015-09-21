package com.dataman.webservice;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONException;
import org.json.JsonHelper;

//import com.dataman.nlp.LDAService;
import com.dataman.webservice.io.ArticlID;
import com.dataman.webservice.io.InputMsg;
import com.dataman.webservice.io.MsgDAO;
import com.dataman.webservice.io.OutputMsg;
import com.dataman.webservice.io.DateSpan;
import com.dataman.webservice.util.Base64Util;

import org.apache.spark.deploy.SparkSubmit;
import com.dataman.nlp.predictArticle;
import com.dataman.nlp.TopicHot;
// @Path here defines class level path. Identifies the URI path that 
// a resource class will serve requests for.
@Path("artipredict")
public class ArtiPredictService {
 	
	
 // @GET here defines, this method will method will process HTTP GET
 // requests.
 @GET
 // @Path here defines method level path. Identifies the URI path that a
 // resource class method will serve requests for.
 @Path("/name/{i}")
 // @Produces here defines the media type(s) that the methods
 // of a resource class can produce.
 @Produces(MediaType.TEXT_XML)
 // @PathParam injects the value of URI parameter that defined in @Path
 // expression, into the method.
 public String userName(@PathParam("i") String i) {

  String name = i;
  return "<User>" + "<Name>" + name + "</Name>" + "</User>";
 }

 @POST 
 @Path("pred") 
 @Produces(MediaType.TEXT_PLAIN)
 public String predArt(@FormParam("msg") String jsonMsgInUTF8WithBase64) {
		
		// decode
		String jsonMsg = Base64Util
				.decodeBase64withUTF8(jsonMsgInUTF8WithBase64);
		
		// JSON TO BEAN
		InputMsg msg = new InputMsg();
		try {
			JsonHelper.toJavaBean(msg, jsonMsg);
		} catch (JSONException | ParseException e) {
			System.err.println(jsonMsg);
			e.printStackTrace();
		}

		OutputMsg oum = new OutputMsg();
		oum.setAppid(msg.getAppid());
		oum.setArticleid(msg.getArticleid());
		try{
			MsgDAO.inser(msg); //TODO 如果文章已经存在于数据库中，直接取数据库中的内容做预测还是返回错误？
          /*              String[] arg0=new String[]{
                            "--class","Knn",
                            "--conf","SPARK_CONF_DIR=/home/yma/spark-1.5.0-SNAPSHOT-bin-2.6.0/conf",
                            "/home/yma/knnJoin/target/scala-2.10/knn-assembly-1.0.jar"
                        };
                        SparkSubmit.main(arg0); 
	*/
               //         String[] re = Knn.getRecomm(4);
		        List<String> re = predictArticle.getArticle(Analyzer.sc, Analyzer.sqlContext, Analyzer.model, msg.getArticleid(), msg.getAppid(), msg.getContent(), Analyzer.analyzer);
	                String[] rel = re.get(0).split(":");
	                oum.setKeywords(rel[1]);
	                String[] articleIds = rel[0].split(",");
                        oum.setRelated_arts(rel[0]);
/*			List<ArticlID> related_arts = new ArrayList<ArticlID>();
                        System.out.println("articleId: " + msg.getArticleid());
			for(int i = 0; i < articleIds.length; i ++) {
                            System.out.println(articleIds[i]);
			    related_arts.add(new ArticlID(Integer.parseInt(articleIds[i]), msg.getAppid()));
			}
                        System.out.println(related_arts.size());
			oum.setRelated_arts(related_arts);
*/
		} catch (SQLException e) {
			oum.setDetail("发生异常：" + e.getMessage());
			e.printStackTrace();
		}
		return Base64Util.encodeUTF8String(JsonHelper.toJSON(oum).toString());
 }
 
 @POST 
 @Path("upload") 
 @Produces(MediaType.TEXT_PLAIN)
 public String uploadArt(@FormParam("msg") String jsonMsgInUTF8WithBase64) {
		
		// decode
		String jsonMsg = Base64Util
				.decodeBase64withUTF8(jsonMsgInUTF8WithBase64);
		
		// JSON TO BEAN
		InputMsg msg = new InputMsg();
		try {
			JsonHelper.toJavaBean(msg, jsonMsg);
		} catch (JSONException | ParseException e) {
			System.err.println(jsonMsg);
			e.printStackTrace();
		}

		OutputMsg oum = new OutputMsg();
		oum.setAppid(msg.getAppid());
		oum.setArticleid(msg.getArticleid());
		try{
			MsgDAO.inser(msg);
			oum.setDetail("1");
		} catch (SQLException e) {
			oum.setDetail("0:"+e.getMessage());
			e.printStackTrace();
		}
		return Base64Util.encodeUTF8String(JsonHelper.toJSON(oum).toString());
 }

 @POST 
 @Path("hot") 
 @Produces(MediaType.TEXT_PLAIN)
 public String topicHot(@FormParam("msg") String jsonMsgInUTF8WithBase64) {
        
        // decode
        String jsonMsg = Base64Util
                .decodeBase64withUTF8(jsonMsgInUTF8WithBase64);
        
        // JSON TO BEAN
        DateSpan msg = new DateSpan();
        try {
            JsonHelper.toJavaBean(msg, jsonMsg);
        } catch (JSONException | ParseException e) {
            System.err.println(jsonMsg);
            e.printStackTrace();
        }

        String re = TopicHot.getArticle(Analyzer.sc, Analyzer.sqlContext, msg.getAppid());
        OutputMsg oum = new OutputMsg();
        oum.setAppid(msg.getAppid());
        oum.setTopic_hot(re);
        return Base64Util.encodeUTF8String(JsonHelper.toJSON(oum).toString());
 }
}
