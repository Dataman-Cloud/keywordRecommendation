package org.json;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * 1:��JavaBeanת����Map��JSONObject
 * 2:��Mapת����Javabean
 * 3:��JSONObjectת����Map��Javabean
 * 
 * @author Alexia
 */

public class JsonHelper {
    
    /**
     * ��Javabeanת��ΪMap
     * 
     * @param javaBean
     *            javaBean
     * @return Map����
     */
    public static Map<String,Object> toMap(Object javaBean) {

        Map<String,Object> result = new HashMap<String,Object>();
        Method[] methods = javaBean.getClass().getDeclaredMethods();

        for (Method method : methods) {

            try {

                if (method.getName().startsWith("get")) {

                    String field = method.getName();
                    field = field.substring(field.indexOf("get") + 3);
                    field = field.toLowerCase().charAt(0) + field.substring(1);

                    Object value = method.invoke(javaBean);
                    result.put(field, value);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return result;

    }

    /**
     * ��Json����ת����Map
     * 
     * @param jsonObject
     *            json����
     * @return Map����
     * @throws JSONException
     */
    public static Map<String,Object> toMap(String jsonString) throws JSONException {

        JSONObject jsonObject = new JSONObject(jsonString);
        
        return toMap(jsonObject);

    }

	public static Map<String, Object> toMap(JSONObject jsonObject) {
		Map<String,Object> result = new HashMap<String,Object>();
        Iterator<String> iterator = jsonObject.keys();
        String key = null;
        Object value = null;
        
        while (iterator.hasNext()) {

            key = iterator.next();
            value = jsonObject.get(key);
            if(value!=null){
            	result.put(key, value);
            }
        }
        return result;
	}

    /**
     * ��JavaBeanת����JSONObject��ͨ��Map��ת��
     * 
     * @param bean
     *            javaBean
     * @return json����
     */
    public static JSONObject toJSON(Object bean) {

        return new JSONObject(toMap(bean));

    }

    /**
     * ��Mapת����Javabean
     * 
     * @param javabean
     *            javaBean
     * @param data
     *            Map����
     */
    public static void toJavaBean(Object javabean, Map<String,Object> data) {

        Method[] methods = javabean.getClass().getDeclaredMethods();
        for (Method method : methods) {

            try {
                if (method.getName().startsWith("set")) {

                    String field = method.getName();
                    field = field.substring(field.indexOf("set") + 3);
                    field = field.toLowerCase().charAt(0) + field.substring(1);
                    Object value = data.get(field);
                    Class<?>[] cs = method.getParameterTypes();
                    if(value!=null){
                    	if(cs[0].getName().equals("java.lang.String")){
                    		method.invoke(javabean,value.toString());
                    	}else{
                    		method.invoke(javabean,value);
                    	}
                    }
                }
            } catch (Exception e) {
            	System.err.println(method.getName());
            	e.printStackTrace();
            }

        }

    }

    /**
     * JSONObject��JavaBean
     * 
     * @param bean
     *            javaBean
     * @return json����
     * @throws ParseException
     *             json�����쳣
     * @throws JSONException
     */
    public static void toJavaBean(Object javabean, String jsonString)
            throws ParseException, JSONException {

        JSONObject jsonObject = new JSONObject(jsonString);
    
        Map<String,Object> map = toMap(jsonObject);
        
        toJavaBean(javabean, map);

    }

}
