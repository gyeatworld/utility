package cn.com.bestv.utility;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * @author zehui.zeng
 * @date 13-3-17 下午5:00
 */
public class JSONUtil {
    private static Gson gson = new Gson();
    private static Gson gjson = null;// 带日期转换
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Date.class,new DateTypeAdapter());
        gsonBuilder.registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter());
        gjson = gsonBuilder.create();
    }
    /**
     * JSON字符串转换为Map对象(url转码)
     * @param jsonStr
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public static Map<String,Object> json2map(String jsonStr) throws UnsupportedEncodingException {
        Map<String,Object> jsonObject = (Map<String,Object>)gson.fromJson(jsonStr, Map.class);
        Iterator<?> it = jsonObject.keySet().iterator();
        Map<String,Object> result = new HashMap<String,Object>();
        String key = "";
        Object val = null;
        while (it.hasNext()){
            key = it.next().toString();
            val = jsonObject.get(key);
            if(val == null){
                result.put(key,"");
            }else {
                result.put(key, URLDecoder.decode(val.toString(),"UTF-8"));
            }
        }
        return result;
    }

    /***
     * JSON字符串转换为Map对象
     * @param jsonStr
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public static Map<String,Object> convertObject(String jsonStr) throws UnsupportedEncodingException {
        Map<String,Object> jsonObject = gson.fromJson(jsonStr, Map.class);
        return jsonObject;
    }

    /***
     * JSON字符串转换为List对象
     * @param jsonStr
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public static List<Map<String,Object>> convertList(String jsonStr) throws UnsupportedEncodingException {
        return gson.fromJson(jsonStr, List.class);
    }

    /***
     * JSON字符串转换为Map对象
     * @param jsonStr
     * @return
     * @throws java.io.UnsupportedEncodingException
     */
    public static String convertString(Object obj) throws UnsupportedEncodingException {
        return gjson.toJson(obj);
    }


}

class DateTypeAdapter implements JsonSerializer<Date>, JsonDeserializer<Date>{
    private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    public JsonElement serialize(Date ts, Type t, JsonSerializationContext jsc) {
        String dfString = format.format(new Date(ts.getTime()));
        return new JsonPrimitive(dfString);
    }
    public Date deserialize(JsonElement json, Type t, JsonDeserializationContext jsc) throws JsonParseException {
        if (!(json instanceof JsonPrimitive)) {
            throw new JsonParseException("The date should be a string value");
        }
        try{
            java.util.Date date = format.parse(json.getAsString());
            return new Date(date.getTime());
        } catch (ParseException e) {
            throw new JsonParseException(e);
        }
    }
}

 class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp>{
    private final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    //private final DateFormat formatYMD = new SimpleDateFormat("yyyy-MM-dd");
    public JsonElement serialize(Timestamp ts, Type t, JsonSerializationContext jsc) {
        String dfString = format.format(new Date(ts.getTime()));
        return new JsonPrimitive(dfString);
    }
    public Timestamp deserialize(JsonElement json, Type t, JsonDeserializationContext jsc) throws JsonParseException {
        if (!(json instanceof JsonPrimitive)) {
            throw new JsonParseException("The date should be a string value");
        }
        try{
            java.util.Date date = format.parse(json.getAsString());
            return new Timestamp(date.getTime());
        } catch (ParseException e) {
            throw new JsonParseException(e);
        }
    }
}


