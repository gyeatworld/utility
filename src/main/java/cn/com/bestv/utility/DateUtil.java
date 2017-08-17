package cn.com.bestv.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @author zehui.zeng
 * @date 13-3-18 下午2:42
 */
public class DateUtil {
    public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd";
    public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /*** 日期转字符串 ***/
    public static String fomatDate(Date date,String format){
         DateFormat dateFormat = null;
         if(format == null || format.length() == 0){
             format = DEFAULT_DATE_PATTERN;
         }
         dateFormat = new SimpleDateFormat(format);
         return dateFormat.format(date);
    }

    /*** 字符串转日期 ***/
    public static Date parseDate(String date,String format) throws ParseException {
        DateFormat dateFormat = null;
        if(format == null || format.length() == 0){
            format = DEFAULT_DATE_PATTERN;
        }
        dateFormat = new SimpleDateFormat(format);
        return dateFormat.parse(date);
    }
    
    /*** 获取星期 ***/
   	public static String getWeekDay(Calendar c) {
   		String retStr = "";
   		if (c == null) {
   			return retStr;
   		}
   		switch (c.get(Calendar.DAY_OF_WEEK)) {
   		case Calendar.MONDAY:
   			retStr = "星期一";
   			break;
   		case Calendar.TUESDAY:
   			retStr = "星期二";
   			break;
   		case Calendar.WEDNESDAY:
   			retStr = "星期三";
   			break;
   		case Calendar.THURSDAY:
   			retStr = "星期四";
   			break;
   		case Calendar.FRIDAY:
   			retStr = "星期五";
   			break;
   		case Calendar.SATURDAY:
   			retStr = "星期六";
   			break;
   		case Calendar.SUNDAY:
   			retStr = "星期日";
   			break;
   		}
   		return retStr;
   	}
   	
   	//stime:'yyyy-MM-dd HH:mm:ss' duration:'HH:mm:ss' return:'yyyy-MM-dd HH:mm:ss'
   	public static String timePlus(String stime,String duration){
   		String retStr = "";
   		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
   		Long totalMs = 0L;
   		try {
   			totalMs = sdf.parse(stime).getTime()+convertStrToMs(duration);
   		} catch (ParseException e) {
   			e.printStackTrace();
   		}
   		retStr = sdf.format(new Date(totalMs));
   		return retStr;
   	}
   	
   	//时分秒转微秒
   	public static Long convertStrToMs(String str){
   		Long ms = 0L;
   		int hours = Integer.parseInt(str.split(":")[0]);
   		int minutes = Integer.parseInt(str.split(":")[1]);
   		int seconds = Integer.parseInt(str.split(":")[2]);
   		ms += hours*60*60*1000;
   		ms += minutes*60*1000;
   		ms += seconds*1000;
   		return ms;
   	}
   	
   	    
    public static Date dayStartTime(Date date){
    	Calendar calendar = Calendar.getInstance();
    	calendar.setTime(date);
    	calendar.set(Calendar.HOUR_OF_DAY, 0);
    	calendar.set(Calendar.MINUTE, 0);
    	calendar.set(Calendar.SECOND, 0);
    	return calendar.getTime();
    }
    
    public static Date dayEndTime(Date date){
    	Calendar calendar = Calendar.getInstance();
    	calendar.setTime(date);
    	calendar.set(Calendar.HOUR_OF_DAY, 23);
    	calendar.set(Calendar.MINUTE, 59);
    	calendar.set(Calendar.SECOND,59);
    	return calendar.getTime();
    }
    
    public static List<Date> days(Date startDate,Date endDate){

    	List<Date> days = new ArrayList<Date>();
    	
    	Calendar startCalendar = Calendar.getInstance();
    	startDate = dayStartTime(startDate);
    	startCalendar.setTime(startDate);
    	//days.add(startCalendar.getTime());

    	Calendar endCalendar = Calendar.getInstance();
    	endDate = dayStartTime(endDate);
    	endCalendar.setTime(endDate);
    	while(startCalendar.before(endCalendar) ){
    		days.add(startCalendar.getTime());
    		startCalendar.add(Calendar.DAY_OF_MONTH, 1);
    	}
    	
    	return days;
    }
    
    public static Date getDateBeforeOrAfter(Date curDate, int iDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(curDate);
        cal.add(Calendar.DAY_OF_MONTH, iDate);
        return cal.getTime();
    }
}
