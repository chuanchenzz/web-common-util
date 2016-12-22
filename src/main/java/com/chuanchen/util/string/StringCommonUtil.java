package com.chuanchen.util.string;

public class StringCommonUtil {
     public static boolean isBlank(String str){
    	 return (str == null || "".equals(str));
     }
     public static boolean isNotBlank(String str){
    	 return (str != null && !"".equals("str"));
     }
}
