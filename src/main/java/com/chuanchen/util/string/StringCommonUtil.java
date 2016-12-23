package com.chuanchen.util.string;

public class StringCommonUtil {
     public static boolean isBlank(String str){
    	 int strLen = 0;
    	 if(str == null || (strLen = str.length()) == 0){
    		 return true;
    	 }
    	 for(int i = 0;i < strLen;i++){
    		 if(Character.isWhitespace(str.charAt(i)) == false){
    			 return false;
    		 }
    	 }
    	 return true;
     }
     public static boolean isNotBlank(String str){
    	 int strLen = 0;
    	 if(str != null && (strLen = str.length()) > 0){
    		 for(int i = 0;i < strLen;i++){
    			 if(!Character.isWhitespace(str.charAt(i))){
    				 return true;
    			 }
    		 }
    	 }
    	 return false;
     }
}
