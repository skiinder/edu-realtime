//package com.atguigu.edu.realtime.test;
//
//import java.io.*;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.net.URLEncoder;
//
///**
// * description:
// * Created by 铁盾 on 2022/7/22
// */
//public class TestClass {
//    public static String getHttpUrl(String text) throws IOException {
//        String jsonstr = null;
//        BufferedReader in = null;
//        StringBuilder inputString = new StringBuilder();
//
//        try {
//            String param = URLEncoder.encode(text, "UTF-8");
//            URL url = new URL(HttpConstant.HTTP_MATCH_KEYWORD_PRE + param);
//            //调用url的openConnection()方法,获得连接对象
//            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//
//            //设置HttpURLConnection的属性
//            conn.setRequestMethod(HttpConstant.HTTP_REQUEST_METHOD_GET);
//            conn.setReadTimeout(5000);
//            conn.setConnectTimeout(5000);
//
//            //通过响应码来判断是否连接成功
//            if (conn.getResponseCode() == 200) {
//                //获得服务器返回的字节流
//                InputStream is = conn.getInputStream();
//                in = new BufferedReader(new InputStreamReader(is));
//                String inputLine;
//                while ((inputLine = in.readLine()) != null) {
//                    inputString.append(inputLine);
//                }
//                is.close();
//                jsonstr = inputString.toString();
//                return jsonstr;
//            }
//}
