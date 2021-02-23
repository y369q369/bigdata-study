package com.example.springbootApi.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class CommonUtil {

    // 获取文件的MD5值
    public static String getMD5(String str) {
        String value = null;
        try {
            byte[] tem = str.getBytes();
            MessageDigest md5 = MessageDigest.getInstance("md5");
            md5.reset();
            md5.update(tem);
            byte encrypt[] = md5.digest();
            StringBuilder sb = new StringBuilder();
            for (byte t : encrypt) {
                String s = Integer.toHexString(t & 0xFF);
                if (s.length() == 1) {
                    s = "0" + s;
                }
                sb.append(s);
            }
            value = sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return value;
    }

}
