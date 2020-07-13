package com.kuaishou.kcode;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-07-14 00:14
 **/
public class Utils {
    public static long toFullMinute(long timestamp) {
        return timestamp / 60000 * 60000;
    }

    public static String decimalFormat(double number) {
        // 0.0就直接.0, 其他保留两位小数
        if (number != 0.0) {
            String str = String.valueOf(number);
            int dotPos = str.indexOf(".");
            // xx.0
            if (str.length() < dotPos + 3) return str + "0";
            return str.substring(0, dotPos + 3);
        }
        return ".0";
    }
}
