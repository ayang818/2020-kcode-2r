package com.kuaishou.kcode.check.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-07-13 11:45
 **/
public class DateParserTest {
    public static void main(String[] args) throws ParseException {
        DateTimeFormatter threadSafeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
        SimpleDateFormat normalSafeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String a = "2020-06-18 12:33";
        int thres = 100000;
        ThreadLocal<SimpleDateFormat> formatUtil = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm"));

        long start = System.currentTimeMillis();
        int i = thres;
        while (i >= 0) {
            threadSafeFormatter.parse(a);
            i--;
        }
        System.out.println(String.format("cost %d ms", System.currentTimeMillis() - start));

        // faster!
        start = System.currentTimeMillis();
        i = thres;
        while (i >= 0) {
            formatUtil.get().parse(a);
            i--;
        }
        System.out.println(String.format("cost %d ms", System.currentTimeMillis() - start));
    }

}
