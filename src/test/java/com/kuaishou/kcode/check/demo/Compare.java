package com.kuaishou.kcode.check.demo;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * <p></p>
 *
 * @author : chengyi
 * @date : 2020-07-12 15:15
 **/
public class Compare {
    public static void main(String[] args) throws IOException {
        // 1594171800000
        // String tm = "2020-07-08 09:30";
        // Date date = new Date();
        // SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        // try {
        //     Date parse = dateFormatter.parse(tm);
        //     System.out.println(parse.getTime());
        // } catch (ParseException e) {
        //     e.printStackTrace();
        // }
        // System.exit(0);
        Set<String> ansSet = new HashSet<>();
        String ansPath = "C:\\Users\\10042\\data\\kcode-second\\KcodeAlertAnalysis-data-small\\Q1Result-test.data";
        // String ansPath = "C:\\Users\\10042\\data\\kcode-second\\KcodeAlertAnalysis-data1\\data1\\Q1Result-1.txt";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(ansPath));
        String line = null;
        while ((line = bufferedReader.readLine()) != null) {
            ansSet.add(line);
        }

        String cmpPath = "D:\\test.data";
        BufferedReader br = new BufferedReader(new FileReader(cmpPath));
        Set<String> cmpSet = new HashSet<>();
        while ((line = br.readLine()) != null) {
           cmpSet.add(line);
        }
        int num = 0;
        for (String str : cmpSet) {
            if(!ansSet.contains(str)) {
                num += 1;
                System.out.println(str);
            }
        }
        System.out.println(num);
    }
}
