package com.kuaishou.kcode.check.demo;

import static com.kuaishou.kcode.check.demo.Utils.createQ1CheckResult;
import static com.kuaishou.kcode.check.demo.Utils.createQ2Result;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.kuaishou.kcode.KcodeAlertAnalysis;
import com.kuaishou.kcode.KcodeAlertAnalysisImpl;

/**
 * @author KCODE
 * Created on 2020-07-01
 */
public class KcodeAlertAnalysisTest {

    public static void main(String[] args) throws Exception {
        // 第一套数据集
        //kcodeAlertForStudent-1.data，原始监控数据
        String sourceFilePath1 = "";
        // ruleForStudent-1，报警规则
        String ruleFilePath1 = "";
        // Q1Result-1.txt，第一问结果
        String q1ResultFilePath1 = "";
        // Q2Result-1.txt，第二问输出和结果
        String q2ResultFilePath1 = "";

        // 第二套数据集
        //kcodeAlertForStudent-2.data，原始监控数据
        String sourceFilePath2 = "";
        // ruleForStudent-2，报警规则
        String ruleFilePath2 = "";
        // Q1Result-2.txt，第一问结果
        String q1ResultFilePath2 = "";
        // Q2Result-2.txt，第二问输出和结果
        String q2ResultFilePath2 = "";

        testQuestion12(sourceFilePath1, ruleFilePath1, q1ResultFilePath1, q2ResultFilePath1);
        testQuestion12(sourceFilePath2, ruleFilePath2, q1ResultFilePath2, q2ResultFilePath2);

    }

    public static void testQuestion12(String sourceFilePath, String ruleFilePath, String q1ResultFilePath, String q2ResultFilePath) throws Exception {
        // Q1
        Set<Q1Result> q1CheckResult = createQ1CheckResult(q1ResultFilePath);
        KcodeAlertAnalysis instance = new KcodeAlertAnalysisImpl();
        List<String> alertRules = Files.lines(Paths.get(ruleFilePath)).collect(Collectors.toList());
        long start = System.nanoTime();
        Collection<String> alertResult = instance.alarmMonitor(sourceFilePath, alertRules);
        long finish = System.nanoTime();
        if (Objects.isNull(alertResult) || alertResult.size() != q1CheckResult.size()) {
            System.out.println("Q1 Error Size:" + q1CheckResult + "," + alertResult.size());
            return;
        }
        Set<Q1Result> resultSet = alertResult.stream().map(line -> new Q1Result(line)).collect(Collectors.toSet());
        if (!resultSet.containsAll(q1CheckResult)) {
            System.out.println("Q1 Error Value");
            return;
        }
        System.out.println("Q1:" + (finish - start));

        // Q2
        Map<Q2Input, Set<Q2Result>> q2Result = createQ2Result(q2ResultFilePath);
        long cast = 0L;
        for (Map.Entry<Q2Input, Set<Q2Result>> entry : q2Result.entrySet()) {
            start = System.nanoTime();
            Q2Input q2Input = entry.getKey();
            Collection<String> longestPaths = instance.getLongestPath(q2Input.getCaller(), q2Input.getResponder(), q2Input.getTime(), q2Input.getType());
            finish = System.nanoTime();
            Set<Q2Result> checkResult = entry.getValue();

            if (Objects.isNull(longestPaths) || longestPaths.size() != checkResult.size()) {
                System.out.println("Q2 Error Size:" + q2Input + "," + checkResult.size() + longestPaths.size());
                return;
            }
            Set<Q2Result> results = longestPaths.stream().map(line -> new Q2Result(line)).collect(Collectors.toSet());
            if (!results.containsAll(checkResult)) {
                System.out.println("Q2 Error Result:" + q2Input);
                return;
            }
            cast += (finish - start);
        }
        System.out.println("Q2:" + (finish - start));
    }
}