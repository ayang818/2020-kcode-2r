package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chengyi0818
 * Created on 2020-07-10
 */
public class KcodeAlertAnalysisImpl implements KcodeAlertAnalysis {
    /*
    数据对象
    callerService+callerIp+responderService+responderIp -> Map(timestamp -> Span)
    */
    Map<String, Map<Long, Span>> dataMap = new ConcurrentHashMap<>();
    /* 数据处理线程池 */
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(16, 16, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1000));
    /* global date formatter */
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    /* 一个线程池中的任务多少行 */
    int taskNumberThreshold = 4000;
    /* 每分钟的毫秒跨度 */
    int millspace = 60000;

    @Override
    public Collection<String> alarmMonitor(String path, Collection<String> alertRules) {
        long start = System.currentTimeMillis();
        System.out.println("开始读取日志......");
        BufferedReader bufferedReader = null;
        String line;
        try {
            bufferedReader = new BufferedReader(new FileReader(path));
            int size = 0;
            String[] lines = new String[taskNumberThreshold];
            while ((line = bufferedReader.readLine()) != null) {
                lines[size] = line;
                size += 1;
                if (size >= taskNumberThreshold) {
                    String[] tmpLines = lines;
                    threadPool.execute(() -> handleLines(tmpLines, taskNumberThreshold));
                    lines = new String[taskNumberThreshold];
                    size = 0;
                }
            }
            final String[] tmpLines = lines;
            final int sz = size;
            threadPool.execute(() -> handleLines(tmpLines, sz));
        } catch (IOException e) {
            e.printStackTrace();
        }
        /* 等待线程池任务处理完 */
        while (threadPool.getQueue().size() > 0 && threadPool.getActiveCount() > 0) {};
        System.out.println(String.format("数据解析完毕，耗时 %d ms，开始生成报警信息....", System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        List<Rule> ruleList = parseRules(alertRules);
        Set<String> res = getAlertInfo(ruleList);
        System.out.println(String.format("报警信息生成完毕，共 %d 条，耗时 %d ms", res.size(), System.currentTimeMillis() - start));
        return res;
    }

    private Set<String> getAlertInfo(List<Rule> ruleList) {
        System.out.println(String.format("规则集中有 %d 条规则", ruleList.size()));
        int ruleSize = ruleList.size();
        Set<String> res = new HashSet<>(2000);
        // servicePair+ipPair -> type[](p99 或 SR) -> Map<Long, AlertRecord>
        Map<String, Map<Integer, Map<Long, AlertRecord>>[]> collectMap = new HashMap<>(200);
        /*
        * TODO 有一小部分精度误差 p99 和 SR 都有一小部分有精度误差
        */
        // 遍历所有的servicePair+ipPair
        dataMap.forEach((key, longSpanMap) -> {
            // serviceA, 172.17.60.2, serviceB, 172.17.60.3
            String[] e = key.split(",");
            // 遍历所有的时间戳 + span
            longSpanMap.forEach((timestamp, span) -> {
                // 遍历所有规则集
                for (int i = 0; i < ruleSize; i++) {
                    Rule rule = ruleList.get(i);
                    // 将可能需要警告的筛选出来
                    boolean possibleAlert = check(rule, e, timestamp, span);
                    if (possibleAlert) {
                        // 将信息填充入collectMap
                        Map<Integer, Map<Long, AlertRecord>>[] typeArray = collectMap.computeIfAbsent(key, (v) -> new Map[2]);
                        Map<Integer, Map<Long, AlertRecord>> ruleIdMap = null;
                        if ((ruleIdMap = typeArray[rule.getType()]) == null) {
                            ruleIdMap = new HashMap<>();
                            typeArray[rule.getType()] = ruleIdMap;
                        }
                        // 按照 ruleId 进行分组
                        Map<Long, AlertRecord> timestampAlertRecordMap = ruleIdMap.computeIfAbsent(rule.getId(), (v) -> new HashMap<>());
                        if (rule.getType() == 0) {
                            timestampAlertRecordMap.computeIfAbsent(timestamp, (v) -> new AlertRecord(rule.getId(), span.getP99() + "ms", rule.getTriggerMinutes()));
                        } else if (rule.getType() == 1) {
                            timestampAlertRecordMap.computeIfAbsent(timestamp, (v) -> new AlertRecord(rule.getId(), decimalFormat(span.getSucRate()) + "%", rule.getTriggerMinutes()));
                        }
                    }
                }
            });
        });
        // 满足的规则，format后的时间，主调服务，主调ip，被调服务，被调ip，报警值（p99或SR）
        // 遍历 collectMap，得到最终答案
        collectMap.forEach((key, typeArray) -> {
            for (int i = 0; i <= 1; i++) {
                Map<Integer, Map<Long, AlertRecord>> ruleIdMap = typeArray[i];
                // 若为空，说明没有对应的rule，或没有满足的数据
                if (ruleIdMap == null) continue;
                ruleIdMap.forEach((ruleId, timestampAlertRecordMap) -> {
                    // 这里可以保证所有的疑似错误记录都在同一个ruleId分组下
                    timestampAlertRecordMap.forEach((timestamp, alertRecord) -> {
                        int triggerMinute = alertRecord.getTriggerMinute();
                        int left = 0, right = 0;
                        // TODO remove after found
                        // 向前找 triggerMinute 分钟
                        while (left < triggerMinute) {
                            if (timestampAlertRecordMap.get(timestamp - (left + 1) * millspace) != null) {
                                left++;
                            } else {
                                break;
                            }
                        }
                        // 向后找 triggerMinute 分钟
                        while (right < triggerMinute) {
                            if (timestampAlertRecordMap.get(timestamp + (right + 1) * millspace) != null) {
                                right++;
                            } else {
                                break;
                            }
                        }
                        // TODO 成功率有问题
                        // 总共连续 n 分钟，若 >= triggerMinute 分钟，从起始分钟开始，向后构造 record
                        if (left + right + 1 >= triggerMinute) {
                            StringBuilder record = new StringBuilder();
                            long end = timestamp + (right * millspace);
                            int num = 0;
                            for (long start = timestamp - (left * millspace); start <= end; start += millspace) {
                                num += 1;
                                if (num < triggerMinute) continue;
                                AlertRecord ar = timestampAlertRecordMap.get(start);
                                record.append(ar.getRuleId())
                                        .append(",")
                                        .append(parseDate(start))
                                        .append(",")
                                        .append(key)
                                        .append(",")
                                        .append(ar.getAlertVal());
                                res.add(record.toString());
                                record.delete(0, record.length());
                            }
                        }
                    });
                });
            }
        });
        StringBuilder sb = new StringBuilder();
        res.forEach((str) -> sb.append(str).append("\n"));
        try {
            Files.write(Paths.get("D:/test.data"), sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    private String parseDate(long timestamp) {
        return dateFormatter.format(timestamp);
    }

    private boolean check(Rule rule, String[] e, Long timestamp, Span span) {
        // 判断主调
        if (rule.getCallerService().equals(e[0]) || "ALL".equals(rule.getCallerService())) {
            // 判断被调
            if (rule.getResponderService().equals(e[2]) || "ALL".equals(rule.getResponderService())) {
                // 判断rule type, 0 为 p99，1 为 SR
                if (rule.getType() == 0) {
                    // 300ms
                    String threshold = rule.getThreshold();
                    if (span.getP99() > Integer.parseInt(threshold
                            .substring(0, threshold.length() - 2))) {
                        // 报警
                        return true;
                    }
                } else if (rule.getType() == 1) {
                    // 99.9%
                    String threshold = rule.getThreshold();
                    if (span.getSucRate() < Double.parseDouble(threshold.substring(0, threshold.length() - 1))) {
                        // 报警
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private List<Rule> parseRules(Collection<String> alertRules) {
        List<Rule> res = new ArrayList<>(alertRules.size());
        for (String alertRule : alertRules) {
            String[] e = alertRule.split(",");
            int id = Integer.parseInt(e[0]);
            String callerService = e[1];
            String responderService = e[2];
            byte type = "P99".equals(e[3]) ? (byte) 0 : (byte) 1;
            int i = e[4].indexOf(">");
            int j = e[4].indexOf("<");
            int triggerMinuteEnd = i == -1 ? j : i;
            /* 单独数字 */
            int triggerMinute = Integer.parseInt(e[4].substring(0, triggerMinuteEnd));
            /* 99.9% 或 300ms 类似 */
            String threshold = e[5];
            Rule rule = new Rule(id, callerService, responderService, type, triggerMinute, threshold);
            res.add(rule);
        }
        return res;
    }

    private void handleLines(String[] lines, int len) {
        for (int i = 0; i < len; i++) {
            handleLine(lines[i]);
        }
    }

    private void handleLine(String line) {
        String[] each = line.split(",");
        String callerService = each[0];
        String callerIp = each[1];
        String responderService = each[2];
        String responderIp = each[3];
        String isSuc = each[4];
        int costTime = Short.parseShort(each[5]);
        long timestamp = toFullMinute(Long.parseLong(each[6]));

        /* 构造 dataMap 的 key, +3 是因为有三个逗号 */
        String key = line.substring(0, callerService.length() + callerIp.length() + responderService.length() + responderIp.length() + 3);
        Map<Long, Span> longSpanMap = dataMap.computeIfAbsent(key, (l) -> new ConcurrentHashMap<>());
        Span span = longSpanMap.computeIfAbsent(timestamp, (l) -> new Span());
        /* 更新 span 信息 */
        span.update(costTime, "true".equals(isSuc));
    }


    private long toFullMinute(long timestamp) {
        return timestamp / 60000 * 60000;
    }

    private String decimalFormat(double number) {
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

    @Override
    public Collection<String> getLongestPath(String caller, String responder, String time, String type) {
        return null;
    }
}

class Span {
    int[] bucket;
    AtomicInteger total;
    AtomicInteger suc;

    public Span() {
        bucket = new int[350];
        total = new AtomicInteger(0);
        suc = new AtomicInteger(0);
    }

    public double getSucRate() {
        return (double) suc.get() / total.get() * 100;
    }

    public int getP99() {
        int pos = (int) (total.get() * 0.01) + 1;
        int len = bucket.length;
        for (int i = len - 1; i >= 0; i--) {
            pos -= bucket[i];
            if (pos <= 0) return i;
        }
        return 0;
    }

    /**
     * thread safe
     */
    public void update(int cost, boolean isSuc) {
        synchronized (this) {
            if (cost >= bucket.length) {
                int[] tmp = new int[cost + 20];
                System.arraycopy(bucket, 0, tmp, 0, bucket.length);
                bucket = tmp;
            }
            bucket[cost] += 1;
        }
        total.addAndGet(1);
        if (isSuc) suc.addAndGet(1);
    }

    @Override
    public String toString() {
        return "Span{" +
                "p99=" + getP99() +
                ", total=" + total.get() +
                ", suc=" + suc.get() +
                " sucRate=" + getSucRate() +"}\n";
    }
}

class Rule {
    /* 规则编号，全局唯一，int类型 */
    int id;
    /* 一个具体的主调服务名称或者ALL，ALL代表这个规则对所有的主调生效 */
    String callerService;
    /* 一个具体的被调服务名称或者ALL，ALL代表这个规则对所有的被调生效 备注：主调服务名 和 被调服务名不会同时为ALL */
    String responderService;
    /* 数据类型，(0) : p99, (1) : 成功率 */
    byte type;
    /* 触发时间 */
    int triggerMinutes;
    /* (?) 如果是p99就是小于，如果是 成功率就是大于 */
    byte compareType;
    /* 阈值 */
    String threshold;

    public Rule(int id, String callerService, String responderService, byte type, int triggerMinutes, String threshold) {
        this.id = id;
        this.callerService = callerService;
        this.responderService = responderService;
        this.type = type;
        this.triggerMinutes = triggerMinutes;
        this.compareType = type;
        this.threshold = threshold;
    }

    public int getId() {
        return id;
    }

    public String getCallerService() {
        return callerService;
    }

    public String getResponderService() {
        return responderService;
    }

    public byte getType() {
        return type;
    }

    public int getTriggerMinutes() {
        return triggerMinutes;
    }

    public byte getCompareType() {
        return compareType;
    }

    public String getThreshold() {
        return threshold;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "id=" + id +
                ", callerService='" + callerService + '\'' +
                ", responderService='" + responderService + '\'' +
                ", type=" + type +
                ", triggerMinutes=" + triggerMinutes +
                ", compareType=" + compareType +
                ", threshold='" + threshold + '\'' +
                '}';
    }
}

class AlertRecord {
    int ruleId;
    String alertVal;
    int triggerMinute;

    public AlertRecord(int ruleId, String alertVal, int triggerMinute) {
        this.ruleId = ruleId;
        this.alertVal = alertVal;
        this.triggerMinute = triggerMinute;
    }

    public int getRuleId() {
        return ruleId;
    }

    public String getAlertVal() {
        return alertVal;
    }

    public int getTriggerMinute() {
        return triggerMinute;
    }
}