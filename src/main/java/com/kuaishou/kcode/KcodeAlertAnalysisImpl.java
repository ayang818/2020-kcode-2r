package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.kuaishou.kcode.Utils.decimalFormat;
import static com.kuaishou.kcode.Utils.toFullMinute;

/**
 * @author chengyi0818
 * Created on 2020-07-10
 */
public class KcodeAlertAnalysisImpl implements KcodeAlertAnalysis {
    /* callerService+callerIp+responderService+responderIp -> Map(timestamp -> Span) */
    Map<String, Map<Long, Span>> dataMap = new ConcurrentHashMap<>(600);
    /* callerService,responderService -> formattedTimestamp -> Span, entryKey 就是边集 */
    Map<String, Map<String, Span>> Q2DataMap = new ConcurrentHashMap<>(300);
    /* 点集 */
    Map<String, Point> pointMap = new ConcurrentHashMap<>();
    Map<String, List<String>> q2Cache = new ConcurrentHashMap<>();
    /* 数据处理线程池 */
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(8, 8, 60, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10000));
    /* thread safe formatter */
    ThreadLocal<SimpleDateFormat> formatUtil = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm"));
    /* global date formatter */
    SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    /* 一个线程池中的任务多少行 */
    int taskNumberThreshold = 4000;
    /* 每分钟的毫秒跨度 */
    int millspace = 60000;

    @Override
    public Collection<String> alarmMonitor(String path, Collection<String> alertRules) {
        long start = System.currentTimeMillis();
        // *("开始读取日志......");
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
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        /* 等待线程池任务处理完 */
        threadPool.shutdown();
        while (!threadPool.isTerminated()) {
        }
        // *(String.format("数据解析完毕，耗时 %d ms，开始生成报警信息....", System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        List<Rule> ruleList = parseRules(alertRules);
        Set<String> res = getAlertInfo(ruleList);
        calcByPointMap();
        // *(String.format("报警信息生成完毕，共 %d 条，耗时 %d ms", res.size(), System.currentTimeMillis() - start));
        // *(String.format("点数量 %d，边数量 %d ", pointMap.size(), Q2DataMap.entrySet().size()));
        return res;
    }

    /**
     * @param caller    主调服务名称
     * @param responder 被调服务名称
     * @param time      报警发生时间（分钟），格式 yyyy-MM-dd hh:mm
     * @param type      监控触发类型 SR P99
     * @return [serviceZ->serviceA->serviceB|100.00%,-1% ... ]
     */
    @Override
    public Collection<String> getLongestPath(String caller, String responder, String time, String type) {
        List<String> res;
        Point callerPoint = pointMap.get(caller);
        Point responderPoint = pointMap.get(responder);
        String key = caller + responder + time + type;

        if ((res = q2Cache.get(key)) != null) {
            return res;
        }
        res = new ArrayList<>();
        // 获得前驱/后继路径
        List<Deque<Point>> headPaths = callerPoint.getHeadPaths();
        // 向后构造后继答案
        List<Deque<Point>> tailPaths = responderPoint.getTailPaths();

        StringBuilder servicePathBuilder = new StringBuilder();
        StringBuilder ansPathBuilder = new StringBuilder();
        Point callerP, responderP;
        List<Point> newPath;
        String compactedKey;
        for (Deque<Point> hPath : headPaths) {
            for (Deque<Point> tPath : tailPaths) {
                int len = hPath.size() + tPath.size();
                newPath = new ArrayList<>(len);
                newPath.addAll(hPath);
                newPath.addAll(tPath);
                boolean flag = true;
                for (int i = 0; i <= len - 2; i++) {
                    callerP = newPath.get(i);
                    responderP = newPath.get(i + 1);
                    if (flag) servicePathBuilder.append(callerP.getServiceName());
                    servicePathBuilder.append("->").append(responderP.getServiceName());
                    compactedKey = callerP.getServiceName() + "," + responderP.getServiceName();
                    Span span = Q2DataMap.get(compactedKey).get(time);
                    if (!flag) ansPathBuilder.append(",");
                    if (type.equals(ALERT_TYPE_P99)) {
                        ansPathBuilder.append(span.getP99()).append("ms");
                    } else {
                        ansPathBuilder.append(decimalFormat(span.getSucRate())).append("%");
                    }
                    if (flag) flag = false;
                }
                String record = servicePathBuilder.toString() + "|" + ansPathBuilder.toString();
                // *(record);
                res.add(record);
                servicePathBuilder.delete(0, servicePathBuilder.length());
                ansPathBuilder.delete(0, ansPathBuilder.length());
            }
        }
        q2Cache.put(key, res);
        return res;
    }


    private Set<String> getAlertInfo(List<Rule> ruleList) {
        // *(String.format("规则集中有 %d 条规则", ruleList.size()));
        int ruleSize = ruleList.size();
        Set<String> res = new HashSet<>(5000);
        // servicePair+ipPair -> type[](p99 或 SR)-> ruleId -> Map<Long, AlertRecord>
        Map<String, Map<Integer, Map<Long, AlertRecord>>[]> collectMap = new HashMap<>(400);
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
        return res;
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

    private void calcByPointMap() {
        // 扫描 pointSet 点集合，获得所有点的最长前驱和最长后继
        pointMap.forEach((strServiceName, point) -> {
            // 得到最长前驱
            dfs(point, true);
            // 得到最长后继
            dfs(point, false);
        });
        // 更新每个点的最长前驱/后继路径
        pointMap.forEach((strServiceName, point) -> {
            Deque<Point> headPath = new LinkedList<>();
            List<Deque<Point>> headPaths = new ArrayList<>();
            headPath.offerFirst(point);
            genePointPath(point, headPath, headPaths, false);
            point.setHeadPaths(headPaths);

            Deque<Point> tailPath = new LinkedList<>();
            List<Deque<Point>> tailPaths = new ArrayList<>();
            tailPath.offer(point);
            genePointPath(point, tailPath, tailPaths, true);
            point.setTailPaths(tailPaths);
        });
        // 对每个点构造str形式的 path , p99 path, SR path
        pointMap.forEach((strServiceName, point) -> {
            List<Deque<Point>> headPaths = point.getHeadPaths();
            point.setStrPrePath(geneStrPaths(point, headPaths));
            List<Deque<Point>> tailPaths = point.getTailPaths();
            point.setStrNextPath(geneStrPaths(point, tailPaths));
        });
    }

    private int dfs(Point point, boolean findPre) {
        Set<Point> findPointSet;
        if (findPre) {
            findPointSet = point.getPreSet();
        } else {
            findPointSet = point.getNextSet();
        }
        // 如果前驱/后继为空，那么这个点往前/往后的最长距离为 0
        if (findPointSet.size() == 0) return 0;
        int max = -10000;
        Set<Point> maxDisSet = new HashSet<>();
        // 当前节点的所有前驱，找前驱距离的最大值
        for (Point pnt : findPointSet) {
            int longestDis;
            if (findPre) {
                longestDis = pnt.getMaxPreDis();
            } else {
                longestDis = pnt.getMaxNextDis();
            }
            // 前/后驱已有具体值，记忆化
            if (longestDis != -1) {
                // 加上自身长度
                longestDis += 1;
                if (longestDis > max) {
                    max = longestDis;
                    maxDisSet.clear();
                    maxDisSet.add(pnt);
                } else if (longestDis == max) {
                    maxDisSet.add(pnt);
                }
            } else {
                // 递归得到距离
                int dis = dfs(pnt, findPre);
                dis += 1;
                if (dis > max) {
                    max = dis;
                    maxDisSet.clear();
                    maxDisSet.add(pnt);
                } else if (dis == max) {
                    maxDisSet.add(pnt);
                }
            }
        }
        int res = max;
        if (findPre) {
            point.setMaxPreSet(maxDisSet);
            point.setMaxPreDis(res);
        } else {
            point.setMaxNextSet(maxDisSet);
            point.setMaxNextDis(res);
        }
        return res;
    }

    private void genePointPath(Point responderPoint, Deque<Point> longestPath, List<Deque<Point>> paths, boolean isTail) {
        Set<Point> findSet;
        if (isTail) {
            findSet = responderPoint.getMaxNextSet();
        } else {
            findSet = responderPoint.getMaxPreSet();
        }
        // 没有后继说明，已经到终点了
        if (findSet.size() == 0) paths.add(longestPath);
        for (Point pnt : findSet) {
            Deque<Point> anotherPath = new LinkedList<>(longestPath);
            if (isTail) {
                anotherPath.offer(pnt);
            } else {
                anotherPath.offerFirst(pnt);
            }
            genePointPath(pnt, anotherPath, paths, isTail);
        }
    }

    private List<String> geneStrPaths(Point point, List<Deque<Point>> pointPaths) {
        StringBuilder pathBuilder = new StringBuilder();
        List<String> strPaths = new ArrayList<>();
        for (Deque<Point> headPath : pointPaths) {
            for (Point pnt : headPath) {
                pathBuilder.append(pnt.getServiceName()).append("->");
            }
            pathBuilder.delete(pathBuilder.length() - 2, pathBuilder.length());
            strPaths.add(pathBuilder.toString());
            pathBuilder.delete(0, pathBuilder.length());
        }
        return strPaths;
    }

    private String parseDate(long timestamp) {
        return dateFormatter.format(timestamp);
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
        boolean suc = "true".equals(isSuc);

        /* 构造 dataMap 的 key, +3 是因为有三个逗号 */
        String key = line.substring(0, callerService.length() + callerIp.length() + responderService.length() + responderIp.length() + 3);
        Map<Long, Span> longSpanMap = dataMap.computeIfAbsent(key, (l) -> new ConcurrentHashMap<>());
        Span span = longSpanMap.computeIfAbsent(timestamp, (l) -> new Span());
        /* 更新 span 信息 */
        span.update(costTime, suc);
        /* 一阶段处理结束 */


        /* callerService,responderService -> formattedTimestamp -> Span */
        String q2Key = callerService + "," + responderService;
        SimpleDateFormat ft = formatUtil.get();
        String date = ft.format(timestamp);
        Map<String, Span> nextMap = Q2DataMap.computeIfAbsent(q2Key, (l) -> new ConcurrentHashMap<>());
        Span serviceSpan = nextMap.computeIfAbsent(date, (l) -> new Span());
        serviceSpan.update(costTime, suc);

        Point enter = pointMap.computeIfAbsent(callerService, (l) -> new Point(callerService));
        Point out = pointMap.computeIfAbsent(responderService, (l) -> new Point(responderService));
        synchronized (enter) {
            enter.getNextSet().add(out);
        }
        synchronized (out) {
            out.getPreSet().add(enter);
        }
    }
}

class Span {
    int[] bucket;
    AtomicInteger total;
    AtomicInteger suc;
    int p99;

    public Span() {
        bucket = new int[350];
        total = new AtomicInteger(0);
        suc = new AtomicInteger(0);
        p99 = -1;
    }

    public double getSucRate() {
        return (double) suc.get() / total.get() * 100;
    }

    public int getP99() {
        if (p99 != -1) return p99;
        int pos = (int) (total.get() * 0.01) + 1;
        int len = bucket.length;
        for (int i = len - 1; i >= 0; i--) {
            pos -= bucket[i];
            if (pos <= 0) {
                p99 = i;
                return i;
            }
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
                " sucRate=" + getSucRate() + "}\n";
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

class Point {
    int maxPreDis;
    int maxNextDis;
    String serviceName;
    Set<Point> maxPreSet;
    Set<Point> maxNextSet;
    Set<Point> preSet;
    Set<Point> nextSet;
    List<Deque<Point>> headPaths;
    List<Deque<Point>> tailPaths;
    List<String> strPrePath;
    List<String> strNextPath;

    public Point(String serviceName) {
        this.serviceName = serviceName;
        this.maxPreDis = -1;
        this.maxNextDis = -1;
        preSet = new HashSet<>();
        nextSet = new HashSet<>();
        this.maxPreSet = new HashSet<>();
        this.maxNextSet = new HashSet<>();
    }

    public List<String> getStrPrePath() {
        return strPrePath;
    }

    public void setStrPrePath(List<String> strPrePath) {
        this.strPrePath = strPrePath;
    }

    public void setStrNextPath(List<String> strNextPath) {
        this.strNextPath = strNextPath;
    }

    public List<String> getStrNextPath() {
        return strNextPath;
    }

    public List<Deque<Point>> getHeadPaths() {
        return headPaths;
    }

    public void setHeadPaths(List<Deque<Point>> headPaths) {
        this.headPaths = headPaths;
    }

    public List<Deque<Point>> getTailPaths() {
        return tailPaths;
    }

    public void setTailPaths(List<Deque<Point>> tailPaths) {
        this.tailPaths = tailPaths;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setMaxNextDis(int maxNextDis) {
        this.maxNextDis = maxNextDis;
    }

    public int getMaxNextDis() {
        return maxNextDis;
    }

    public Set<Point> getPreSet() {
        return preSet;
    }

    public Set<Point> getNextSet() {
        return nextSet;
    }

    public Set<Point> getMaxPreSet() {
        return maxPreSet;
    }

    public Set<Point> getMaxNextSet() {
        return maxNextSet;
    }

    public int getMaxPreDis() {
        return maxPreDis;
    }

    public void setMaxPreDis(int maxPreDis) {
        this.maxPreDis = maxPreDis;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setMaxPreSet(Set<Point> maxPreSet) {
        this.maxPreSet = maxPreSet;
    }

    public void setMaxNextSet(Set<Point> maxNextSet) {
        this.maxNextSet = maxNextSet;
    }

    public void setPreSet(Set<Point> preSet) {
        this.preSet = preSet;
    }

    public void setNextSet(Set<Point> nextSet) {
        this.nextSet = nextSet;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Point point = (Point) o;
        return serviceName.equals(point.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName);
    }
}