package redis;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Redis {
    private final ConcurrentMap<String, Object> redisMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<Long>> expireTimer = new ConcurrentHashMap<>();


    Redis() {
        // no-op
    }

    public String set(String key, String value) {
        return set(key, value, new ArrayList<>());
    }

    // This method returns "OK" if this method is executed successfully else it returns null
    public String set(String key, Object value, List<String> options) {
        List<Long> list;
        boolean flag = false; // For KEEPTTL
        int case1 = 0; // For EX and PX
        int case2 = 0; // For NX and XX
        for (int i = 0; i < options.size(); i++) {
            if (options.get(i) == "EX") {
                i++;
                Long timer = Long.valueOf(options.get(i));
                List<Long> list1 = new ArrayList<>();
                list1.add(timer * 1000);
                list1.add(System.currentTimeMillis());
                expireTimer.put(key, list1);
                case1 = 1;
            } else if (options.get(i) == "PX") {
                i++;
                Long timer = Long.valueOf(options.get(i));
                List<Long> list1 = new ArrayList<>();
                list1.add(timer);
                list1.add(System.currentTimeMillis());
                expireTimer.put(key, list1);
                case1 = 2;
            } else if (options.get(i) == "NX") {
                case2 = 1;
            } else if (options.get(i) == "XX") {
                case2 = 2;
            } else if (options.get(i) == "KEEPTTL") {
                flag = true;
            }
        }
        if (case2 == 0) {
            if (case1 == 0) {
                if (redisMap.containsKey(key)) {
                    redisMap.put(key, value);
                    if (!flag) {
                        list = expireTimer.get(key);
                        list.remove(1);
                        list.add(System.currentTimeMillis());
                        expireTimer.put(key, list);
                    }
                } else {
                    redisMap.put(key, value);
                    list = new ArrayList<>();
                    Long expireTime = Long.valueOf(-1); // here -1 indicates that the expire time is not set and this key will never be expired until the value is set
                    list.add(expireTime);
                    list.add(System.currentTimeMillis());
                    expireTimer.put(key, list);
                }
                return "OK";
            } else {
                redisMap.put(key, value);
                return "OK";
            }
        } else if (case2 == 1) {
            if (!redisMap.containsKey(key)) {
                redisMap.put(key, value);
                if (case1 == 0) {
                    list = new ArrayList<>();
                    Long expireTime = Long.valueOf(-1); // here -1 indicates that the expire time is not set and this key will never be expired until the value is set
                    list.add(expireTime);
                    list.add(System.currentTimeMillis());
                    expireTimer.put(key, list);
                }
                return "OK";
            } else
                return null;
        } else if (case2 == 2) {
            if (redisMap.containsKey(key)) {
                redisMap.put(key, value);
                if (case1 == 0 && !flag) {
                    list = expireTimer.get(key);
                    list.remove(1);
                    list.add(System.currentTimeMillis());
                    expireTimer.put(key, list);
                }
                return "OK";
            } else
                return null;
        }
        return null;
    }

    // This method returns the value of the key if exists else returns null if the key does not exist.
    public String get(String key) {
        if (!redisMap.containsKey(key)) {
            System.out.println("The key doesn't exist in the map");
            return null;
        } else {
            List<Long> list = expireTimer.get(key);
            if (System.currentTimeMillis() - list.get(1) >= list.get(0)) {
                System.out.println("This key has been expired");
                redisMap.remove(key);
                expireTimer.remove(key);
                return null;
            }
            return (String) redisMap.get(key);
        }
    }

    // This method returns true if the timeout is set successfully else(if key does not exist) returns false.
    public boolean expire(String key, Long milliseconds) {
        // If this key exists then only we set its expiry timer
        if (expireTimer.containsKey(key)) {
            List<Long> list = new ArrayList<>();
            list.add(milliseconds);
            list.add(System.currentTimeMillis());
            expireTimer.put(key, list);
            return true;
        }
        return false;
    }

    // Returns the number of elements added to the list when no option is gives
    public int zadd(String key, List<Pair<String, Integer>> list) {
        return zadd(key, list, new ArrayList<>());
    }

    // Returns the number of elements added to the list
    public int zadd(String key, List<Pair<String, Integer>> list, List<String> options) {
        int case1 = 0; //for nx and xx
        int case2 = 0; // for ch
        int case3 = 0; //for incr
        for (int i = 0; i < options.size(); i++) {
            if (options.get(i) == "XX") {
                case1 = 1;
            } else if (options.get(i) == "NX") {
                case1 = 2;
            } else if (options.get(i) == "CH") {
                case2 = 1;
            } else if (options.get(i) == "INCR") {
                case3 = 1;
            }
        }
        int cnt = 0;
        if (case3 == 1) {
            if (list.size() != 1) {
                System.out.println("Number of elemets to be added must be one when INCR is used.");
                return 0;
            }
            if (redisMap.containsKey(key)) {
                Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                if (map.containsKey(list.get(0).getKey())) {
                    map.put(list.get(0).getKey(), map.get(list.get(0).getKey()) + list.get(0).getValue());
                } else {
                    map.put(list.get(0).getKey(), list.get(0).getValue());
                }
            }
        }
        if (case2 == 0) {
            if (case1 == 0) {
                if (!redisMap.containsKey(key)) {
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < list.size(); i++) {
                        map.put(list.get(i).getKey(), list.get(i).getValue());
                    }
                    redisMap.put(key, map);
                    cnt = list.size();
                } else {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), map.get(list.get(i).getKey()) + list.get(i).getValue());
                        } else {
                            map.put(list.get(i).getKey(), list.get(i).getValue());
                            cnt++;
                        }
                    }
                }
            } else if (case1 == 1) {
                if (redisMap.containsKey(key)) {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), map.get(list.get(i).getKey()) + list.get(i).getValue());
                        }

                    }
                }
            } else if (case1 == 2) {
                if (!redisMap.containsKey(key)) {
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < list.size(); i++) {
                        map.put(list.get(i).getKey(), list.get(i).getValue());
                    }
                    redisMap.put(key, map);
                    cnt = list.size();
                } else {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (!map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), list.get(i).getValue());
                            cnt++;
                        }
                    }
                }
            }
        } else if (case2 == 1) {
            if (case1 == 0) {
                if (!redisMap.containsKey(key)) {
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < list.size(); i++) {
                        map.put(list.get(i).getKey(), list.get(i).getValue());
                    }
                    redisMap.put(key, map);
                    cnt = list.size();
                } else {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), map.get(list.get(i).getKey()) + list.get(i).getValue());
                            cnt++;
                        } else {
                            map.put(list.get(i).getKey(), list.get(i).getValue());
                            cnt++;
                        }
                    }
                }
            } else if (case2 == 1) {
                if (redisMap.containsKey(key)) {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), map.get(list.get(i).getKey()) + list.get(i).getValue());
                            cnt++;
                        }
                    }
                }
            } else if (case2 == 2) {
                if (!redisMap.containsKey(key)) {
                    Map<String, Integer> map = new HashMap<>();
                    for (int i = 0; i < list.size(); i++) {
                        map.put(list.get(i).getKey(), list.get(i).getValue());
                    }
                    redisMap.put(key, map);
                    cnt = list.size();
                } else {
                    Map<String, Integer> map = (Map<String, Integer>) redisMap.get(key);
                    for (int i = 0; i < list.size(); i++) {
                        if (!map.containsKey(list.get(i).getKey())) {
                            map.put(list.get(i).getKey(), list.get(i).getValue());
                            cnt++;
                        }
                    }
                }
            }
        }

        return cnt;
    }

    // Returns the rank of the element. -1 represents that the element is not present
    public int zrank(String key, String element) {
        if (redisMap.containsKey(key)) {
            Map<String, Integer> map = sortByValue((HashMap<String, Integer>) redisMap.get(key));
            List<String> list = new ArrayList<>(map.keySet());
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) == element) {
                    return i + 1;
                }
            }
        }
        return -1;
    }

    // Returns the list of element
    public List<String> zrange(String key, int startIndex, int endIndex, boolean withScore) {
        if (redisMap.containsKey(key)) {
            Map<String, Integer> map = sortByValue((HashMap<String, Integer>) redisMap.get(key));
            int len = map.size();
            startIndex = (startIndex + len) % len;
            endIndex = (endIndex + len) % len;
            List<String> ans = new ArrayList<>();
            if (startIndex > endIndex || startIndex >= len) {
                return ans; // returns empty list
            }
            if (endIndex >= len) {
                endIndex = len - 1;
            }
            List<String> list = new ArrayList<>(map.keySet());
            if (withScore) {
                for (int i = startIndex; i <= endIndex; i++) {
                    ans.add(list.get(i));
                    ans.add(String.valueOf(map.get(list.get(i))));
                }
            } else {
                for (int i = startIndex; i <= endIndex; i++) {
                    ans.add(list.get(i));
                }
            }
            return ans;

        } else {
            List<String> list = new ArrayList<>();
            return list; // Return empty list
        }
    }

    public HashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                if (o1.getValue() == o2.getValue())
                    return o1.getKey().compareTo(o2.getKey());
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

}
