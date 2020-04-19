package redis;

import javafx.util.Pair;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Demo {
    public static void main(String[] args) {
        Redis redis = new Redis();

        //for get, set and expire
        String res =redis.set("abc", "hello");
        System.out.println(res);
        List<String> list = new ArrayList<String>();
        list.add("EX");
        list.add("10");
        list.add("NX");
        res = redis.set("abc", "hello", list);
        System.out.println(res);
        res = redis.set("xyz", "world", list);
        System.out.println(res);
        res = redis.get("abc");
        System.out.println(res);
        res = redis.get("bcd");
        System.out.println(res);
        boolean ans = redis.expire("xyz", (long) 1000);
        System.out.println(ans);

        // for zadd, zrange and zrank
        List<Pair<String, Integer>> list1 = new ArrayList<>();
        list1.add(new Pair<>("one", 1));
        list1.add(new Pair<>("two", 2));
        list1.add(new Pair<>("three", 3));
        int cnt = redis.zadd("mykey", list1);
        System.out.println(cnt);

        cnt = redis.zrank("mykey", "two");
        System.out.println(cnt);

        List<String> list2 = redis.zrange("mykey", 0, 2, true);
        for(int i=0;i<list2.size();i++) {
            System.out.println(list2.get(i));
        }
    }
}
