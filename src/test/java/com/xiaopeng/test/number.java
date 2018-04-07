package com.xiaopeng.test;

import com.xiaopeng.bi.utils.FileUtil;

import java.util.ArrayList;

/**
 * Created by kequan on 9/7/17.
 */
public class number {

    public static void main(String[] args) {
        java.util.Random r = new java.util.Random();

        ArrayList arr = new ArrayList();
        while (arr.size() <= 436812 * 5) {
            long a = r.nextLong();
            if (a >= 10000000000L) {
                long b = new Long((a + "").substring(0, 11));
                if (!arr.contains(b)) {
                    arr.add(b);
                    System.out.println(arr.size());
                    FileUtil.apppendTofile("number" + arr.size() % 5 + ".txt", b + "");
                }
            }
        }

    }
}
