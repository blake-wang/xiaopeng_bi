package com.xiaopeng.test;

import com.xiaopeng.bi.checkdata.MissInfo2Redis;
import sun.tools.jar.Main;

import java.sql.SQLException;

/**
 * Created by kequan on 4/11/17.
 */
public class SortTest {


    public static void main(String[] args) {

        try {
            MissInfo2Redis.checkAccount("tc272041227");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static int[] bubbleSort(int[] score) {
        for (int i = 0; i < score.length - 1; i++) {
            for (int j = 0; j < score.length - i - 1; j++) {
                if (score[j] > score[j + 1]) {
                    int temp = score[j];
                    score[j] = score[j + 1];
                    score[j + 1] = temp;
                }
            }
        }
        return  score;
    }

}
