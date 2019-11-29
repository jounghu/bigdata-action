package com.skrein.spark.sql.action.weibo;

import org.apache.commons.math3.primes.Primes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author :hujiansong
 * @date :2019/11/11 11:24
 * @since :1.8
 */
public class Main {
    public static void main(String[] args) {
        List<InnerClass.Student> studentList = new ArrayList<InnerClass.Student>();

        InnerClass innerClass1 = new InnerClass();
        InnerClass innerClass2 = new InnerClass();

        studentList.add(innerClass1.new Student("11"));
        studentList.add(innerClass2.new Student("11"));

        System.out.println(studentList);

    }
}
