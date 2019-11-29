package com.skrein.spark.sql.action.weibo;

/**
 * @author :hujiansong
 * @date :2019/11/11 11:24
 * @since :1.8
 */
public class InnerClass {


    class Student {
        private String name;

        public Student(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "name='" + name + '\'' +
                    '}';
        }
    }
}
