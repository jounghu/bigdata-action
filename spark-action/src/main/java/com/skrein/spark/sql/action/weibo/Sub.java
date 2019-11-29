package com.skrein.spark.sql.action.weibo;

/**
 * @author :hujiansong
 * @date :2019/11/12 18:04
 * @since :1.8
 */
public class Sub extends Supper {
    public void method(Supper supper){
        System.out.println(this.name);
        System.out.println(supper.name);
    }

    public static void main(String[] args) {
        new Sub().method(new Sub());
    }
}
