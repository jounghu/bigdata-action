package com.skrein.spark.sql.action.printOPP;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author :hujiansong
 * @date :2019/11/18 18:14
 * @since :1.8
 */
public class CountThread implements Callable<Integer> {

    private List<Integer> num;

    private int count;

    private List<Integer> debugList = new ArrayList<>();

    public CountThread(List<Integer> num) {
        this.num = num;
    }

    @Override
    public Integer call() throws Exception {
        for (int i = 0; i < num.size(); i++) {
            if (isPri(num.get(i))) {
                this.count++;
                debugList.add(num.get(i));
            }
        }
        System.out.println(Thread.currentThread().getName() + ": " + this.debugList);
        return this.count;
    }

    private boolean isPri(int num) {
        for (int i = 2; i < num; i++) {
            if (num % i == 0) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        List<Integer> oriList = new ArrayList<>();
        int total = 100000;
        for (int i = 0; i < total; i++) {
            oriList.add(i);
        }

        int pageSize = 10000;

        int page = total / pageSize;

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < page + 1; i++) {
            if (i == page - 1) {
                Future<Integer> submit = executorService.submit(new CountThread(oriList.subList(pageSize * i, oriList.size())));
                futures.add(submit);
            } else {
                Future<Integer> submit = executorService.submit(new CountThread(oriList.subList(pageSize * i, pageSize * (i + 1))));
                futures.add(submit);
            }
        }
        int totalPri = 0;
        for (Future<Integer> future : futures) {
            Integer integer = future.get();
            totalPri += integer;
        }

        System.out.println(totalPri);
    }

}
