package com.atguigu.edu.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * description:
 * Created by 铁盾 on 2022/6/18
 */
public class ThreadPoolUtil {

    private static final class ThreadPoolExecutorHolder {
        static final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                4, 20, 5 * 60, TimeUnit.SECONDS,
                new LinkedBlockingDeque<>()
        );
    }

    public static ThreadPoolExecutor getInstance() {
        return ThreadPoolExecutorHolder.threadPoolExecutor;
    }
}
