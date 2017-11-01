adaptive-executor-java [![Build Status](https://travis-ci.org/PhantomThief/adaptive-executor-java.svg)](https://travis-ci.org/PhantomThief/adaptive-executor-java) [![Coverage Status](https://coveralls.io/repos/PhantomThief/adaptive-executor-java/badge.svg?branch=master)](https://coveralls.io/r/PhantomThief/adaptive-executor-java?branch=master)
=======================

自适应的Executor框架封装

* 全局最大线程数设置，防止线程泄露
* 单次任务线程数分配策略定制
* 更友好的批量提交任务API
* 只支持Java8

## 基本使用

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>adaptive-executor</artifactId>
    <version>0.1.5-SNAPSHOT</version>
</dependency>
```

```Java
// 声明executor
AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
        .withGlobalMaxThread(10) // 全局最大线程数10
        .adaptiveThread(5, 8) // 每5个任务使用一个线程，每次提交任务最多使用8个线程
        .maxThreadAsPossible(6) // 每次提交任务最多使用6个线程，尽可能多的使用多线程（这个和上面策略二选一）
        .build();
        
// 调用
Collection<Integer> tasks = Arrays.asList(1, 2, 3); // 原始任务

Map<Integer, String> result = executor.invokeAll(tasks, task -> task + " done."); // 并发执行

executor.run(tasks, System.out::println); // 并发执行，无返回值    
```

## 高级使用

可以使用withThreadStrategy(IntUnaryOperator)定制线程派生策略，IntUnaryOperator传入参数为单次提交的任务数，返回需要派生的线程数。不过注意，这个线程数依然受制于全局线程最大数。

例如：

```Java
AdaptiveExecutor executor = AdaptiveExecutor.newBuilder() //
        .withGlobalMaxThread(10) // 全局最大线程数10
        .withThreadStrategy(keySize -> keySize) // 传入多少key，就建立多少线程，和maxThreadAsPossible(Integer.MAX_VALUE)效果相同
        .build();
```

## 注意事项

* 如果派生线程数为1，则会以单线程方式执行，将以调用者线程执行
* 多线程任务，超过线程池允许的线程数后，会阻塞。如果希望超过线程池允许的线程数任务以调用者线程执行，可以使用enableCallerRunsPolicy()开启，这样做能充分利用调用者线程执行任务，不过注意，对于任务执行时间分布比较大的任务，如果出现分配给调用者线程的任务执行时间过长，会阻塞多线程提交，从而造成线程饥饿，请谨慎使用（Java Executor Framework本身CallerRunsPolicy也有一样的问题）
* AdaptiveExecutor是线程安全的，所以建议尽可能的共享实例
* AdaptiveExecutor本身会持有一个线程池，所以在用完之后需要关闭