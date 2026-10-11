package com.hmdp.utils;

public interface ILock {
    // 获取锁
    // 锁持有的超时时间，过期后自动释放
    boolean tryLock(long timeoutSec);

    // 释放锁
    void unlock();
}
