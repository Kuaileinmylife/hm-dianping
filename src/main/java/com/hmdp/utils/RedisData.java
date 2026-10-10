package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class RedisData {
    // 过期时间
    private LocalDateTime expireTime;
    private Object data;
}
