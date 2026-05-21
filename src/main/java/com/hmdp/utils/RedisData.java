package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 封装redis缓存逻辑删除对象
 */
@Data
public class RedisData {
    // 逻辑过期时间
    private LocalDateTime expireTime;
    // 封装数据对象
    private Object data;

}
