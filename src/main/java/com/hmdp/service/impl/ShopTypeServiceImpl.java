package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 检查redis里有没有
        String key = "cache:shop-type";
        List<String> cachedList = stringRedisTemplate.opsForList().range(key, 0, -1);

        // 有直接返回
        if(cachedList != null && !cachedList.isEmpty()) {
            // 把json转换成ShopType
            List<ShopType> typeList = cachedList.stream().map(json-> JSONUtil
                    .toBean(json, ShopType.class))
                    .collect(Collectors.toList());
            return Result.ok(typeList);
        }

        // 没有就查数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();

        // 有就写入redis
        if(!typeList.isEmpty()) {
            List<String> jsonList = typeList.stream()
                    .map(type->JSONUtil.toJsonStr(type))
                    .collect(Collectors.toList());

            stringRedisTemplate.opsForList().rightPushAll(key, jsonList);
        }

        // 设置缓存有效期
        stringRedisTemplate.expire(key, 30, TimeUnit.MINUTES);

        return Result.ok(typeList);
    }
}
