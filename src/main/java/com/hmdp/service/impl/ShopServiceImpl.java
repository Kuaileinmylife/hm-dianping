package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id){
        // 缓存穿透
//         Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if(shop == null){
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    // 保留课程练习入口，复用修复后的工具类。
    public Shop queryWithLogicalExpire(Long id) {
        return cacheClient.queryWithLogicalExpire(
                CACHE_SHOP_KEY, id, Shop.class, this::getById, 30L, TimeUnit.SECONDS);
    }

    // 互斥锁解决缓存击穿
    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        String lockKey = "lock:" + key;
        while (true) {
            String json = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(json)) {
                return JSONUtil.toBean(json, Shop.class);
            }
            if (json != null) {
                return null;
            }
            String token = tryLock(lockKey);
            if (token == null) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("等待缓存重建时被中断", e);
                }
                continue;
            }
            try {
                // 获取锁后再次检查缓存。
                if (stringRedisTemplate.opsForValue().get(key) != null) {
                    continue;
                }
                Shop shop = getById(id);
                if (shop == null) {
                    stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                } else {
                    cacheClient.set(key, shop, CACHE_SHOP_TTL, TimeUnit.MINUTES);
                }
                return shop;
            } finally {
                unlock(lockKey, token);
            }
        }
    }
    // 缓存穿透
    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 从redis查询店铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 判断是否存在
        if(StrUtil.isNotBlank(shopJson)){
            // 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        // 判断查的缓存是不是空值
        if(shopJson != null){
            return null;
        }

        // 不存在，根据id查数据库
        Shop shop = getById(id);

        // 不存在，返回错误
        if(shop == null){
            // 为了避免缓存穿透还要将null值写入缓存
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);

            return null;
        }

        // 存在写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 返回
        return shop;
    }

    private String tryLock(String key) {
        String token = UUID.randomUUID().toString();
        Boolean flag = stringRedisTemplate.opsForValue()
                .setIfAbsent(key, token, LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag) ? token : null;
    }

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('del', KEYS[1]) else return 0 end", Long.class);

    private void unlock(String key, String token) {
        stringRedisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(key), token);
    }
    public void saveShop2Redis(Long id, Long expireSeconds){
        // 查询店铺数据
        Shop shop = getById(id);
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断是否需要感觉坐标查询
        if(x == null || y == null){
            // 不需要坐标查询，按数据库查
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;

        // 查询redis, 按照距离排序，分页.结果：shopId, distance
        // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y), // 经纬坐标
                        new Distance(5000), // 半径
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().sortAscending().limit(end)
                );

        // 解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        if(list.size() <= from){
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }

        // 截取from~end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for(Shop shop : shops){
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        // 返回
        return Result.ok(shops);
    }

    @Override
    @Transactional
    public Result updateShop(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id不能为空");
        }

        // 更新数据库
        updateById(shop);
        // 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }
}
