package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

/** 在 IDEA 中运行 main，将 MySQL 店铺坐标导入 Redis GEO。 */
public class ShopGeoImport {

    public static void main(String[] args) {
        // 只加载数据库和 Redis 所需配置，不启动 Web 服务或秒杀订单消费线程。
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(ImportConfig.class)
                .web(WebApplicationType.NONE).run(args)) {
            ShopMapper shopMapper = context.getBean(ShopMapper.class);
            StringRedisTemplate redis = context.getBean(StringRedisTemplate.class);

            List<Shop> shops = shopMapper.selectList(null);
            // 写入之前检查全部数据，避免将经度、纬度写反或传入无效坐标。
            for (Shop shop : shops) {
                if (shop.getTypeId() == null || shop.getX() == null || shop.getY() == null
                        || !Double.isFinite(shop.getX()) || !Double.isFinite(shop.getY())
                        || Math.abs(shop.getX()) > 180 || Math.abs(shop.getY()) > 85.05112878) {
                    throw new IllegalArgumentException("店铺分类或坐标无效，id=" + shop.getId());
                }
            }

            Map<Long, List<Shop>> groups = shops.stream()
                    .collect(Collectors.groupingBy(Shop::getTypeId));
            for (Map.Entry<Long, List<Shop>> entry : groups.entrySet()) {
                String key = SHOP_GEO_KEY + entry.getKey();
                List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>();
                for (Shop shop : entry.getValue()) {
                    // member 是店铺 ID；Point 的顺序为经度 x、纬度 y。
                    locations.add(new RedisGeoCommands.GeoLocation<>(
                            shop.getId().toString(), new Point(shop.getX(), shop.getY())));
                }
                redis.opsForGeo().add(key, locations);
                // 从 Redis 读回本次导入的全部 ID，确认坐标已经写入。
                String[] ids = entry.getValue().stream()
                        .map(shop -> shop.getId().toString()).toArray(String[]::new);
                List<Point> positions = redis.opsForGeo().position(key, ids);
                if (positions == null || positions.size() != ids.length || positions.contains(null)) {
                    throw new IllegalStateException("GEO 导入校验失败：" + key);
                }
                System.out.println("GEO_IMPORTED " + key + " count=" + ids.length);
            }
            System.out.println("GEO_IMPORT_COMPLETE shops=" + shops.size() + " types=" + groups.size());
        }
    }

    @Configuration
    @EnableAutoConfiguration
    @MapperScan("com.hmdp.mapper")
    static class ImportConfig {
    }
}
