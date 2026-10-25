package com.hmdp;

import com.hmdp.entity.User;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class RocketMQTest {
    @Resource
    private RocketMQTemplate rocketMQTemplate;

    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Test
    void testSendMessage(){
        SendResult msg = rocketMQTemplate.syncSend("hmdp-mq-test", "你好哇");
        System.out.println("发送结果：" + msg);

        // 检查broker是否返回发送成功
        assertEquals(SendStatus.SEND_OK, msg.getSendStatus());
    }

    @Test
    void prepareLoadTestUsers() throws Exception{
        // 从本地测试库里选出100个不同用户
        List<User> users = userService.query()
                .orderByAsc("id")
                .last("LIMIT 100")
                .list();

        if(users.size() < 100){
            throw new IllegalStateException("测试用户不足100个");
        }

        List<String> lines = new ArrayList<>();
        lines.add("userId,token");

        for(User user:users){
            // 每个用户生成一个独立token
            String token = UUID.randomUUID().toString().replace("-","");

            // 按照现有登录格式保存用户信息
            Map<String, String> userMap = new HashMap<>();
            userMap.put("id", user.getId().toString());

            if(user.getNickName() != null){
                userMap.put("nickName", user.getNickName());
            }

            if(user.getIcon() != null){
                userMap.put("icon", user.getIcon());
            }

            String key = RedisConstants.LOGIN_USER_KEY + token;
            stringRedisTemplate.opsForHash().putAll(key, userMap);
            stringRedisTemplate.expire(key, 30, TimeUnit.MINUTES);

            // 一行对应一个用户
            lines.add(user.getId() + "," + token);

        }

        // 所有用户准备完成后，统一导出CSV文件
        Path file = Paths.get("target", "jmeter-users.csv").toAbsolutePath();

        Files.createDirectories(file.getParent());
        Files.write(file, lines, StandardCharsets.UTF_8);

        System.out.println("用户数据已生成，文件位置：" + file);
    }
}
