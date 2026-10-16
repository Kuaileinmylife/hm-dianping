package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import lombok.extern.slf4j.Slf4j;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.BlogLike;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogLikeService;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private IBlogLikeService blogLikeService;

    @Override
    public Result queryBlogById(Long id) {
        // 查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }

        // 查询blog有关的用户
        queryBlogUser(blog);
        // 查询blog是否被点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();

        if(user == null){
            blog.setIsLike(false);
            return;
        }

        // 判断当前用户是否已经点赞
        String key = BLOG_LIKE_STATUS_KEY + blog.getId() + ":" + user.getId();

        // 先查缓存
        try{
            // 查当前用户是否点赞
            String cached = stringRedisTemplate.opsForValue().get(key);

            //"1"：已点赞
            //"0"：未点赞
            //null：没有这条缓存
            if("1".equals(cached) || "0".equals(cached)){
                blog.setIsLike("1".equals(cached));
                return;
            }
        }catch(DataAccessException e){
            log.warn("读取点赞状态缓存失败，改查数据库，key=" + key, e);
        }

        // 没有缓存就查数据库
        Long count = blogLikeService.query()
                .eq("blog_id", blog.getId())
                .eq("user_id", user.getId())
                .count();

        boolean liked = count > 0;
        blog.setIsLike(liked);

        // 再把数据库的数据放到redis
        try {
            stringRedisTemplate.opsForValue().set(key, liked ? "1" : "0", 60, TimeUnit.SECONDS);
        } catch (DataAccessException e) {
            log.warn("写入点赞状态缓存失败，key=" + key, e);
        }
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog->{
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            return Result.fail("请先登录");
        }
        if (id == null || id <= 0) {
            return Result.fail("笔记id无效");
        }
        Long userId = user.getId();
        // 同一用户对同一笔记的判断和修改必须放在同一把锁里。
        RLock lock = redissonClient.getLock("lock:blog:like:" + id + ":" + userId);
        if (!lock.tryLock()) {
            return Result.fail("操作太快，请稍后重试");
        }

        // 外层：保证数据库或 Redis 操作出现异常时，都能执行解锁
        try {
            // 从数据库判断是否已经点赞
            Long count = blogLikeService.query().eq("blog_id", id).eq("user_id", userId).count();

            // 记录本次操作的目标状态
            boolean liked = count == 0;


            if (liked) {
                // 调用另一个service的事务方法
                // 保存点赞关系+数据库点赞数+1
                blogLikeService.likeBlog(id, userId);
            }else{
                // 取消点赞
                // 删除点赞关系和扣减数量，在同一个数据库事务内完成
                blogLikeService.unlikeBlog(id, userId);
            }

            // 数据库中的点赞状态已经改变，使旧状态缓存失效
            String statusKey = BLOG_LIKE_STATUS_KEY + id + ":" + userId;
            try {
                stringRedisTemplate.delete(statusKey);
            } catch (DataAccessException e) {
                log.error("点赞状态缓存失效失败，key=" + statusKey, e);
            }


            // 数据库已提交；状态缓存失效失败时暂由过期时间兜底，尚无主动补偿。
            return Result.ok();
        } finally {
            // 未取得锁的请求不会进入这里；异常时也释放自己持有的锁。
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店笔记
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增笔记失败");
        }
        // 查询笔记作者的所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();

        // 推送笔记id给所有粉丝
        for(Follow follow : follows){
            // 获取粉丝id
            Long userId = follow.getUserId();
            // 推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    @Override // 滚动分页查询
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();

        // 查询收件箱
        String key = FEED_KEY + userId;
        // 进行滚动分页查询 ZREVRANGEBYSCORE key Max Min Limit offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);

        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }

        // 解析数据: blogId, minTime(时间戳), offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for(ZSetOperations.TypedTuple<String> tuple : typedTuples){
            // 获取id
            ids.add(Long.valueOf(tuple.getValue()));

            // 获取分数（时间戳）
            long time = tuple.getScore().longValue();

            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }

        // 根据id查Blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            // 查询blog有关的用户
            queryBlogUser(blog);
            // 查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);
        r.setMinTime(minTime);
        return Result.ok(r);
    }

    @Override
    public Result queryBlogLikes(Long id) {
        // 查询最早点赞的5条记录
        List<BlogLike> likes = blogLikeService.query()
                .eq("blog_id", id)
                .orderByAsc("create_time")
                .orderByAsc("id")
                .last("LIMIT 5")
                .list();

        if(likes.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        // 解析出其中的用户id
        List<Long> ids = likes.stream().map(BlogLike::getUserId).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 根据用户id查询用户 WHERE id IN (5,1) ORDER BY FIELD(id, 5, 1)
        List<UserDTO> users = userService.query()
                .in("id", ids)
                .last("ORDER BY FIELD(id,"+idStr+")").list()
                .stream()
                .map(user-> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        // 返回
        return Result.ok(users);
    }
}
