package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.entity.Blog;
import com.hmdp.entity.BlogLike;
import com.hmdp.mapper.BlogLikeMapper;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogLikeService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

@Service
public class BlogLikeServiceImpl extends ServiceImpl<BlogLikeMapper, BlogLike> implements IBlogLikeService {
    @Resource
    private BlogMapper blogMapper;

    @Override
    @Transactional
    public void unlikeBlog(Long blogId, Long userId) {
        // 删除这个用户对这个笔记的点赞记录
        boolean removed = remove(
                new LambdaQueryWrapper<BlogLike>()
                        .eq(BlogLike::getBlogId, blogId)
                        .eq(BlogLike::getUserId, userId)
        );

        // 没有点赞记录，就不能再扣减点赞数
        if(!removed){
            return;
        }

        // 笔记点赞数-1，且不能-成负数
        int rows = blogMapper.update(
                null,
                new UpdateWrapper<Blog>()
                        .setSql("liked = liked - 1")
                        .eq("id", blogId)
                        .gt("liked", 0)
        );

        if(rows != 1){
            throw new IllegalStateException("取消点赞失败，点赞数异常");
        }
    }

    @Override
    @Transactional
    public void likeBlog(Long blogId, Long userId) {
        // 保存点赞关系
        BlogLike blogLike = new BlogLike();
        blogLike.setBlogId(blogId);
        blogLike.setUserId(userId);

        boolean saved = save(blogLike);
        if(!saved){
            throw new IllegalStateException("保存点赞记录失败");
        }

        // 笔记点赞数+1
        int rows = blogMapper.update(
                null,
                new UpdateWrapper<Blog>().setSql("liked = liked + 1").eq("id", blogId)
        );

        if(rows != 1){
            throw new IllegalStateException("笔记不存在或点赞数更新失败");
        }
    }
}
