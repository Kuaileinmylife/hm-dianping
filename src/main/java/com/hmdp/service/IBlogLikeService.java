package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.entity.BlogLike;

public interface IBlogLikeService extends IService<BlogLike> {
    void likeBlog(Long blogId, Long userId);

    void unlikeBlog(Long blogId, Long userId);
}
