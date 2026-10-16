package com.hmdp.entity;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("tb_blog_like")
public class BlogLike {
    @TableId(type = IdType.AUTO)
    private Long id;

    private Long blogId;

    private Long userId;

    private LocalDateTime createTime;
}
