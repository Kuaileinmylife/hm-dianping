-- 点赞关系表：一行表示一个用户点赞了一篇笔记。
CREATE TABLE IF NOT EXISTS tb_blog_like (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '点赞记录ID',
    blog_id BIGINT UNSIGNED NOT NULL COMMENT '笔记ID',
    user_id BIGINT UNSIGNED NOT NULL COMMENT '用户ID',
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '点赞时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_blog_user (blog_id, user_id),
    KEY idx_user_blog (user_id, blog_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='笔记点赞关系表';