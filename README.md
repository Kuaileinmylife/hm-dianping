<div align="center">

# 黑马点评 · Redis 实战与优化

**从跟课实现，到理解问题，再动手改进。**

Java 8 · Spring Boot 2.7.4 · MyBatis-Plus · MySQL · Redis · Redisson · Nginx

[项目亮点](#项目亮点) · [我的优化](#我的优化) · [本地运行](#本地运行) · [测试验证](#测试验证)

</div>

---

## 关于项目

这是我跟随黑马程序员 Redis 实战课程完成，并在课程基础上继续优化的学习项目。业务围绕本地生活场景展开，包含店铺查询、优惠券秒杀、探店笔记、点赞、关注和签到等功能。

学习过程中，我先跟着课程完成业务，再尝试独立复写，结合实际遇到的问题理解缓存、锁、事务和异步处理的作用。完成课程后，重点对点赞功能做了一轮改进：把点赞关系持久化到 MySQL，并补上状态缓存、异常处理和自动化测试。

> 本仓库记录我的学习与改进过程。基础业务和主要技术方案来自课程，后续优化在下文单独说明。

## 项目亮点

| 业务场景 | 实现思路 | 实践重点 |
| --- | --- | --- |
| 验证码登录 | Redis 保存验证码和 Token 登录信息，拦截器解析身份、刷新有效期 | 登录态共享、ThreadLocal 用户上下文 |
| 店铺缓存 | 实现空值缓存、互斥重建、逻辑过期等课程方案，抽取 CacheClient | 缓存穿透、缓存击穿、异步重建 |
| 优惠券秒杀 | Lua 执行库存与购买资格判断，写入 Redis Stream，后台消费并落库 | 原子操作、异步下单、消费确认与 Pending 重试 |
| 分布式锁 | 练习 Redis 锁与 Lua 解锁，并使用 Redisson 保护业务临界区 | 锁粒度、锁归属、事务边界 |
| 关注与动态 | Set 求共同关注，ZSet 保存粉丝收件箱并滚动分页 | 集合运算、Feed 推送、时间游标 |
| 附近店铺 | GEO 保存店铺坐标，按距离升序查询并保留返回顺序 | 经纬度、距离排序、分页 |
| 用户签到 | Bitmap 记录签到，位运算计算连续签到天数 | 位存储与统计 |

店铺缓存的不同方案保留在 Service 和工具类中，便于对照学习。**当前店铺详情 Controller 直接调用 `getById`，尚未接入缓存查询入口**，因此不能把缓存练习代码等同于当前详情接口已使用缓存。

## 我的优化

### 1. 点赞关系持久化：MySQL 保存事实，Redis 缓存状态

课程版通过 MySQL 保存点赞数、Redis ZSet 保存点赞用户。Redis 数据丢失后，仅凭点赞数无法恢复“谁给哪篇笔记点过赞”。

针对这个问题，我增加了 `tb_blog_like` 点赞关系表：

- 每条记录对应一个用户对一篇笔记的点赞。
- 使用 `(blog_id, user_id)` 唯一约束，防止重复关系。
- 将关系新增与点赞数加一、关系删除与点赞数减一分别放入同一个 MySQL 事务。
- 取消点赞时先检查关系是否删除成功，扣减数量时增加 `liked > 0` 条件。
- 点赞用户列表改为查询数据库，按点赞时间和记录 ID 排序，返回最早的 5 位点赞用户。

### 2. 细化锁粒度，明确事务调用边界

使用“笔记 ID + 用户 ID”作为 Redisson 锁的维度，保护同一用户对同一笔记的状态判断与修改。重叠请求未取得锁时返回提示，取得锁后在 `finally` 中释放。

事务操作放在独立的 `BlogLikeServiceImpl`，由外层 Service 通过 Spring Bean 调用，让事务代理生效。数据库事务方法成功返回后，再使 Redis 状态缓存失效。

### 3. 点赞状态缓存与异常回查

```text
读取点赞状态
  ├─ Redis 命中 "1" / "0" → 返回已点赞 / 未点赞
  └─ 缓存缺失、值无效或读取异常 → 查询 MySQL
                                      ↓
                              回填缓存，有效期 60 秒

点赞 / 取消点赞
  → 获取用户与笔记维度的锁
  → 查询数据库中的点赞关系
  → 在事务中修改关系和点赞数
  → 删除旧状态缓存
  → 释放锁
```

状态缓存键为 `blog:like:status:{blogId}:{userId}`。同时缓存已点赞和未点赞状态，避免未点赞用户重复查询数据库。

Redis 读取、回填或删除异常会记录日志；读取异常时回查 MySQL，删除异常时保留已经成功提交的数据库结果，由缓存过期兜底。旧的点赞 ZSet 同步代码也已移除，避免维护无人读取的数据。

### 4. 修复联调过程中发现的问题

- **店铺列表分页**：增加加载状态、结束标记与请求版本判断，处理重复加载及切换条件后旧请求覆盖新列表的问题。
- **距离排序**：GEO 查询显式指定距离升序，并在查询 MySQL 后保留 GEO 返回的店铺顺序。前端当前使用演示坐标，距离是相对该坐标计算的。
- **GEO 数据导入**：提供独立的 `ShopGeoImport` 入口，按分类导入坐标，并校验坐标和写入结果。
- **本地 Redis 环境**：使用 Docker 容器和数据卷运行 Redis，减少对学习虚拟机的依赖。

### 当前边界

这一轮优化重点是点赞数据的持久化、事务边界和异常处理，仍保留以下取舍：

- 点赞接口采用“切换状态”，顺序重复请求仍可能把点赞切换回来，并非幂等接口。
- 缓存删除失败或并发回填可能导致短暂旧状态；当前通过过期时间兜底，尚未增加持久化补偿任务。
- 点赞关系表不能自动恢复历史 Redis 点赞用户，旧点赞数与新关系表需要另行核对。
- Redis 仍用于登录和分布式锁，局部缓存回查不代表整个系统可以在 Redis 停机时正常运行。

## 技术栈

| 层次 | 技术 |
| --- | --- |
| 后端 | Java 8、Spring Boot 2.7.4、MyBatis-Plus 3.5.2 |
| 数据与缓存 | MySQL、Redis、Spring Data Redis、Redisson |
| 并发与异步 | Lua、线程池、Redis Stream、Spring 事务 |
| 页面与代理 | 课程前端页面、Nginx 1.18.0 |
| 工具与测试 | Maven、Hutool、JUnit 5、Mockito、Spring Test、Docker |

## 本地运行

### 1. 获取代码与初始化数据库

```bash
git clone https://github.com/Kuaileinmylife/hm-dianping.git
cd hm-dianping
```

准备 JDK 8、Maven、MySQL，以及 Docker Desktop 或可访问的 Redis 服务。

在 MySQL 中准备 `hmdp` 数据库，首次搭建时依次导入：

1. 根目录的 `hmdp.sql`：课程基础表和数据。
2. `src/main/resources/db/blog_like.sql`：后续增加的点赞关系表。

已有学习数据时，请先检查 SQL 内容，避免重新导入覆盖数据。

### 2. 启动 Redis

下面是 Windows PowerShell 下的一次性创建命令：

```powershell
docker run -d --name hmdp-redis -p 127.0.0.1:6379:6379 -v hmdp-redis-data:/data redis:7 redis-server --appendonly yes
```

容器已经存在时使用 `docker start hmdp-redis`。检查连接并在首次启动后创建秒杀消费者组：

```powershell
docker exec hmdp-redis redis-cli ping
docker exec hmdp-redis redis-cli XGROUP CREATE stream.orders g1 0 MKSTREAM
```

`ping` 应返回 `PONG`；重复创建消费者组时返回 `BUSYGROUP` 表示组已存在。

### 3. 配置并启动后端

将 `src/main/resources/application.example.yaml` 复制为同目录的 `application.yaml`，填写本机 MySQL 和 Redis 配置。实际配置文件已被 Git 忽略。

`RedissonConfig` 中也有 Redis 连接地址；修改 Redis 地址或认证信息时，需要同时检查这处配置。

在 IDEA 中使用 JDK 8 运行 `HmDianPingApplication`，后端端口为 **8081**。

### 4. 启动页面与初始化业务数据

在 Windows 上进入 `src/main/resources/nginx-1.18.0`，运行 Nginx，浏览器打开 **http://localhost:8080**。

- 附近店铺：在 IDEA 中运行 `src/test/java/com/hmdp/ShopGeoImport.java` 的 `main`，将 MySQL 店铺坐标导入 Redis。
- 秒杀测试：通过 `POST /voucher/seckill` 创建秒杀券，检查数据库与 Redis 库存已初始化后再测试。仅导入 MySQL 旧券不会自动恢复 Redis 库存。
- 换用全新的 Redis 后，需要重新登录，并按需初始化消费者组、GEO 和秒杀数据。

## 测试验证

```bash
mvn test
```

当前自动化测试共 **27 项**：

| 测试类 | 数量 | 覆盖内容 |
| --- | ---: | --- |
| `BlogLikeTest` | 15 | 点赞与取消、并发拦截、缓存命中与回查、Redis 异常、用户列表顺序 |
| `BlogLikeTransactionTest` | 7 | Spring 事务代理的提交与回滚决策、关系保存失败、重复键异常、扣减失败 |
| `CacheClientTest` | 5 | 缓存工具类回归 |

本次验证结果：**27 项通过，0 失败，0 错误，0 跳过**。点赞功能也完成了本地手动验证。

自动化测试使用模拟依赖，不需要启动 MySQL 或 Redis。其中事务测试使用真实 Spring 事务拦截器与模拟事务管理器，验证提交、回滚的调用决策；不等同于真实数据库事务回滚或唯一约束的集成验证，也未提供吞吐量或压测结论。

## 目录导航

```text
├── hmdp.sql                         # 课程基础数据库
├── pom.xml                          # Maven 依赖
├── src/main/java/com/hmdp
│   ├── config                       # MVC、Redis 等配置
│   ├── controller                   # HTTP 接口
│   ├── entity / dto / mapper        # 数据模型与持久层
│   ├── service/impl                 # 业务实现与点赞事务
│   └── utils                        # 用户上下文、缓存工具、锁、ID 生成器
├── src/main/resources
│   ├── application.example.yaml     # 配置模板
│   ├── db/blog_like.sql             # 点赞关系表
│   ├── seckill.lua / unlock.lua     # Redis 脚本
│   └── nginx-1.18.0                 # 前端页面与 Nginx
└── src/test/java/com/hmdp
    ├── ShopGeoImport.java           # GEO 导入入口
    ├── service/impl                 # 点赞与事务测试
    └── utils                        # 缓存工具测试
```

## 课程来源与致谢

基础项目来自黑马程序员 Redis 实战课程，感谢课程作者和相关资料的分享。

[黑马程序员 Redis 入门到实战课程](https://www.bilibili.com/video/BV1cr4y1671t)

本仓库用于个人学习、复写与优化实践，保留原有代码中的作者信息。欢迎交流实现思路，也欢迎指出问题。
