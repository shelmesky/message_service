## message_service
高性能消息服务，快速搭建可扩展集群，吞吐量高延迟低。

### 接口测试

* CPU：Intel Core i7-4710MQ CPU @ 2.50GHz x 8
* RAM：8GB DDR3 1600
* OS：Ubuntu 14.04.3 LTS
* Software：Golang 1.5

### 内部实现

* 支持并发的map结构
* 原子操和自旋锁替代读写锁
* 对象池
* 内存池
* 多级缓存加并发缓存
* 广播消息使用引用计数
* 定期回收内存对象
* 定时器使用时间轮算法

##### 发送消息的RPS：10W/S

![image description](https://raw.githubusercontent.com/shelmesky/message_service/master/benchmark/message_service_post_rps.jpg)

##### 轮询接口的RPS：14W/S，每秒240MB吞吐量，每秒接收90W条消息

![image description](https://raw.githubusercontent.com/shelmesky/message_service/master/benchmark/poll_rps.jpg)
