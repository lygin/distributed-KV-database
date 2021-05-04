aut : HUST-李俊

### Overview

---

通过一系列的project，来搭建起来一个基于raft 一致性算法的分布式键值存储

- project1：  构建一个独立的 kv server（只有1个node)

- project2：  基于raft算法实现分布式键值服务器

  -2a：实现基本的raft一致性算法，三个部分-a：Leader选举 , -b:日志同步，-c:节点接口

  -2b：利用2a实现的Raft算法模块来构建分布式kv数据库服务端

  -2c：利用snapshot对服务器进行日志压缩（服务器不能让日志无限增长）

- project3：  在project2的基础上支持多个Raft集群

  -3a：对raft算法补充成员变更和leader变更逻辑。

  -3b：使服务端支持3a的功能 ，并实现区域分割。 

  -3c：为调度器实现调度操作，具体是为`Scheduler`实现`AddPeer`和`RemovePeer`命令。 

- project4： 构建`transaction system`实现多客户端通信，主要两个部分：`MVCC`和 `transactional API`