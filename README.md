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

### Project1

---

**总结：**

这一部分直接使用badger作为存储引擎，无需自行定义，编程前可以学习一下badger中的API，对badger数据库的get、put、delete等操作在框架代码中已基本提供，实现时只需构造相应的参数并调用对应的函数即可。

**两件事：**

**①实现一个standalone storage engine**

由于是lab1，还没有引入分布式，现在只要写一个单进程的storage engine，实现tinykv 中已经定义好的storage interface。

不用自己去写一个存储引擎，tinykv已经实现了一个badger db，只需要调用badger db的相关接口就能实现存储操作。

**②实现server的gRPC接口**

server需要实现好四个rpc接口（Get,Put,Delete,Scan),只需要调用standalone storage engine提供的方法做一点额外处理就行

**代码编写部分：**

- `server`：该目录下的`server.go`是需要补充的文件之一，`Server`结构体是与客户端链接的部分，需要实`Put/Delete/Get/Scan`这几个操作。
- `storage`：主要关注`modify.go`、`storage.go`和`standalone_storage/standalone_storage.go`，这也是上述提到的引擎部分，`Server`结构体中的`storage`也就是这里的standalone_storage
- `util`：重点关注`engine_util`下的`engines.go`, `write_batch.go`, `cf_iterator.go`，通过`engine_util`提供的方法执行所有读/写操作

**注意点：**`server_test.go`这个是测试文件，错误都需要这找原因。

### Project2

---

**总结：**

主要是Raft算法的实现与封装应用，即基于raft算法实现分布式键值服务器

2A.实现Raft算法

2B.基于Raft算法构建分布式kv数据库服务端

2C.在2B的基础上增加日志压缩和安装快照的相关处理逻辑

**Raft一致性算法**

**①Raft将系统中的角色分为领导者（Leader）、跟从者（Follower）和候选人（Candidate）：**

- **Leader**：接受客户端请求，并向Follower同步请求日志，当日志同步到大多数节点上后告诉Follower提交日志。
- **Follower**：接受并持久化Leader同步的日志，在Leader告之日志可以提交之后，提交日志。
- **Candidate**：Leader选举过程中的临时角色。

**②Raft要求系统在任意时刻最多只有一个Leader，正常工作期间只有Leader和Followers。**

③**三者之间的角色状态转换**

Follower只响应其他服务器的请求。如果Follower超时没有收到Leader的消息，它会成为一个Candidate并且开始一次Leader选举。收到大多数服务器投票的Candidate会成为新的Leader。Leader在stop之前会一直保持Leader的状态。

**Steps:**

- Leader选举：Leader通过定期向所有Followers发送心跳信息维持其统治

  Raft 使用心跳（heartbeat）触发Leader选举。当服务器启动时，初始化为Follower。Leader向所有Followers周期性发送heartbeat。如果Follower在选举超时时间内没有收到Leader的heartbeat，就会等待一段随机的时间后发起一次Leader选举。

  **代码编写部分：**`raft/raft.go`

  **注意点**：论文只提到投票数多于一半时变成leader，但没有说反对数多余一半时变成follower，所以这部分得自己加，否则有些测试过不去。

- 日志同步：Leader通过强制Followers复制它的日志来处理日志的不一致，Followers上的不一致的日志会被Leader的日志覆盖。

  Leader选出后，就开始接收客户端的请求。Leader把请求作为日志条目（Log entries）加入到它的日志中，然后并行的向其他服务器发起 `AppendEntries`  复制日志条目。当这条日志被复制到大多数服务器上，Leader将这条日志应用到它的状态机并向客户端返回执行结果。

  **代码编写部分：**`raft/raft.go` `raft/log.go`

  **注意点：**此时没有实现日志压缩，可以对其进行修改，分阶段缩小测试规模进行测试。

- 实现Rawnode接口：`raft.RawNode`是与上层应用程序交互的接口，如发送`message`给其他`Peer`

  **代码编写部分：**`raft/rawnode.go`

- 根据自己的理解，理清楚整个流程（个人理解5层）：

  **代码编写部分：**`kv/raftstore/peer_msg_handler.go`中处理Raft命令的`proposeRaftCommand` func

  处理Raft返回ready时的`handleRaftReady`函数

  **注意点：**2b复杂的测试函数，可能会因内存不足导致卡死，可以修改Makefile逐一对测试函数进行测试。

- 日志压缩：实际的系统中，不能让日志无限增长。Raft采用对整个系统进行snapshot来解决，snapshot之前的日志都可以丢弃。

  Snapshot中包含:

  1.日志元数据。最后一条已提交的 log entry的 `log index`和`term`。这两个值在snapshot之后的第一条log entry的AppendEntries RPC的完整性检查的时候会被用上。

  2.系统当前状态

  **代码编写部分：**`raftstore/peer.go` `raft/raft.go` `raft/log.go`

- 成员变更：每次只能增加或删除一个成员。由Leader发起，得到多数派确认后，返回客户端成员变更成功。(Project3)

- 安全性：主要通过控制`term`与`log index`实现（Project3）

  拥有最新的已提交的log entry的Follower才有资格成为Leader。

  Leader只能推进commit index来提交当前term的已经复制到大多数服务器上的日志，旧term日志的提交要等到提交当前term的日志来间接提交（log index 小于 commit index的日志被间接提交）。

### Project3

---

**总结：**

在project2的基础上支持多个Raft集群。

3A. 在raft模块增加处理增删节点、转换leader的相关处理逻辑

3B. 在服务端增加处理增删节点、转换leader、region split的相关逻辑

3C. 通过regionHeartbeat更新scheduler中的相关信息、实现Schedule函数生成调度操作，避免store中有过多region



**3A.在Raft模块增加领导者更改和成员更改**

在Raft模块增加MsgTransferLeader，EntryConfChange，MsgHup相关处理逻辑

**3B.在RaftStore中增加领导者更改、成员更改、区域分割**

**（**1**）领导者更改**

​    proposeRaftCommand中直接调用rawnode的TransferLeader并返回相应的response即可

**（**2**）成员更改**

proposeRaftCommand中调用rawnode的ProposeConfChange

​    handleraftready中更新confver、ctx.storeMeta、PeerCache等相关信息，调用rawnode的ApplyConfChange

**（**3**）区域分割**

​    Region中维护了该region的start key和end key，当Region中的数据过多时应对该Region进行分割，划分为两个Region从而提高并发度，执行区域分割的大致流程如下：

1. ​	split checker检测是否需要进行Region split，若需要则生成split key并发送相应命令

2. ​    proposeRaftCommand执行propose操作

3. ​    提交后在HandleRaftReady对该命令进行处理：


- ​       检测split key是否在start key 和end key之间，不满足返回

- ​       创建新region 原region和新region key范围为start~split，split~end

- ​       更新d.ctx.storeMeta中的regionranges和regions

- ​       为新region创建peer，调insertPeerCache插入peer信息

- ​       发送MsgTypeStart消息


4. ​	执行get命令时检测请求的key是否在该region中，若不在该region中返回相应错误

**3C.实现调度器Schedule**

- ​	kv数据库中所有数据被分为几个Region，每个Region包含多个副本，调度程序负责调度每个副本的位置以及Store中Region的个数以避免Store中有过多Region。

- ​    为实现调度功能，region应向调度程序定期发送心跳，使调度程序能更新相关的信息从而能实现正确的调度，processRegionHeartbea函数实现处理region发送的心跳，该函数通过regioninfo更新raftcluster中的相关信息。

- ​	为避免一个store中有过多Region而影响性能，要对Store中的Region进行调度，通过Schedule函数实现

### project4

---

**总结：**

在project3的基础上支持分布式事务

- ​    4A 实现结构体MvccTxn的相关函数
- ​    4B 实现server.go中的KvGet, KvPrewrite, KvCommit函数
- ​    4C 实现server.go中的KvScan, KvCheckTxnStatus, KvBatchRollback,KvResolveLock函数