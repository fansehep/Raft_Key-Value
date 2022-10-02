# Raft KV Server
#### 本仓库是 MIT 6.824 的课后 lab
- 分布式的引入, 使多机之间进行合作提供强大的算力或存储. 但是多台机器的引入, 也带来了其他的问题, 例如, 当某台机器宕机, 断网这种情况怎么办, 由于网络分区, 延迟数据迟迟未返回怎么办, 各台机器的数据不一致怎么办, 如果服务器宕机多台, 是否还能继续向外提供服务?
- raft 作为一个工程上易于理解的分布式共识算法, 则为上述情况有效的提供容灾, 使服务保持一个高可用性, 当集群的有效机器 >= 1/2台时, 仍然能向外提供服务.

### 2A 领导人选举 (Leader Election)
- 所有服务器节点启动时，都拥有一个任期(Term)，和一个id，且所有服务器启动时都是 Follower, Raft 中每个节点拥有三个角色 Leader(集群有且唯一的领导者节点), Follower(被动受到Leader日志的节点), Candidate(准备成为Leader的候选者).
- 集群开始时, 所有节点都是Follower, 当Follower与Leader的会话超时时(论文中所说是150ms - 350ms, 个人实现是 350ms - 750ms), 将成为Candidate:
  - 1. 角色成为 Candidate
  - 2. 当前节点 term + 1
  - 3. 投自己一票
  - 4. 并行地向集群发送请求投票 RPC,
    - ```c++
      message RequestVoteArgs {
        Term        int // Candidate 的term
        CandidateId int // Candidate 的ID
      }
      message RequestVoteReply {
        Term        int  // 回复者的 term
        VoteGranted bool // 是否投给该Candidate
      }
      ``` 
    - 当受到投票 >= (1 / 2)N 时, 成为集群的Leader, 开始并行地向集群中的其他节点发送心跳, 或者在发送投票或者等待投票的过程中超时, 则需要立即变为Follower, 并且开启下一次投票. raft 使用随机超时时间.
    - 受到RPC 的
- 