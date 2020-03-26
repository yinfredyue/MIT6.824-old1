## Lab2

This lab is about implementing Raft. Effectively, it covers up to section 5 in the paper. Section 6 is not discussed and section 7 ~ 8 is in Lab3.

Thoughts:

1. Reading the provided material is important. Other than the paper itself, the student guide, the two guides (locking, structure) on Raft are all important and very insightful. Read them carefully!

2. In my initial implementation, I created many background routine in `Make`, to handle operations like commit, apply, election timeout, send heartbeat, send log replication request. This is ok, but less efficient and could cause weird bugs (one is mentioned in the comment of `raft.go`). So following the suggestion in the Raft structure guide that:

    > ```
    > It's easiest to do the RPC reply processing in the same goroutine, rather than sending reply information over a channel.
    > ```

    I create only two background routines in `Make`: `electionTimeoutRoutine` and `applyCommandRoutine`. For other operations, I either handle the reply directly when I receive them, or start them only when needed (like `periodicHeartbeatRoutine` and `logReplicationRoutine`).

3. Design issue. How to set up the `applyCommandRoutine`? The guide mentions that:

    > ```
    > You'll want to have a separate long-running goroutine that sends
    > committed log entries in order on the applyCh. It must be separate,
    > since sending on the applyCh can block; and it must be a single
    > goroutine, since otherwise it may be hard to ensure that you send log
    > entries in log order. The code that advances commitIndex will need to
    > kick the apply goroutine; it's probably easiest to use a condition
    > variable (Go's sync.Cond) for this.
    > ```

    So there're 3 issues: seprate go routine (to avoid blocking), in log order, how to kick it? 

    This is done in `applyCommandRoutine`. It would be notified by the code that might change `rf.CommitIndex` using a channel. Also, the log order is automatically maintained since there's only one such routine and it's sending in the log order to the `applyCh`. 

4. There're actually many similaries between heartbeat and normal AppendEntries. Actually, heartbeat should be different from AppendEntries only in the `entries` send, the `AppendEntries` RPC handler should handle them in the same way and the reply from heartbeat and normal AppendEntries should be handled in the same way! This is done in `sendAppendEntriesAndHandleReply`.

5. Another problem is how to coordinate heartbeat and normal AppendEntries for log replication. According to Figure 2, heartbeat should be generated only when the network is idle, and if the reply from `AppendEntries` is negative due to log consistency, it should retry. But it does not mention what should be done if we didn't receive any reply due to network partition or if the follower is dead. 

    It turns out that if you blindly retry in the second case when the follower crashed, the leader would fire out too many `AppendEntries` to the dead server and doesn't get enough CPU cycles to send AppendEntries to other servers and would lose its leadership! So you should be careful when you retry. 

6. Fast backup is important to pass the test in 2B and 2C. If you implement it in 2B, 2C would not take too many efforts.