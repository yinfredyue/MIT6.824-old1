## Lab3a

Lab3a is about implementing a KV store based on raft. Each client talks to a number of servers, while each server runs on a Raft instance.

```
  +-------------+
  |				|
  +-------------|-----------+				
  |				V			V
Client 		+-------+	+-------+	+-------+
            |  k/v  |	|  k/v  |	|  k/v  |
            +-------+	+-------+ 	+-------+
            |  Raft	|	|  Raft	|	|  Raft	|
            +-------+	+-------+	+-------+

```



### A buggy implementation

Initial implementaion. In `keepReceivingApplyCh`, whenever a valid `ApplyMsg` is received, store it in some data structure of `map[int64]bool`. The `Get` and `PutAppend` handler would check that data structure several times for the `OpID`. If found, the handler would apply the command and remove the `OpID` from the data strcure. This easily passes the first 7 testcases, but fail the others. 

### What's the problem?

This impelenmentaion has a important flaw. The command is executed only on the leader, which receives the `Get`/`PutAppend` RPC call and waits for the `OpID` to appears in the `map[int64]bool`. For other followers, they received the `Put`/`AppendEntries` call but doesn't check the `map[int64]bool` and thus would never execute any command! So if we have no failure or network partition, the only server is alwasy alive this implementation works. But surely it would not work if failure/partition happens!



### Reconsider the definition of "Commit"

Now reconsider what's the meaning of a command being **committed** and gets received by the k/v server on the `applyCh`. It means that the server should definitely execute the command! So the correct way to execute the command immediately in `KeepReceivingApplyCh`. **This also guarantees the linearizablity** since we're receving commands in log order!



### Duplication

Fix the code accordingly doesn't pass the remaining tests yet. The hint mentions the following,

> Your solution needs to handle a leader that has called Start() for a Clerk's RPC, but loses its leadership before the request is committed to the log. In this case you should arrange for the Clerk to re-send the request to other servers until it finds the new leader. **One way to do this is for the server to detect that it has lost leadership, by noticing that a different request has appeared at the index returned by Start(), or that Raft's term has changed.** ...

The rule for our k/v serer is that the client should **only get the reply from the current leader**. When the current leader receives a request, calls `Start` on the underlying Raft instance and starts waiting for reply. Then network partition happens immediately and this leader ends up in the minority partition. Two cases:

1. The leader is not able to send out AppendEntries before the network partion happens and thus the command is not commited. In this case, the command would never be received on the `applyCh` and it's ok.
2. The leader is able to send out AppendEntries before the network partition. So this command gets replicated in the majority and would eventually be replicated on all servers when the network paritition heals.   So it would appear on the `applyCh` on the leader we started out with. But the initial leader would reply `ErrWrongLeader` and the client would reissue the command to another server, possibly the new leader in the majority partition! So in this way, the same command would get executed twice!

So we need to store the executed `OpID` and corresponding output in `appliedResult`, of type`map[int64]string` so that we deduplicate the commands. In `keepReceiveApplyCh`, we apply the command only if it doesn't appear in `appliedResult`. The `Get`/`PutAppend` handler would only check the `appliedResult`. Whenever it finds a key of the `OpID` it's waiting for, it checks if the `CommitTerm` matches the term when `Start` is callled. If no, then it means that server lost its leadership before commiting the command, and **though the server has executed the command, it should not reply to the client.** So it turns about that  `by noticing that a different request has appeared at the index returned by Start(), or that Raft's term has changed`, the check only needs to be done on the term **OR** the index. This really takes me some time to figure it out. 

With this design clear, it's straight-forward to write the implementation!



Sidenote:

My implementaion could be quite different from other implementations you might find in Github, which uses `chan` to coordinate `Put`/`Append` and `keepReceivingApplyCh`. I tried that approach find it hard to avoid deadlock, so I think my implementaion is actually easier to reason about. 

Compared to Lab2, this time we don't have detailed instruction to refer to. This gives us more freedom but also requires more design before the actual implementation.