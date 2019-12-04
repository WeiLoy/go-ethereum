// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package downloader

import (
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
)

// stateReq represents a batch of state fetch requests grouped together into
// a single data retrieval network packet.
// stateReq表示一组组合在一起的状态获取请求
// 单个数据检索网络数据包。
type stateReq struct {
	items    []common.Hash              // Hashes of the state items to download
	tasks    map[common.Hash]*stateTask // Download tasks to track previous attempts
	timeout  time.Duration              // Maximum round trip time for this to complete
	timer    *time.Timer                // Timer to fire when the RTT timeout expires
	peer     *peerConnection            // Peer that we're requesting from
	response [][]byte                   // Response data of the peer (nil for timeouts)
	dropped  bool                       // Flag whether the peer dropped off early
}

// timedOut returns if this request timed out.
func (req *stateReq) timedOut() bool {
	return req.response == nil
}

// stateSyncStats is a collection of progress stats to report during a state trie
// sync to RPC requests as well as to display in user logs.
type stateSyncStats struct {
	processed  uint64 // Number of state entries processed
	duplicate  uint64 // Number of state entries downloaded twice
	unexpected uint64 // Number of non-requested state entries received
	pending    uint64 // Number of still pending state entries
}

// syncState starts downloading state with the given root hash.
func (d *Downloader) syncState(root common.Hash) *stateSync {
	s := newStateSync(d, root)
	select {
	case d.stateSyncStart <- s:
	case <-d.quitCh:
		s.err = errCancelStateFetch
		close(s.done)
	}
	return s
}

// stateFetcher manages the active state sync and accepts requests
// on its behalf.
// stateFetcher管理活动状态同步并接受请求
// 代表它。
func (d *Downloader) stateFetcher() {
	for {
		select {
		// 这里是接收从syncState发来的stateSync对象
		case s := <-d.stateSyncStart:
			for next := s; next != nil; {
				next = d.runStateSync(next)
			}
		case <-d.stateCh:
			// Ignore state responses while no sync is running.
		case <-d.quitCh:
			return
		}
	}
}

// runStateSync runs a state synchronisation until it completes or another root
// hash is requested to be switched over to.
//runStateSync运行状态同步，直到它完成或另一个根
//请求切换到哈希
func (d *Downloader) runStateSync(s *stateSync) *stateSync {
	var (
		active   = make(map[string]*stateReq) // Currently in-flight requests
		finished []*stateReq                  // Completed or failed requests
		timeout  = make(chan *stateReq)       // Timed out active requests
	)
	defer func() {
		// Cancel active request timers on exit. Also set peers to idle so they're
		// available for the next sync.
		for _, req := range active {
			req.timer.Stop()
			req.peer.SetNodeDataIdle(len(req.items))
		}
	}()
	// Run the state sync.
	go s.run()
	defer s.Cancel()

	// Listen for peer departure events to cancel assigned tasks
	peerDrop := make(chan *peerConnection, 1024)
	peerSub := s.d.peers.SubscribePeerDrops(peerDrop)
	defer peerSub.Unsubscribe()

	for {
		// Enable sending of the first buffered element if there is one.
		var (
			deliverReq   *stateReq
			deliverReqCh chan *stateReq
		)
		// 这里就是一直循环来判断，是不是已经有完成的请求，如果有就把数组第一个值取出来
		// 这里就类似实现了一个优先级队列，先完成的请求先被处理
		if len(finished) > 0 {
			deliverReq = finished[0]
			deliverReqCh = s.deliver
		}

		select {
		// The stateSync lifecycle:
		case next := <-d.stateSyncStart:
			return next

		case <-s.done:
			return nil

		// Send the next finished request to the current sync:
		// 把待处理的数据发过去
		case deliverReqCh <- deliverReq:
			// Shift out the first request, but also set the emptied slot to nil for GC
			// 这里把前面取出去做处理的数据，从完成队列里面删掉
			copy(finished, finished[1:])
			finished[len(finished)-1] = nil
			finished = finished[:len(finished)-1]

		// Handle incoming state packs:
		// 这里是处理节点发来的数据包
		case pack := <-d.stateCh:
			// Discard any data not requested (or previously timed out)
			// 丢弃未请求的任何数据（或之前已超时）
			req := active[pack.PeerId()]
			if req == nil {
				log.Debug("Unrequested node data", "peer", pack.PeerId(), "len", pack.Items())
				continue
			}
			// Finalize the request and queue up for processing
			// 完成请求之后，把接收到的数据放到完成的队列里面，然后等着排队处理
			req.timer.Stop()
			req.response = pack.(*statePack).states

			finished = append(finished, req)
			// 把这个完成的请求，从正在请求中的map里面删掉
			delete(active, pack.PeerId())

			// Handle dropped peer connections:
		case p := <-peerDrop:
			// Skip if no request is currently pending
			req := active[p.id]
			if req == nil {
				continue
			}
			// Finalize the request and queue up for processing
			req.timer.Stop()
			req.dropped = true

			finished = append(finished, req)
			delete(active, p.id)

		// Handle timed-out requests:
		case req := <-timeout:
			// If the peer is already requesting something else, ignore the stale timeout.
			// This can happen when the timeout and the delivery happens simultaneously,
			// causing both pathways to trigger.
			if active[req.peer.id] != req {
				continue
			}
			// Move the timed out data back into the download queue
			finished = append(finished, req)
			delete(active, req.peer.id)

		// Track outgoing state requests:
		// 向节点发送获取数据的请求之后，会通知到这里来
		case req := <-d.trackStateReq:
			// If an active request already exists for this peer, we have a problem. In
			// theory the trie node schedule must never assign two requests to the same
			// peer. In practice however, a peer might receive a request, disconnect and
			// immediately reconnect before the previous times out. In this case the first
			// request is never honored, alas we must not silently overwrite it, as that
			// causes valid requests to go missing and sync to get stuck.
			// 如果此对等方已存在活动请求，则表示存在问题。 在
			//理论trie节点调度必须永远不要为这两个请求分配两个请求
			//同行 然而，在实践中，对等体可能会收到请求，断开连接和
			//在之前的时间之前立即重新连接。 在这种情况下第一个
			//请求永远不会被尊重，唉，我们不能无声地覆盖它，就像那样
			//导致有效请求丢失并同步以卡住
			if old := active[req.peer.id]; old != nil {
				log.Warn("Busy peer assigned new state fetch", "peer", old.peer.id)

				// Make sure the previous one doesn't get siletly lost
				old.timer.Stop()
				old.dropped = true

				finished = append(finished, old)
			}
			// Start a timer to notify the sync loop if the peer stalled.
			req.timer = time.AfterFunc(req.timeout, func() {
				select {
				case timeout <- req:
				case <-s.done:
					// Prevent leaking of timer goroutines in the unlikely case where a
					// timer is fired just before exiting runStateSync.
				}
			})
			active[req.peer.id] = req
		}
	}
}

// stateSync schedules requests for downloading a particular state trie defined
// by a given state root.
type stateSync struct {
	d *Downloader // Downloader instance to access and manage current peerset

	sched  *trie.Sync                 // State trie sync scheduler defining the tasks
	keccak hash.Hash                  // Keccak256 hasher to verify deliveries with
	tasks  map[common.Hash]*stateTask // Set of tasks currently queued for retrieval

	numUncommitted   int
	bytesUncommitted int

	deliver    chan *stateReq // Delivery channel multiplexing peer responses
	cancel     chan struct{}  // Channel to signal a termination request
	cancelOnce sync.Once      // Ensures cancel only ever gets called once
	done       chan struct{}  // Channel to signal termination completion
	err        error          // Any error hit during sync (set before completion)
}

// stateTask represents a single trie node download task, containing a set of
// peers already attempted retrieval from to detect stalled syncs and abort.
// stateTask表示单个节点下载任务，包含一组
// 对等体已经尝试检索以检测停滞的同步并中止
type stateTask struct {
	attempts map[string]struct{}
}

// newStateSync creates a new state trie download scheduler. This method does not
// yet start the sync. The user needs to call run to initiate.
func newStateSync(d *Downloader, root common.Hash) *stateSync {
	return &stateSync{
		d:       d,
		sched:   state.NewStateSync(root, d.stateDB),
		keccak:  sha3.NewKeccak256(),
		tasks:   make(map[common.Hash]*stateTask),
		deliver: make(chan *stateReq),
		cancel:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// run starts the task assignment and response processing loop, blocking until
// it finishes, and finally notifying any goroutines waiting for the loop to
// finish.
func (s *stateSync) run() {
	s.err = s.loop()
	close(s.done)
}

// Wait blocks until the sync is done or canceled.
func (s *stateSync) Wait() error {
	<-s.done
	return s.err
}

// Cancel cancels the sync and waits until it has shut down.
func (s *stateSync) Cancel() error {
	s.cancelOnce.Do(func() { close(s.cancel) })
	return s.Wait()
}

// loop is the main event loop of a state trie sync. It it responsible for the
// assignment of new tasks to peers (including sending it to them) as well as
// for the processing of inbound data. Note, that the loop does not directly
// receive data from peers, rather those are buffered up in the downloader and
// pushed here async. The reason is to decouple processing from data receipt
// and timeouts.
//loop是状态trie sync的主要事件循环。 它负责
//将新任务分配给对等方（包括将其发送给它们）以及
//用于处理入站数据。 注意，循环不直接
//从同行接收数据，而不是在下载器中缓存的数据
//推送到这里异步。 原因是将处理与数据接收分离
//和超时
func (s *stateSync) loop() (err error) {
	// Listen for new peer events to assign tasks to them
	newPeer := make(chan *peerConnection, 1024)
	peerSub := s.d.peers.SubscribeNewPeers(newPeer)
	defer peerSub.Unsubscribe()
	defer func() {
		cerr := s.commit(true)
		if err == nil {
			err = cerr
		}
	}()

	// Keep assigning new tasks until the sync completes or aborts
	// 这里就是只要还有待请求的任务就继续执行，直到请求完所有valueNode
	// 因为请求到最后的valueNode时，以及没有请求任务了，但是value只存储在内存中还没有存入磁盘，所以在defer里面会最后再提交一次
	for s.sched.Pending() > 0 {
		// 这里才是真正的提交，通过过来的State数据，会写入到db中
		if err = s.commit(false); err != nil {
			return err
		}
		// 这里就是分配任务
		s.assignTasks()
		// Tasks assigned, wait for something to happen
		select {
		case <-newPeer:
			// New peer arrived, try to assign it download tasks

		case <-s.cancel:
			return errCancelStateFetch

		case <-s.d.cancelCh:
			return errCancelStateFetch

		case req := <-s.deliver:
			// 接收请求完成的数据，从runStateSync发送过来的
			// Response, disconnect or timeout triggered, drop the peer if stalling
			log.Trace("Received node data response", "peer", req.peer.id, "count", len(req.response), "dropped", req.dropped, "timeout", !req.dropped && req.timedOut())
			// 这里是最低要两项item
			if len(req.items) <= 2 && !req.dropped && req.timedOut() {
				// 2 items are the minimum requested, if even that times out, we've no use of
				// this peer at the moment.
				log.Warn("Stalling state sync, dropping peer", "peer", req.peer.id)
				s.d.dropPeer(req.peer.id)
			}
			// Process all the received blobs and check for stale delivery
			// 处理所有收到的blob并检查陈旧的交付
			if err = s.process(req); err != nil {
				log.Warn("Node data write error", "err", err)
				return err
			}
			// 这里..
			req.peer.SetNodeDataIdle(len(req.response))
		}
	}
	return nil
}

func (s *stateSync) commit(force bool) error {
	if !force && s.bytesUncommitted < ethdb.IdealBatchSize {
		return nil
	}
	start := time.Now()
	b := s.d.stateDB.NewBatch()
	if written, err := s.sched.Commit(b); written == 0 || err != nil {
		return err
	}
	if err := b.Write(); err != nil {
		return fmt.Errorf("DB write error: %v", err)
	}
	s.updateStats(s.numUncommitted, 0, 0, time.Since(start))
	s.numUncommitted = 0
	s.bytesUncommitted = 0
	return nil
}

// assignTasks attempts to assign new tasks to all idle peers, either from the
// batch currently being retried, or fetching new data from the trie sync itself.
// assignTasks尝试将新任务分配给所有空闲对等体，或者从
// 当前正在重试批处理，或从trie同步本身获取新数据
func (s *stateSync) assignTasks() {
	// Iterate over all idle peers and try to assign them state fetches
	// 迭代所有空闲对等体并尝试为它们分配状态提取
	// 所有没有正在同步state数据的节点
	peers, _ := s.d.peers.NodeDataIdlePeers()
	// 这里就是向多个节点去请求数据
	// 而且是把所有等待请求的数据按照每个节点的情况去分配给每个节点不同量的任务
	// 所有数据是同时向多个节点去获取的
	for _, p := range peers {
		// Assign a batch of fetches proportional to the estimated latency/bandwidth
		// 分配与估计的延迟/带宽成比例的一批提取
		cap := p.NodeDataCapacity(s.d.requestRTT())
		req := &stateReq{peer: p, timeout: s.d.requestTTL()}
		// 这里就是构建要请求的任务，把要获取的数据hash构建好到req对象里面
		s.fillTasks(cap, req)

		// If the peer was assigned tasks to fetch, send the network request
		// 如果为对等体分配了要获取的任务，则发送网络请求
		if len(req.items) > 0 {
			req.peer.log.Trace("Requesting new batch of data", "type", "state", "count", len(req.items))
			select {
			case s.d.trackStateReq <- req:
				req.peer.FetchNodeData(req.items)
			case <-s.cancel:
			case <-s.d.cancelCh:
			}
		}
	}
}

// fillTasks fills the given request object with a maximum of n state download
// tasks to send to the remote peer.
func (s *stateSync) fillTasks(n int, req *stateReq) {
	// Refill available tasks from the scheduler.
	// 从调度程序重新填充可用任务。
	// 任务没到上线就填充满
	if len(s.tasks) < n {
		new := s.sched.Missing(n - len(s.tasks))
		for _, hash := range new {
			s.tasks[hash] = &stateTask{make(map[string]struct{})}
		}
	}
	// Find tasks that haven't been tried with the request's peer.
	// 查找未使用请求的对等方尝试过的任务。
	req.items = make([]common.Hash, 0, n)
	req.tasks = make(map[common.Hash]*stateTask, n)
	for hash, t := range s.tasks {
		// Stop when we've gathered enough requests
		// 当我们收集到足够的请求时停止
		if len(req.items) == n {
			break
		}
		// Skip any requests we've already tried from this peer
		// 如果这个任务已经分配过这个节点了就跳过
		if _, ok := t.attempts[req.peer.id]; ok {
			continue
		}
		// Assign the request to this peer
		// 将请求分配给此对等方
		t.attempts[req.peer.id] = struct{}{}
		req.items = append(req.items, hash)
		req.tasks[hash] = t
		delete(s.tasks, hash)
	}
}

// process iterates over a batch of delivered state data, injecting each item
// into a running state sync, re-queuing any items that were requested but not
// delivered.
func (s *stateSync) process(req *stateReq) error {
	// Collect processing stats and update progress if valid data was received
	duplicate, unexpected := 0, 0

	defer func(start time.Time) {
		if duplicate > 0 || unexpected > 0 {
			s.updateStats(0, duplicate, unexpected, time.Since(start))
		}
	}(time.Now())

	// Iterate over all the delivered data and inject one-by-one into the trie
	// 开始循环把所有接收到的数据存储trie中，实际上是先存储到缓存中，然后loop()函数会检测是否有需要提交的数据
	progress := false

	for _, blob := range req.response {
		prog, hash, err := s.processNodeData(blob)
		switch err {
		case nil:
			s.numUncommitted++
			s.bytesUncommitted += len(blob)
			progress = progress || prog
		case trie.ErrNotRequested:
			unexpected++
		case trie.ErrAlreadyProcessed:
			duplicate++
		default:
			return fmt.Errorf("invalid state node %s: %v", hash.TerminalString(), err)
		}
		if _, ok := req.tasks[hash]; ok {
			delete(req.tasks, hash)
		}
	}
	// Put unfulfilled tasks back into the retry queue
	npeers := s.d.peers.Len()
	for hash, task := range req.tasks {
		// If the node did deliver something, missing items may be due to a protocol
		// limit or a previous timeout + delayed delivery. Both cases should permit
		// the node to retry the missing items (to avoid single-peer stalls).
		if len(req.response) > 0 || req.timedOut() {
			delete(task.attempts, req.peer.id)
		}
		// If we've requested the node too many times already, it may be a malicious
		// sync where nobody has the right data. Abort.
		if len(task.attempts) >= npeers {
			return fmt.Errorf("state node %s failed with all peers (%d tries, %d peers)", hash.TerminalString(), len(task.attempts), npeers)
		}
		// Missing item, place into the retry queue.
		s.tasks[hash] = task
	}
	return nil
}

// processNodeData tries to inject a trie node data blob delivered from a remote
// peer into the state trie, returning whether anything useful was written or any
// error occurred.
func (s *stateSync) processNodeData(blob []byte) (bool, common.Hash, error) {
	res := trie.SyncResult{Data: blob}
	s.keccak.Reset()
	s.keccak.Write(blob)
	s.keccak.Sum(res.Hash[:0])
	committed, _, err := s.sched.Process([]trie.SyncResult{res})
	return committed, res.Hash, err
}

// updateStats bumps the various state sync progress counters and displays a log
// message for the user to see.
func (s *stateSync) updateStats(written, duplicate, unexpected int, duration time.Duration) {
	s.d.syncStatsLock.Lock()
	defer s.d.syncStatsLock.Unlock()

	s.d.syncStatsState.pending = uint64(s.sched.Pending())
	s.d.syncStatsState.processed += uint64(written)
	s.d.syncStatsState.duplicate += uint64(duplicate)
	s.d.syncStatsState.unexpected += uint64(unexpected)

	if written > 0 || duplicate > 0 || unexpected > 0 {
		log.Info("Imported new state entries", "count", written, "elapsed", common.PrettyDuration(duration), "processed", s.d.syncStatsState.processed, "pending", s.d.syncStatsState.pending, "retry", len(s.tasks), "duplicate", s.d.syncStatsState.duplicate, "unexpected", s.d.syncStatsState.unexpected)
	}
	if written > 0 {
		rawdb.WriteFastTrieProgress(s.d.stateDB, s.d.syncStatsState.processed)
	}
}
