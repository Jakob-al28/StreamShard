package partition

import (
	"time"

	"github.com/jakob-al28/StreamShard/internal/aggregate"
	"github.com/jakob-al28/StreamShard/internal/log"
)

const compactInterval = 5 * time.Minute
const compactThreshold = 10000

type ApplyResult struct {
	Entry log.Entry
	Fresh bool
}

type applyCmd struct {
	id, key string
	value   []byte
	reply   chan ApplyResult
}

type queryCmd struct {
	reply chan QueryResult
}

type logCmd struct {
	offset uint64
	reply  chan []log.Entry
}

type baseCmd struct {
	reply chan uint64
}

type compactCmd struct{}

type QueryResult struct {
	Counts map[string]uint64
	TopK   []aggregate.TopEntry
}

type Partition struct {
	apply    chan applyCmd
	query    chan queryCmd
	logs     chan logCmd
	base     chan baseCmd
	compact  chan compactCmd
	applyCap int
}

func New(window time.Duration, topK int, queueCap int) *Partition {
	return Open("", window, topK, queueCap, false, 1)
}

// Open starts a partition. batchMax > 1 enables WAL write batching: the apply loop
// drains up to batchMax queued writes and persists them with one marshal+write pass
func Open(dataDir string, window time.Duration, topK int, queueCap int, noIdempotent bool, batchMax int) *Partition {
	if batchMax < 1 {
		batchMax = 1
	}
	p := &Partition{
		apply:    make(chan applyCmd, queueCap),
		query:    make(chan queryCmd, 64),
		logs:     make(chan logCmd, 64),
		base:     make(chan baseCmd, 64),
		compact:  make(chan compactCmd, 1),
		applyCap: queueCap,
	}
	go p.run(dataDir, window, topK, noIdempotent, batchMax)
	if dataDir != "" {
		go p.compactor()
	}
	return p
}

func (p *Partition) compactor() {
	t := time.NewTicker(compactInterval)
	defer t.Stop()
	for range t.C {
		select {
		case p.compact <- compactCmd{}:
		default:
		}
	}
}

func (p *Partition) run(dataDir string, window time.Duration, topK int, noIdempotent bool, batchMax int) {
	l, err := log.Open(dataDir)
	if err != nil {
		panic(err)
	}
	l.SetNoIdempotent(noIdempotent)
	agg := aggregate.New(window, topK)

	snap, _ := log.LoadSnapshot(dataDir)
	if snap != nil {
		agg.LoadSnapshot(*snap)
	}
	for _, e := range l.Entries() {
		agg.Apply(e)
	}

	for {
		select {
		case cmd := <-p.apply:
			if batchMax > 1 {
				p.applyBatch(l, agg, cmd, batchMax)
			} else {
				e, fresh := l.Append(cmd.id, cmd.key, cmd.value)
				if fresh {
					agg.Apply(e)
				}
				cmd.reply <- ApplyResult{Entry: e, Fresh: fresh}
			}

		case cmd := <-p.query:
			cmd.reply <- QueryResult{
				Counts: agg.Counts(),
				TopK:   agg.TopK(),
			}

		case cmd := <-p.logs:
			cmd.reply <- l.Since(cmd.offset)

		case cmd := <-p.base:
			cmd.reply <- l.BaseOffset()

		case <-p.compact:
			if uint64(len(l.Entries())) >= compactThreshold {
				snap := agg.Snapshot(l.Head())
				l.Compact(snap)
			}
		}
	}
}

// applyBatch processes first plus up to batchMax-1 already-queued apply commands as
// a single batched WAL write, then replies to each in order. Draining is non-blocking,
// so a batch is only as large as what is already waiting — latency is never increased
// to wait for a batch to fill.
func (p *Partition) applyBatch(l *log.Log, agg *aggregate.Aggregate, first applyCmd, batchMax int) {
	cmds := make([]applyCmd, 0, batchMax)
	cmds = append(cmds, first)
	for len(cmds) < batchMax {
		select {
		case c := <-p.apply:
			cmds = append(cmds, c)
		default:
			goto drained
		}
	}
drained:
	ids := make([]string, len(cmds))
	keys := make([]string, len(cmds))
	values := make([][]byte, len(cmds))
	for i, c := range cmds {
		ids[i], keys[i], values[i] = c.id, c.key, c.value
	}
	entries, fresh := l.AppendBatch(ids, keys, values)
	for i, c := range cmds {
		if fresh[i] {
			agg.Apply(entries[i])
		}
		c.reply <- ApplyResult{Entry: entries[i], Fresh: fresh[i]}
	}
}

func (p *Partition) QueueDepth() int {
	return len(p.apply)
}

func (p *Partition) QueueCap() int {
	return p.applyCap
}

func (p *Partition) Apply(id, key string, value []byte) ApplyResult {
	reply := make(chan ApplyResult, 1)
	p.apply <- applyCmd{id: id, key: key, value: value, reply: reply}
	return <-reply
}

func (p *Partition) Query() QueryResult {
	reply := make(chan QueryResult, 1)
	p.query <- queryCmd{reply: reply}
	return <-reply
}

func (p *Partition) LogSince(offset uint64) []log.Entry {
	reply := make(chan []log.Entry, 1)
	p.logs <- logCmd{offset: offset, reply: reply}
	return <-reply
}

func (p *Partition) BaseOffset() uint64 {
	reply := make(chan uint64, 1)
	p.base <- baseCmd{reply: reply}
	return <-reply
}
