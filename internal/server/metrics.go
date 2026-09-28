package server

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/MonishPuttu/Key-value-store/internal/cluster"
)

// collector exports Raft and engine state at scrape time.
type collector struct {
	s         *cluster.Store
	isLeader  *prometheus.Desc
	term      *prometheus.Desc
	commit    *prometheus.Desc
	applied   *prometheus.Desc
	nodes     *prometheus.Desc
	lvlFiles  *prometheus.Desc
	lvlBytes  *prometheus.Desc
	counters  *prometheus.Desc
	writeAmp  *prometheus.Desc
	memtable  *prometheus.Desc
	immutable *prometheus.Desc
}

func newCollector(s *cluster.Store) *collector {
	d := func(name, help string, labels ...string) *prometheus.Desc {
		return prometheus.NewDesc(name, help, labels, nil)
	}
	return &collector{
		s:         s,
		isLeader:  d("kv_raft_is_leader", "1 if this node is the Raft leader."),
		term:      d("kv_raft_term", "Current Raft term."),
		commit:    d("kv_raft_commit_index", "Raft commit index."),
		applied:   d("kv_raft_applied_index", "Index applied to the state machine."),
		nodes:     d("kv_raft_nodes", "Number of cluster members."),
		lvlFiles:  d("kv_engine_level_files", "SSTables per level.", "level"),
		lvlBytes:  d("kv_engine_level_bytes", "Bytes per level.", "level"),
		counters:  d("kv_engine_events_total", "Engine event counters.", "event"),
		writeAmp:  d("kv_engine_write_amplification", "Disk bytes written per user byte."),
		memtable:  d("kv_engine_memtable_bytes", "Active memtable size."),
		immutable: d("kv_engine_immutable_memtables", "Memtables waiting to be flushed."),
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.isLeader, c.term, c.commit, c.applied, c.nodes, c.lvlFiles,
		c.lvlBytes, c.counters, c.writeAmp, c.memtable, c.immutable} {
		ch <- d
	}
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	g := func(d *prometheus.Desc, v float64, l ...string) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, l...)
	}
	cnt := func(v int64, event string) {
		ch <- prometheus.MustNewConstMetric(c.counters, prometheus.CounterValue, float64(v), event)
	}
	st := c.s.Status()
	leader := 0.0
	if st.State == "Leader" {
		leader = 1
	}
	g(c.isLeader, leader)
	term, _ := strconv.ParseFloat(st.Term, 64)
	g(c.term, term)
	g(c.commit, float64(st.CommitIndex))
	g(c.applied, float64(st.AppliedIndex))
	g(c.nodes, float64(len(st.Nodes)))

	m := c.s.EngineMetrics()
	for _, l := range m.Levels {
		lvl := strconv.Itoa(l.Level)
		g(c.lvlFiles, float64(l.Files), lvl)
		g(c.lvlBytes, float64(l.Bytes), lvl)
	}
	g(c.writeAmp, m.WriteAmplification)
	g(c.memtable, float64(m.MemtableBytes))
	g(c.immutable, float64(m.ImmutableMemtables))
	cnt(m.Puts, "put")
	cnt(m.Deletes, "delete")
	cnt(m.Gets, "get")
	cnt(m.GetHits, "get_hit")
	cnt(m.Flushes, "flush")
	cnt(m.Compactions, "compaction")
	cnt(m.TrivialMoves, "trivial_move")
	cnt(m.WriteStalls, "write_stall")
	cnt(m.BlockReads, "block_read")
	cnt(m.CacheHits, "cache_hit")
	cnt(m.CacheMisses, "cache_miss")
	cnt(m.BloomNegatives, "bloom_negative")
	cnt(m.BloomFalsePositives, "bloom_false_positive")
}
