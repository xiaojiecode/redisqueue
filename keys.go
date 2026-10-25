package redisqueue

import "strconv"

type keys struct {
	receipts   string
	readyZ     string
	readyList  string
	working    string
	processing string
	data       string
	priority   string
	attempts   string
	enqueued   string
	updated    string
	dead       string
	deadZ      string
	deadReason string
	deadAt     string
	concTotal  string
	concSvc    string
}

func newKeys(base string) keys {
	return keys{
		receipts:   base + "receipts",
		readyZ:     base + "ready:priorities",
		readyList:  base + "ready:list:",
		working:    base + "working",
		processing: base + "processing",
		data:       base + "data",
		priority:   base + "priority",
		attempts:   base + "attempts",
		enqueued:   base + "enqueued",
		updated:    base + "updated",
		dead:       base + "dead",
		deadZ:      base + "dead:index",
		deadReason: base + "dead:reason",
		deadAt:     base + "dead:at",
		concTotal:  base + "concurrency:total",
		concSvc:    base + "concurrency:service:",
	}
}

func (k keys) readyListKey(priority int64) string {
	return k.readyList + strconv.FormatInt(priority, 10)
}

func (k keys) serviceConcurrencyKey(serviceName string) string {
	return k.concSvc + serviceName
}
