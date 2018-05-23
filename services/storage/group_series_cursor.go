package storage

import (
	"bytes"
	"context"
	"sort"
	"strings"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/opentracing/opentracing-go"
)

type groupResultSet struct {
	ctx context.Context
	cur *indexSeriesCursor

	i    int
	rows []sortRow
	keys [][]byte
	gc   singleGroupCursor
	km   keyMerger
}

func newGroupResultSet(ctx context.Context, req readRequest, cur *indexSeriesCursor, keys []string) *groupResultSet {
	g := &groupResultSet{
		cur:  cur,
		ctx:  ctx,
		keys: make([][]byte, len(keys)),
		gc: singleGroupCursor{
			req: req,
		},
	}

	for i, k := range keys {
		g.keys[i] = []byte(k)
	}

	g.sort()

	return g
}
func (c *groupResultSet) Close() {
	c.cur.Close()
}

func (c *groupResultSet) Next() *singleGroupCursor {
	if c.i < len(c.rows) {
		c.nextGroup()
		return &c.gc
	}
	return nil
}

func (c *groupResultSet) nextGroup() {
	c.km.setTags(c.rows[c.i].row.tags)

	rowKey := c.rows[c.i].key
	j := c.i + 1
	for j < len(c.rows) && bytes.Equal(rowKey, c.rows[j].key) {
		c.km.mergeTagKeys(c.rows[j].row.tags)
		j++
	}

	c.gc.reset(c.rows[c.i:j])
	c.gc.keys = c.km.get()

	c.i = j
}

type sortRow struct {
	row *seriesRow
	key []byte
}

var nilKey = [...]byte{0xff}

func (c *groupResultSet) sort() {
	span := opentracing.SpanFromContext(c.ctx)
	if span != nil {
		span = opentracing.StartSpan("group_series_cursor.sort", opentracing.ChildOf(span.Context()))
		defer span.Finish()
	}

	var rows []sortRow
	vals := make([][]byte, len(c.keys))
	tagsBuf := &tagsBuffer{sz: 4096}

	row := c.cur.Next()
	for row != nil {
		nr := *row
		nr.stags = tagsBuf.copyTags(nr.stags)
		nr.tags = tagsBuf.copyTags(nr.tags)

		l := 0
		for i, k := range c.keys {
			vals[i] = nr.tags.Get(k)
			if len(vals[i]) == 0 {
				vals[i] = nilKey[:] // if there was no value, ensure it sorts last
			}
			l += len(vals[i])
		}

		rows = append(rows, sortRow{row: &nr})
		p := len(rows) - 1
		rows[p].key = make([]byte, 0, l)
		for _, v := range vals {
			rows[p].key = append(rows[p].key, v...)
		}

		row = c.cur.Next()
	}

	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].key, rows[j].key) == -1
	})

	if span != nil {
		span.SetTag("rows", len(rows))
	}

	c.rows = rows

	// free early
	c.cur.Close()
}

type singleGroupCursor struct {
	req  readRequest
	i    int
	rows []sortRow
	keys [][]byte
}

func (c *singleGroupCursor) reset(rows []sortRow) {
	c.i = 0
	c.rows = rows
}

func (c *singleGroupCursor) Keys() [][]byte    { return c.keys }
func (c *singleGroupCursor) Tags() models.Tags { return c.rows[c.i-1].row.tags }

func (c *singleGroupCursor) Next() bool {
	if c.i < len(c.rows) {
		c.i++
		return true
	}
	return false
}

func (c *singleGroupCursor) Cursor() tsdb.Cursor {
	cur := newMultiShardBatchCursor(c.req.ctx, *c.rows[c.i-1].row, &c.req)
	if c.req.aggregate != nil {
		cur = newAggregateBatchCursor(c.req.ctx, c.req.aggregate, cur)
	}
	return cur
}

// keyMerger is responsible for determining a merged set of tag keys
type keyMerger struct {
	i    int
	keys [2][][]byte
}

func (km *keyMerger) setTags(tags models.Tags) {
	km.i = 0
	if cap(km.keys[0]) < len(tags) {
		km.keys[0] = make([][]byte, len(tags))
	} else {
		km.keys[0] = km.keys[0][:len(tags)]
	}
	for i := range tags {
		km.keys[0][i] = tags[i].Key
	}
}

func (km *keyMerger) get() [][]byte { return km.keys[km.i&1] }

func (km *keyMerger) String() string {
	var s []string
	for _, k := range km.get() {
		s = append(s, string(k))
	}
	return strings.Join(s, ",")
}

func (km *keyMerger) mergeTagKeys(tags models.Tags) {
	keys := km.keys[km.i&1]
	i, j := 0, 0
	for i < len(keys) && j < len(tags) && bytes.Compare(keys[i], tags[j].Key) == 0 {
		i++
		j++
	}

	if j == len(tags) {
		// no new tags
		return
	}

	km.i = (km.i + 1) & 1
	l := len(keys) + len(tags)
	if cap(km.keys[km.i]) < l {
		km.keys[km.i] = make([][]byte, l)
	} else {
		km.keys[km.i] = km.keys[km.i][:l]
	}

	keya := km.keys[km.i]

	// back up the pointers
	if i > 0 {
		i--
		j--
	}

	k := i
	copy(keya[:k], keys[:k])

	for i < len(keys) && j < len(tags) {
		cmp := bytes.Compare(keys[i], tags[j].Key)
		if cmp < 0 {
			keya[k] = keys[i]
			i++
		} else if cmp > 0 {
			keya[k] = tags[j].Key
			j++
		} else {
			keya[k] = keys[i]
			i++
			j++
		}
		k++
	}

	if i < len(keys) {
		k += copy(keya[k:], keys[i:])
	}

	for j < len(tags) {
		keya[k] = tags[j].Key
		j++
		k++
	}

	km.keys[km.i] = keya[:k]
}
