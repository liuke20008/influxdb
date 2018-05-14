package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/tsdb"
)

func TestMergeTagKeys(t *testing.T) {
	tests := []struct {
		name string
		keys []tsdb.TagKeys
		exp  []string
	}{
		{name: "empty"},
		{
			name: "len01",
			keys: []tsdb.TagKeys{
				{
					Measurement: "maaa",
					Keys:        []string{"aaa", "bbb", "ccc"},
				},
			},
			exp: []string{"aaa", "bbb", "ccc"},
		},
		{
			name: "len03 dupes|☐",
			keys: []tsdb.TagKeys{
				{
					Measurement: "maaa",
					Keys:        []string{"aaa", "bbb", "ddd"},
				},
				{
					Measurement: "mbbb",
					Keys:        []string{"ccc", "eee", "fff"},
				},
				{
					Measurement: "mc",
					Keys:        []string{"zzz"},
				},
			},
			exp: []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "zzz"},
		},
		{
			name: "len03 dupes|☑︎",
			keys: []tsdb.TagKeys{
				{
					Measurement: "maaa",
					Keys:        []string{"aaa", "bbb"},
				},
				{
					Measurement: "mbbb",
					Keys:        []string{"bbb", "eee", "fff"},
				},
				{
					Measurement: "mccc",
					Keys:        []string{"ccc", "ddd", "fff", "ggg"},
				},
			},
			exp: []string{"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MergeTagKeys(tt.keys); !cmp.Equal(got, tt.exp) {
				t.Errorf("-got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}
