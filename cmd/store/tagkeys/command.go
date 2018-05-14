package tagkeys

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxql"
	"github.com/influxdata/yarpc"
	"go.uber.org/zap"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	addr            string
	database        string
	retentionPolicy string
	startTime       int64
	endTime         int64
	silent          bool
	expr            string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func parseTime(v string) (int64, error) {
	if s, err := time.Parse(time.RFC3339, v); err == nil {
		return s.UnixNano(), nil
	}

	if i, err := strconv.ParseInt(v, 10, 64); err == nil {
		return i, nil
	}

	return 0, errors.New("invalid time")
}

// Run executes the command.
func (cmd *Command) Run(args ...string) error {
	var start, end string
	fs := flag.NewFlagSet("tag-keys", flag.ExitOnError)
	fs.StringVar(&cmd.addr, "addr", ":8082", "the RPC address")
	fs.StringVar(&cmd.database, "database", "", "the database to query")
	fs.StringVar(&cmd.retentionPolicy, "retention", "", "Optional: the retention policy to query")
	fs.StringVar(&start, "start", "", "Optional: the start time to query (RFC3339 format)")
	fs.StringVar(&end, "end", "", "Optional: the end time to query (RFC3339 format)")
	fs.BoolVar(&cmd.silent, "silent", false, "silence output")
	fs.StringVar(&cmd.expr, "expr", "", "InfluxQL conditional expression")

	fs.SetOutput(cmd.Stdout)
	fs.Usage = func() {
		fmt.Fprintln(cmd.Stdout, "List tag keys via RPC")
		fmt.Fprintf(cmd.Stdout, "Usage: %s tag-keys [flags]\n\n", filepath.Base(os.Args[0]))
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return err
	}

	// set defaults
	if start != "" {
		t, err := parseTime(start)
		if err != nil {
			return err
		}
		cmd.startTime = t

	} else {
		cmd.startTime = models.MinNanoTime
	}
	if end != "" {
		t, err := parseTime(end)
		if err != nil {
			return err
		}
		cmd.endTime = t

	} else {
		// set end time to max if it is not set.
		cmd.endTime = models.MaxNanoTime
	}

	if err := cmd.validate(); err != nil {
		return err
	}

	conn, err := yarpc.Dial(cmd.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	return cmd.query(storage.NewStorageClient(conn))
}

func (cmd *Command) validate() error {
	if cmd.database == "" {
		return fmt.Errorf("must specify a database")
	}
	if cmd.startTime != 0 && cmd.endTime != 0 && cmd.endTime < cmd.startTime {
		return fmt.Errorf("end time before start time")
	}
	return nil
}

func (cmd *Command) query(c storage.StorageClient) error {
	var req storage.ReadTagKeysRequest
	req.Database = cmd.database
	if cmd.retentionPolicy != "" {
		req.Database += "/" + cmd.retentionPolicy
	}

	req.TimestampRange.Start = cmd.startTime
	req.TimestampRange.End = cmd.endTime

	//if cmd.expr != "" {
	//	expr, err := influxql.ParseExpr(cmd.expr)
	//	if err != nil {
	//		return nil
	//	}
	//	fmt.Fprintln(cmd.Stdout, expr)
	//	var v exprToNodeVisitor
	//	influxql.Walk(&v, expr)
	//	if v.Err() != nil {
	//		return v.Err()
	//	}
	//
	//	req.Predicate = &storage.Predicate{Root: v.nodes[0]}
	//}

	stream, err := c.ReadTagKeys(context.Background(), &req)
	if err != nil {
		fmt.Fprintln(cmd.Stdout, err)
		return err
	}

	wr := bufio.NewWriter(os.Stdout)

	now := time.Now()
	defer func() {
		dur := time.Since(now)
		fmt.Fprintf(cmd.Stdout, "time: %v\n", dur)
	}()

	var count int
	for {
		var res storage.ReadTagKeysResponse
		if err = stream.RecvMsg(&res); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		count += len(res.Keys)
		if !cmd.silent {
			for i := range res.Keys {
				wr.WriteString("\033[36m")
				wr.WriteString(res.Keys[i])
				wr.WriteString("\033[0m\n")
			}
		}
	}

	wr.Flush()

	fmt.Fprintln(cmd.Stdout)
	fmt.Fprint(cmd.Stdout, "count: ", count, "\n")

	return nil
}

type exprToNodeVisitor struct {
	nodes []*storage.Node
	err   error
}

func (v *exprToNodeVisitor) Err() error {
	return v.err
}

func (v *exprToNodeVisitor) pop() (top *storage.Node) {
	if len(v.nodes) < 1 {
		panic("exprToNodeVisitor: stack empty")
	}

	top, v.nodes = v.nodes[len(v.nodes)-1], v.nodes[:len(v.nodes)-1]
	return
}

func (v *exprToNodeVisitor) pop2() (lhs, rhs *storage.Node) {
	if len(v.nodes) < 2 {
		panic("exprToNodeVisitor: stack empty")
	}

	rhs = v.nodes[len(v.nodes)-1]
	lhs = v.nodes[len(v.nodes)-2]
	v.nodes = v.nodes[:len(v.nodes)-2]
	return
}

func mapOpToComparison(op influxql.Token) storage.Node_Comparison {
	switch op {
	case influxql.EQ:
		return storage.ComparisonEqual
	case influxql.NEQ:
		return storage.ComparisonNotEqual
	case influxql.LT:
		return storage.ComparisonLess
	case influxql.LTE:
		return storage.ComparisonLessEqual
	case influxql.GT:
		return storage.ComparisonGreater
	case influxql.GTE:
		return storage.ComparisonGreaterEqual

	default:
		return -1
	}
}

func (v *exprToNodeVisitor) Visit(node influxql.Node) influxql.Visitor {
	switch n := node.(type) {
	case *influxql.BinaryExpr:
		if v.err != nil {
			return nil
		}

		influxql.Walk(v, n.LHS)
		if v.err != nil {
			return nil
		}

		influxql.Walk(v, n.RHS)
		if v.err != nil {
			return nil
		}

		if comp := mapOpToComparison(n.Op); comp != -1 {
			lhs, rhs := v.pop2()
			v.nodes = append(v.nodes, &storage.Node{
				NodeType: storage.NodeTypeComparisonExpression,
				Value:    &storage.Node_Comparison_{Comparison: comp},
				Children: []*storage.Node{lhs, rhs},
			})
		} else if n.Op == influxql.AND || n.Op == influxql.OR {
			var op storage.Node_Logical
			if n.Op == influxql.AND {
				op = storage.LogicalAnd
			} else {
				op = storage.LogicalOr
			}

			lhs, rhs := v.pop2()
			v.nodes = append(v.nodes, &storage.Node{
				NodeType: storage.NodeTypeLogicalExpression,
				Value:    &storage.Node_Logical_{Logical: op},
				Children: []*storage.Node{lhs, rhs},
			})
		} else {
			v.err = fmt.Errorf("unsupported operator, %s", n.Op)
		}

		return nil

	case *influxql.ParenExpr:
		influxql.Walk(v, n.Expr)
		if v.err != nil {
			return nil
		}

		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeParenExpression,
			Children: []*storage.Node{v.pop()},
		})
		return nil

	case *influxql.StringLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_StringValue{StringValue: n.Val},
		})
		return nil

	case *influxql.NumberLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_FloatValue{FloatValue: n.Val},
		})
		return nil

	case *influxql.IntegerLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_IntegerValue{IntegerValue: n.Val},
		})
		return nil

	case *influxql.UnsignedLiteral:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeLiteral,
			Value:    &storage.Node_UnsignedValue{UnsignedValue: n.Val},
		})
		return nil

	case *influxql.VarRef:
		v.nodes = append(v.nodes, &storage.Node{
			NodeType: storage.NodeTypeTagRef,
			Value:    &storage.Node_TagRefValue{TagRefValue: n.Val},
		})
		return nil

	default:
		v.err = errors.New("unsupported expression")
		return nil
	}
}
