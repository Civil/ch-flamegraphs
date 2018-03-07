package helper

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/Civil/ch-flamegraphs/types"
)

func ReconstructTree(data map[int64]types.ClickhouseField, root *types.FlameGraphNode, minValue int64) {
	for _, i := range root.ChildrenIds {
		if data[i].Value > minValue {
			node := &types.FlameGraphNode{
				Id:          data[i].Id,
				Cluster:     data[i].Cluster,
				Name:        data[i].Name,
				Value:       data[i].Value,
				Total:       data[i].Total,
				Parent:      root,
				ChildrenIds: data[i].ChildrenIds,
			}
			ReconstructTree(data, node, minValue)
			root.Children = append(root.Children, node)
		}
	}
}

type Query struct {
	application string
	ts          string
	tsInt       int64
	instance    string
	samples     string
	date        string
	samplesInt  int64
	minValue    int64
	db          *sql.DB

	nameToNode map[string]*types.FlameGraphNode
}

func NewQuery(db *sql.DB, ts, application, instance, samples string, minValue int64) (*Query, error) {
	samplesInt, err := strconv.ParseInt(samples, 10, 64)
	if err != nil {
		samples = "1"
		samplesInt = 1
	}

	tsInt, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return nil, err
	}

	t := time.Unix(tsInt, 0)
	date := t.Format("2006-01-02")
	if tsInt == 0 {
		date = time.Now().Format("2006-01-02")
	}
	return &Query{
		db:          db,
		ts:          ts,
		tsInt:       tsInt,
		application: application,
		instance:    instance,
		samples:     samples,
		samplesInt:  samplesInt,
		date:        date,
	}, nil
}

func (q *Query) GetStackFlamegraph(showFileNames bool) (*types.FlameGraphNode, error) {
	if err := q.db.Ping(); err != nil {
		return nil, err
	}

	clause := "SELECT max(Timestamp), ID, FunctionName, FileName, IsRoot, max(Line), sum(Samples), sum(MaxSamples), groupUniqArrayArray(ChildrenIDs) from stacktrace WHERE"
	group := " Group By ID, FunctionName, FileName, IsRoot Order By FunctionName"
	where := " AND Application='" + q.application + "' AND Date='" + q.date + "'"
	if q.tsInt == 0 {
		where = " Timestamp IN ( SELECT Timestamp from stacktraceTimestamps where Application='" + q.application + "' order by Timestamp desc limit " + q.samples + ") " + where
	} else {
		where = " Timestamp IN ( SELECT Timestamp from stacktraceTimestamps where Application='" + q.application + "' and Timestamp <= " + q.ts + " order by Timestamp desc limit " + q.samples + ") " + where
	}
	// TODO: Add validation
	if q.instance != "" {
		where = where + " AND Instance='" + q.instance + "'"
	}
	where += group

	rows, err := q.db.Query(clause + where)
	if err != nil {
		return nil, err
	}

	data := make(map[int64]types.ClickhouseField)
	var functionName, fileName string
	var line int64
	var isRoot uint8
	rootId := int64(0)

	for rows.Next() {
		var res types.ClickhouseField

		err := rows.Scan(&res.Timestamp, &res.Id, &functionName, &fileName, &isRoot, &line, &res.Value, &res.Total, &res.ChildrenIds)
		if err != nil {
			return nil, err
		}
		if isRoot != 0 {
			rootId = res.Id
		}
		if isRoot == 0 && showFileNames {
			res.Name = fmt.Sprintf("%v: %v:%v", functionName, fileName, line)
		} else {
			res.Name = functionName
		}
		data[res.Id] = res
	}

	flameGraphTreeRoot := &types.FlameGraphNode{
		Id:          data[rootId].Id,
		Cluster:     data[rootId].Cluster,
		Name:        data[rootId].Name,
		Value:       data[rootId].Value,
		Total:       data[rootId].Total,
		Parent:      nil,
		ChildrenIds: data[rootId].ChildrenIds,
	}

	ReconstructTree(data, flameGraphTreeRoot, q.minValue)
	return flameGraphTreeRoot, nil
}
