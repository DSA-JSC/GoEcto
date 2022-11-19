package repo

import "fmt"

type OrderType uint8
const (
	DESC OrderType = iota + 1
	ASC
)

type orderBy struct {
	name string
	table string
	orderType OrderType
}


func (o *orderBy) query(config ...*DefaultConfigQuery) (string, []interface{}){
	if len(config) > 0 {
		o.table = config[0].RenameTableAs
	}
	q := fmt.Sprintf("ORDER BY `%v`.`%v`", o.table, o.name)
	if o.orderType == DESC {
		q += " DESC"
	}
	if o.orderType == ASC {
		q += " ASC"
	}
	return q, nil
}

func (o *orderBy) Append(querier Querier) Querier {
	return o
}
