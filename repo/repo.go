package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/thaianhsoft/drm/changeset"
	"reflect"
	"strings"
)

type DefaultConfigQuery struct {
	IncludeColAs bool
	RenameTableAs string
}

var repo *Repo
type Repo struct {
	db *sql.DB
}

func NewRepo(config *mysql.Config) *Repo {
	if repo != nil {
		return repo
	}
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil
	}
	repo = &Repo{
		db :db,
	}
	return repo
}

type Querier interface{
	query(config ...*DefaultConfigQuery) (query string, args []interface{})
	Append(Querier) Querier
}

type C struct {
	name string
	table string
	as string
}

func Col(name string, tb string) *C {
	return &C{
		name: name,
		table: tb,
	}
}

func (c *C) As(as string) *C {
	c.as = as
	return c
}

type Selector struct {
	cols []*C
}

func (s *Selector) Append(querier Querier) Querier {
	if _, ok := querier.(*Selector); ok {
		s.cols = append(s.cols, querier.(*Selector).cols...)
	}
	return s
}

func (s *Selector) query(config ...*DefaultConfigQuery) (string, []interface{}) {
	if len(s.cols) == 0 {
		return "", nil
	}

	q := ""
	for i, col := range s.cols {
		var tempRename string = col.table
		if len(config) > 0 {
			tempRename = config[0].RenameTableAs
			if !config[0].IncludeColAs {
				col.as = ""
			}
		}
		q += fmt.Sprintf("`%v`.`%v`", tempRename, col.name)
		if col.as != "" {
			q += " AS "
			q += col.as
		}
		if i < len(s.cols) - 1 {
			q += ", "
		}
	}
	q += " "
	return q, nil
}


type QueryBuilder struct {
	query      string
	table      string
	Projection Querier
	Predicate  Querier
	limit      Querier
	groupBy    Querier
	orderBy    Querier
	args       []interface{}
}
func (q *QueryBuilder) OrderBy(c *C, orderType OrderType) *QueryBuilder {
	o := &orderBy{
		name: c.name,
		table: c.table,
		orderType: orderType,
	}
	q.orderBy = o
	return q
}

func (q *QueryBuilder) Query() (string, []interface{}) {
	query := ""
	if q.query != "" {
		query = q.query
	}
	if q.Projection != nil {
		projectQuery, args := q.Projection.query()
		if args != nil {
			q.args = append(q.args, args)
		}

		query = " SELECT " + projectQuery
		query += q.query
		q.query = query
	}
	if q.Predicate != nil {
		q.query += " "
		predicateQuery, args := q.Predicate.query()
		q.query += predicateQuery
		if args != nil {
			q.args = append(q.args, args...)
		}
	}
	if q.orderBy != nil {
		q.query += " "
		orderByQuery, _ := q.orderBy.query()
		q.query += orderByQuery
	}
	return q.query, q.args
}

func (q *QueryBuilder) Select(col *C) *QueryBuilder {
	if q.Projection == nil {
		q.Projection = &Selector{
			cols: make([]*C, 0),
		}
	}
	q.Projection.(*Selector).cols = append(q.Projection.(*Selector).cols, col)
	return q
}


type PredicateOp uint
func (p PredicateOp) toString() string {
	if p == LessEqual {
		return "<="
	}
	if p == Less {
		return "<"
	}
	if p == GreaterEqual {
		return ">="
	}

	if p == Greater {
		return ">"
	}

	if p == Like {
		return "LIKE"
	}
	return "="
}
const (
	LessEqual PredicateOp = iota
	Less
	GreaterEqual
	Greater
	Equal
	Like
)
type Predicate struct {
	col string
	table string
	op string
	val interface{}
}

func P(col string, table string, op PredicateOp, val interface{}) *Predicate {
	return &Predicate{
		col:   col,
		table: table,
		op:    op.toString(),
		val:   val,
	}
}

type Where struct {
	predicates []*Predicate
}

func (w *Where) Append(querier Querier) Querier {
	if _, check := querier.(*Where); check {
		w.predicates = append(w.predicates, querier.(*Where).predicates...)
	}
	return w
}

func (w *Where) query(config ...*DefaultConfigQuery) (string, []interface{}) {
	query := "WHERE "
	arguments := []interface{}{}
	for i, p := range w.predicates {
		if len(config) > 0 {
			p.table = config[0].RenameTableAs
		}
		query += fmt.Sprintf("`%v`.`%v` %v ?", p.table, p.col, p.op)
		if i < len(w.predicates) - 1 {
			query += " AND "
		}
		arguments = append(arguments, p.val)
	}
	return query, arguments
}

func (q *QueryBuilder) Where(predicate *Predicate) *QueryBuilder {
	if q.Predicate == nil {
		q.Predicate = &Where{
			predicates: make([]*Predicate, 0),
		}
	}
	q.Predicate.(*Where).predicates = append(q.Predicate.(*Where).predicates, predicate)
	return q
}



func (r *Repo) GetById(need interface{}, preloads ...func() (to interface{}, fk string, pk string, inverse bool)) *QueryBuilder {
	nv := reflect.Indirect(reflect.ValueOf(need))
	nvName := nv.Type().Name()
	nvTable := strings.ToLower(nvName) + "s"
	if len(preloads) == 0 {
		return &QueryBuilder{
			query: fmt.Sprintf("FROM `%v`", nvTable),
			table: nvTable,
			args: []interface{}{},
		}
	}
	to, fk, pk, inverse := preloads[0]()
	pv := reflect.Indirect(reflect.ValueOf(to))
	fmt.Println(pv)
	pvName := pv.Type().Name()

	nvKey := pk
	pvTable := strings.ToLower(pvName) + "s"
	pvKey := fk
	if inverse {
		nvKey, pvKey = pvKey, nvKey
	}
	query := fmt.Sprintf("FROM `%v` INNER JOIN `%v` ON `%v`.`%v` = `%v`.`%v`", nvTable, pvTable, nvTable, nvKey, pvTable, pvKey)
	fmt.Println(query)
	return &QueryBuilder{table: nvTable, query: query, args: []interface{}{}}
}

type Condition struct {
	OrderBy bool
}
// update repo
func (r *Repo) ParseToStruct(rows*sql.Rows, cast interface{}, cond ...*Condition) ([]interface{}, []interface{}) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil
	}
	var scaned = map[interface{}]reflect.Value{}
	var orderId = []interface{}{}
	type RelRelation struct {
		relScaned *reflect.Value
		fieldRef  string
		isO2O     bool
	}
	fmt.Println(cols, "cast")
	castReflect := reflect.Indirect(reflect.ValueOf(cast))
	results := []interface{}{}
	for rows.Next() {
		jsonByteAddrs := map[string][]interface{}{}
		castedNew := reflect.Indirect(reflect.New(castReflect.Type()))
		addrs := make([]interface{}, len(cols))
		rels := map[string]*RelRelation{}
		for i, col := range cols {
			_, ok := castedNew.Type().FieldByName(col)
			isJson := false
			if !ok {
				str := strings.Split(col, "$")
				scName := strings.Split(str[0], "Rel")
				if _, jsonOk := changeset.JsonFieldsOfSchemas[scName[0]]; jsonOk {
					if _, jsonOk := changeset.JsonFieldsOfSchemas[scName[0]][str[1]]; jsonOk {
						isJson = true
					}
				}
				fmt.Println("have json", isJson, str[0], str[1], changeset.JsonFieldsOfSchemas)
				if len(str) != 2 {
					var addr interface{}
					addrs[i] = &addr
					continue
				}
				if _, ok := rels[str[0]]; !ok {
					rels[str[0]] = &RelRelation{
						isO2O: false,
					}
				}
				rel := rels[str[0]]
				fRel := castedNew.FieldByName(str[0])
				var fRelType reflect.Type
				if fRel.Type().Kind() == reflect.Slice {
					fRelType = fRel.Type().Elem()
				}
				if fRel.Type().Kind() == reflect.Ptr {
					fRelType = fRel.Type()
					rel.isO2O = true
				}
				// fRelType is pointer => use elem before use new
				rel.fieldRef = str[0]
				if rel.relScaned == nil {
					var newRel = reflect.New(fRelType.Elem())
					rel.relScaned = &newRel
				}
				_, ok = (*rel.relScaned).Type().Elem().FieldByName(str[1])
				if isJson {
					var addr []byte
					if _, ok := jsonByteAddrs[str[1]]; !ok {
						jsonByteAddrs[str[1]] = make([]interface{}, 2)
					}
					fmt.Println("have json field is", str[1])
					jsonByteAddrs[str[1]][0] = &addr
					jsonByteAddrs[str[1]][1] = rel.relScaned
					fmt.Println(jsonByteAddrs, "byte 2")
					addrs[i] = &addr
					continue
				}
				if !ok {
					// check field of rel name exist if not assign empty addr
					var addr interface{}
					addrs[i] = &addr
					continue
				}
				addrs[i] = (*rel.relScaned).Elem().FieldByName(str[1]).Addr().Interface()
				continue
			}
			scName := castReflect.Type().Name()
			if _, jsonOk := changeset.JsonFieldsOfSchemas[scName]; jsonOk {
				if _, jsonOk := changeset.JsonFieldsOfSchemas[scName][col]; jsonOk {
					isJson = true
				}
			}
			if isJson {
				var addr []byte
				if _, ok := jsonByteAddrs[col]; !ok {
					jsonByteAddrs[col] = make([]interface{}, 2)
				}
				fmt.Println("have json field is", col)
				jsonByteAddrs[col][0] = &addr
				jsonByteAddrs[col][1] = &castedNew
				fmt.Println(jsonByteAddrs, "byte 2")
				addrs[i] = &addr
				continue
			}

			f := castedNew.FieldByName(col)
			addrs[i] = f.Addr().Interface()
		}
		rows.Scan(addrs...)
		for fieldName, byteAndStructAddrJson := range jsonByteAddrs {
			fmt.Println(byteAndStructAddrJson, "have")
			if byteAddr, ok := byteAndStructAddrJson[0].(*[]byte); ok {
				var reflectTypeJsonClass reflect.Type
				var havePointerSet bool = false
				if byteAndStructAddrJson[1].(*reflect.Value).Kind() == reflect.Ptr {
					reflectTypeJsonClass = byteAndStructAddrJson[1].(*reflect.Value).Elem().FieldByName(fieldName).Type().Elem()
					havePointerSet = true
				}
				if byteAndStructAddrJson[1].(*reflect.Value).Kind() == reflect.Struct {
					reflectTypeJsonClass = byteAndStructAddrJson[1].(*reflect.Value).FieldByName(fieldName).Type().Elem()
				}
				rvClassJson := reflect.New(reflectTypeJsonClass).Interface()
				if err := json.Unmarshal(*byteAddr, rvClassJson); err == nil {
					fmt.Println(rvClassJson, "cast json okkkk")
					if havePointerSet {
						byteAndStructAddrJson[1].(*reflect.Value).Elem().FieldByName(fieldName).Set(reflect.ValueOf(rvClassJson))
					} else {
						byteAndStructAddrJson[1].(*reflect.Value).FieldByName(fieldName).Set(reflect.ValueOf(rvClassJson))
					}
				} else {
					fmt.Println("cast json failed", err)
				}
			}
		}
		if _, ok := castedNew.Type().FieldByName("Id"); ok {
			idVal := castedNew.FieldByName("Id").Interface()
			if _, ok := scaned[idVal]; !ok {
				scaned[idVal] = castedNew
				if len(cond) == 1 {
					if cond[0].OrderBy {
						orderId = append(orderId, idVal) // sort id of order by
					}
				}
				fmt.Println(scaned)
			}
			fmt.Println("have rels is", rels)
			for _, rel := range rels {
				if !rel.isO2O {
					newVal := reflect.Append(scaned[idVal].FieldByName(rel.fieldRef), *rel.relScaned)
					scaned[idVal].FieldByName(rel.fieldRef).Set(newVal)
				} else {
					scaned[idVal].FieldByName(rel.fieldRef).Set(*rel.relScaned)
				}
			}
			for _, addr := range addrs {
				rv := reflect.Indirect(reflect.ValueOf(addr))
				fmt.Println("have value is rv", rv.Interface())
			}
		}
	}
	if len(orderId) == 0 {
		for k, v := range scaned {
			results = append(results, v.Addr().Interface())
			delete(scaned, k)
		}
		return results, nil
	}
	for _, id := range orderId {
		fmt.Println("order id: ", id)
		if _, ok := scaned[id]; ok {
			results = append(results, scaned[id].Addr().Interface())
			delete(scaned, id)
		}
	}
	return results, nil
}


func (r *Repo) RawQuery(query string, args []interface{}, cast interface{})  ([]interface{}, []interface{}){
	stmt, err := r.db.Prepare(query)
	if err != nil {
		fmt.Println(err, "prepare")
		return nil, nil
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		fmt.Println(err, "rows")
	}

	fmt.Println("can query ok")
	if strings.Contains(query, "ORDER BY") {
		return r.ParseToStruct(rows, cast, &Condition{OrderBy: true})
	}
	return r.ParseToStruct(rows, cast)
}

func (r *Repo) Save(ctx context.Context, cs *changeset.ChangeSet) error {
	query, args := r.insertQuery(cs)
	stmt, err := r.db.PrepareContext(ctx, query)
	fmt.Println(query, args)
	if err != nil {
		fmt.Println("error from prepare query", err)
		return  err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		fmt.Println("error from exec query", err)
		return  err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	cs.ReflectSchema.FieldByName("Id").Set(reflect.ValueOf(uint32(id)))
	cs.ActionRepo = changeset.ActionInsert
	return nil
}

func (r *Repo) SaveTx(ctx context.Context, cs*changeset.ChangeSet, tx *sql.Tx) error {
	query, args := r.insertQuery(cs)
	stmt, err := tx.PrepareContext(ctx, query)
	fmt.Println(query, args)
	if err != nil {
		fmt.Println(err)
		return  err
	}
	result, err := stmt.ExecContext(ctx, args...)
	if err != nil {
		fmt.Println(err)
		return  err
	}
	id, err := result.LastInsertId()
	if err != nil {
		fmt.Println(err)
		return err
	}

	if (cs.Boxes["Id"].GetOps() & (1<<changeset.AI)) != 0 {
		cs.ReflectSchema.FieldByName("Id").Set(reflect.ValueOf(uint32(id)))
	}
	cs.ActionRepo = changeset.ActionInsert
	return nil
}


func (r *Repo) OpenTx(ctx context.Context) *sql.Tx {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil
	}
	return tx
}

func (r *Repo) insertQuery(cs *changeset.ChangeSet) (string, []interface{}){
	tb := strings.ToLower(cs.ReflectSchema.Type().Name()) + "s"
	query := fmt.Sprintf("INSERT INTO `%v` (", tb)
	values := " VALUES ("
	args := []interface{}{}
	for i, col := range cs.CastedBoxes {
		if cs.Boxes[col].UpdatedCol != "" {
			query += fmt.Sprintf("`%v`", cs.Boxes[col].RelTbName+cs.Boxes[col].UpdatedCol)
		} else {
			query += fmt.Sprintf("`%v`", col)
		}
		values += "?"
		if i < len(cs.CastedBoxes) - 1 {
			query += ", "
			values += ", "
		}
		args = append(args, cs.Boxes[col].GetVal())
	}
	query += ")"
	values += ")"
	query += values
	return query, args
}

func (r *Repo) UpdateById(ctx context.Context, cs *changeset.ChangeSet) error {
	query, args := UpdateQuery(cs)
	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		fmt.Println(err)
		return  err
	}

	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		fmt.Println(err)
		return  err
	}
	cs.ActionRepo = changeset.ActionUpdate
	return nil
}

func (r *Repo) UpdateTxById(ctx context.Context, cs *changeset.ChangeSet, tx *sql.Tx) error {
	query, args := UpdateQuery(cs)
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		fmt.Println(err)
		return  err
	}

	_, err = stmt.ExecContext(ctx, args...)
	if err != nil {
		fmt.Println(err)
		return  err
	}
	cs.ActionRepo = changeset.ActionUpdate
	return nil
}


func UpdateQuery(cs *changeset.ChangeSet) (string, []interface{}) {
	tbName := strings.ToLower(cs.ReflectSchema.Type().Name()) + "s"
	query := fmt.Sprintf("UPDATE %v SET ", tbName)
	args := []interface{}{}
	for i, col := range cs.CastedBoxes {
		if cs.Boxes[col].UpdatedCol != "" {
			query += fmt.Sprintf("`%v` = ?", cs.Boxes[col].RelTbName + cs.Boxes[col].UpdatedCol)
		} else {
			query += fmt.Sprintf("`%v` = ?", col)
		}
		args = append(args, cs.Boxes[col].GetVal())
		if i < len(cs.CastedBoxes) - 1 {
			query += ", "
		}
	}
	query += " WHERE `Id` = ?"
	args = append(args, cs.ReflectSchema.FieldByName("Id").Interface())
	return query, args
}

type Rel struct {
	from string
	to string
	fromKey string
	toKey string
	builder *QueryBuilder
}

type CacheKey struct {
	key string
	tb string
}


type QueryRel struct {
	rels []*Rel
	query string
	args []interface{}
	index int
	joinedKeysCache []*CacheKey
}

func (q *QueryRel) OpenRel(rel *Rel) *QueryRel {
	q.rels = append(q.rels, rel)
	return q
}

func (q *QueryRel) ParseToQuery() (string, []interface{}) {
	dfs(q, true)
	return q.query, q.args
}

func dfs(q *QueryRel, rebuild bool) {
	if rebuild {
		if q.index >= len(q.rels) {
			q.index = 0
			dfs(q, false)
			return
		}
		index := q.index
		source := q.rels[index]

		if index < len(q.rels) && index > 0 {
			if q.rels[index-1].from == source.from {
				swapFromAndTo(q.rels[index-1], index, len(q.rels))
			}
			if q.rels[index-1].from == source.to {
				swapFromAndTo(q.rels[index-1], index , len(q.rels))
			}
		}
		q.index++
		dfs(q, true)
	} else {
		if q.index >= len(q.rels) {
			return
		}
		index := q.index
		source := q.rels[index]
		var haveExpandQuery bool
		if index > 0 {
			q.query += "("
		}
		if index < len(q.rels) - 1 {
			var IncludeColAs bool = false
			if index == 0 {
				IncludeColAs = true
			}
			for i := index; i < len(q.rels)-1; i++ {
				if q.rels[i+1].builder != nil {
					fmt.Println(IncludeColAs, index, i)
					projectQuery, _ := q.rels[i+1].builder.Projection.query(&DefaultConfigQuery{
						IncludeColAs: IncludeColAs,
						RenameTableAs: fmt.Sprintf("%v_%v", "r", index+1),
					})
					if projectQuery != "" {
						if !haveExpandQuery {
							haveExpandQuery = true
							q.query += "SELECT "
						} else {
							q.query += ", "
						}
						q.query += projectQuery
					}
				}
			}
		}
		// load self builder
		if q.rels[index].builder != nil {
			if q.rels[index].builder.Projection != nil {
				selfProjectQuery, _ := q.rels[index].builder.Projection.query()
				if selfProjectQuery != "" {
					if !haveExpandQuery {
						haveExpandQuery = true
						q.query += "SELECT "
					} else {
						q.query += ", "
					}
					q.query += selfProjectQuery
				}
			}

		}
		if index > 0 {
			if haveExpandQuery {
				q.query += fmt.Sprintf(", `%v`.`%v`", q.rels[index-1].to, q.rels[index-1].toKey)
			} else {
				q.query += fmt.Sprintf("SELECT `%v`.`%v`", q.rels[index-1].to, q.rels[index-1].toKey)
			}
			q.query += " "
		}
		q.query += fmt.Sprintf("FROM %v INNER JOIN ", source.from)
		q.index++
		dfs(q, false)
		// backtracking
		if index == len(q.rels) - 1 {
			q.query += fmt.Sprintf("%v ON `%v`.`%v` = `%v`.`%v`", source.to, source.from, source.fromKey, source.to, source.toKey)
		}
		if index > 0 {
			tbNameAs := fmt.Sprintf("%v_%v", "r", index)
			q.query += fmt.Sprintf(") AS `%v` ON `%v`.`%v` = `%v`.`%v`",tbNameAs, q.rels[index-1].from, q.rels[index-1].fromKey, tbNameAs, q.rels[index-1].toKey)
		}
		if q.rels[index].builder != nil {
			if q.rels[index].builder.Predicate != nil {
				selfPredicateQuery, args := q.rels[index].builder.Predicate.query()
				if selfPredicateQuery != "" {
					q.query += selfPredicateQuery
					q.args = append(q.args, args...)
				}
			}
		}
	}
}


func JoinMultipleBuilder(left, right *QueryBuilder) {
	lv := reflect.Indirect(reflect.ValueOf(left))
	rv := reflect.Indirect(reflect.ValueOf(right))
	for i := 0; i < lv.NumField(); i++ {
		if lv.Field(i).Type().Name() == "Querier" && lv.Type().Field(i).IsExported(){
			append_name := lv.Type().Field(i).Name
			if _, inLeft := lv.Type().FieldByName(append_name); inLeft {
				if _, inRight := rv.Type().FieldByName(append_name); inRight {
					fmt.Println(lv.FieldByName(append_name), append_name, "exist")
					var from Querier
					var to Querier
					leftAppendVal := lv.FieldByName(append_name).Interface()
					rightAppendVal := rv.FieldByName(append_name).Interface()
					if leftAppendVal != nil {
						from = leftAppendVal.(Querier)
						if rightAppendVal != nil {
							to = rightAppendVal.(Querier)
						}
					} else {
						from = rightAppendVal.(Querier)
						if leftAppendVal != nil {
							to = leftAppendVal.(Querier)
						}
					}

					if from != nil {
						if to != nil {
							from.Append(to)
						}
						lv.FieldByName(append_name).Set(reflect.ValueOf(from))
					}
				}
			}
		}
	}
}

func swapFromAndTo(rel *Rel, indexRel int, n int) {
	fmt.Println(rel, indexRel, n, "not swap")
	rel.fromKey, rel.toKey = rel.toKey, rel.fromKey
	if indexRel == n - 1 {
		rel.from, rel.to = rel.to, rel.from
	} else {
		rel.from = rel.to
		rel.to = fmt.Sprintf("r_%v", indexRel+1)
	}
	fmt.Println(rel, indexRel, n, "swaped")
}

func JoinProjectBuilder(query *string, projectQuery *string) {
	*query = *projectQuery + *query
}

func ReplaceStringHaveAs(query *string) {
	var startPoint int = 0
	var index = 0
	for index < len(*query) {
		fmt.Println(*query, string((*query)[index]), index)
		if string((*query)[index]) == "A" && string((*query)[index-1]) == " " && index > 0 {
			if index < len(*query) - 3 {
				x, y := (*query)[index+1], (*query)[index+2]
				if string(x) == "S" && string(y) == " " {
					// z is start point of rename table
					startPoint = index-1
				}
			}
		}
		if string((*query)[index]) == "," && startPoint != 0 {
			c := (*query)[:startPoint]
			d := (*query)[index:]
			*query = c
			*query += d
			index = startPoint
			startPoint = 0
			continue
		}
		index++
	}
}
