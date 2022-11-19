package changeset

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

var JsonFieldsOfSchemas = map[string]map[string]bool{}
type ActionRepo uint8
const (
	ActionUpdate ActionRepo = iota + 1
	ActionInsert
	ActionDelete
	ActionSelect
)
type Schema interface{
	Validators() map[string]*Box
}

type FieldOp uint8
const (
	AI FieldOp = iota
	Nullable
	NotNullable
	JSONOp
)
type Box struct {
	id         uint32
	ops        uint8
	size       int
	val        interface{}
	UpdatedCol string
	RelTbName string
}

func (b *Box) GetOps() uint8 {
	return b.ops
}
func (b *Box) SetEmbeddedClass(class Schema, colUpdate ...string) *Box {
	b.val = class
	if len(colUpdate) > 0 {
		b.RelTbName = reflect.Indirect(reflect.ValueOf(class)).Type().Name()
		b.UpdatedCol = colUpdate[0]
	}
	return b
}


func (b *Box) GetId() uint32 {
	return b.id
}
func (b *Box) GetVal() interface{} {
	if _, can := b.val.(Schema); can {
		return reflect.ValueOf(b.val).Elem().FieldByName(b.UpdatedCol).Interface()
	}
	if (b.ops & (1 << JSONOp)) != 0 {
		jsonData, err := json.Marshal(b.val)
		if err == nil {
			return jsonData
		}
	}
	return b.val
}

func (b *Box) Ops(ops ...FieldOp) *Box {
	for _, op := range ops {
		b.ops |= 1 << op
	}
	return b
}


func (b *Box) Val(v interface{}) *Box {
	b.val = v
	return b
}

func (b *Box) Size(s int) *Box {
	b.size = s
	return b
}

func (b *Box) JSONField() *Box {
	b.ops |= 1 << JSONOp
	return b
}

func NewBox() *Box{
	return &Box{}
}

type ChangeSet struct {
	ActionRepo ActionRepo
	ReflectSchema reflect.Value
	Boxes         map[string]*Box
	NotNullFields uint32
	CastedBoxes []string
	SubChangeSets map[string]*ChangeSet
}


func CastClass(schema Schema, msg interface{}) *ChangeSet {
	rschema := reflect.Indirect(reflect.ValueOf(schema))
	cs := &ChangeSet{
		Boxes:         schema.Validators(),
		NotNullFields: 0,
		SubChangeSets: map[string]*ChangeSet{},
	}

	var id uint32
	for col, box := range cs.Boxes {
		if (box.ops & (1<<NotNullable)) != 0 {
			cs.NotNullFields |= 1<<id
			box.id = id
		}
		if box.ops & (1<<JSONOp) != 0 {
			if _, ok := JsonFieldsOfSchemas[rschema.Type().Name()]; !ok {
				JsonFieldsOfSchemas[rschema.Type().Name()] = make(map[string]bool)
			}
			JsonFieldsOfSchemas[rschema.Type().Name()][col] = true
		}
		id++
	}

	rmsg := reflect.Indirect(reflect.ValueOf(msg))
	for i := 0; i < rmsg.NumField(); i++ {
		str := strings.Split(rmsg.Type().Field(i).Name, rschema.Type().Name())
		if strings.HasPrefix(rmsg.Type().Field(i).Name, rschema.Type().Name()) {
			if _, ok := cs.Boxes[str[1]]; ok {
				if (cs.NotNullFields & (1 << cs.Boxes[str[1]].id)) != 0 {
					if !rmsg.Field(i).IsZero() || (cs.Boxes[str[1]].ops&(1<<AI)) != 0 {
						cs.NotNullFields &= ^(1 << cs.Boxes[str[1]].id)
					}
				}
				if rmsg.Field(i).Type().Kind() == reflect.String {
					if len(rmsg.Field(i).Interface().(string)) <= cs.Boxes[str[1]].size {
						cs.Boxes[str[1]].Val(rmsg.Field(i).Interface())
					}
				} else {
					if _, can := cs.Boxes[str[1]].val.(Schema); can {
						// this is relation class embedded
						sub_cs := CastClass(cs.Boxes[str[1]].val.(Schema), rmsg.Field(i).Interface())
						cs.SubChangeSets[str[1]] = sub_cs
					} else {
						cs.Boxes[str[1]].Val(rmsg.Field(i).Interface())
					}
				}

				if cs.CastedBoxes == nil {
					cs.CastedBoxes = []string{}
				}

				if _, can := cs.Boxes[str[1]].val.(Schema); can {
					if rschema.FieldByName(str[1]).Type().Kind() == reflect.Slice {
						rschema.FieldByName(str[1]).Set(reflect.Append(rschema.FieldByName(str[1]), reflect.ValueOf(cs.Boxes[str[1]].val)))
					} else {
						rschema.FieldByName(str[1]).Set(reflect.ValueOf(cs.Boxes[str[1]].val))
					}
				} else {
					rschema.FieldByName(str[1]).Set(rmsg.Field(i))
				}
				if (cs.Boxes[str[1]].ops & (1 << AI)) != 0 {
					continue
				}

				if !rmsg.Field(i).IsZero()  {
					if _, can := cs.Boxes[str[1]].val.(Schema); can {
						if cs.Boxes[str[1]].UpdatedCol != "" {
							cs.CastedBoxes = append(cs.CastedBoxes, str[1])
						}
					} else {
						cs.CastedBoxes = append(cs.CastedBoxes, str[1])
					}
				}
			}
		}
	}
	cs.ReflectSchema = rschema
	return cs
}

func CastValues(schema Schema, values map[string]interface{}) *ChangeSet {
	rschema := reflect.Indirect(reflect.ValueOf(schema))
	cs := &ChangeSet{
		Boxes:         schema.Validators(),
		NotNullFields: 0,
		SubChangeSets: map[string]*ChangeSet{},
	}
	var id uint32
	for _, box := range cs.Boxes {
		if box.ops & (1<<NotNullable) != 0 {
			cs.NotNullFields |= 1<<id
			box.id = id
		}
		id++
	}
	for col, value := range values {
		if _, ok := cs.Boxes[col]; ok {
			if cs.NotNullFields & (1<<cs.Boxes[col].id) != 0 {
				if value != "" || value != 0 || value != nil || (cs.Boxes[col].ops & (1<<AI)) != 0{
					cs.NotNullFields &= ^(1<<cs.Boxes[col].id)
				}
			}

			if reflect.TypeOf(value).Kind() == reflect.String {
				if len(value.(string)) <= cs.Boxes[col].size {

				}
			} else {
				cs.Boxes[col].Val(value)
			}

			if cs.Boxes[col].val != nil {
				rschema.FieldByName(col).Set(reflect.ValueOf(value))
			}

			if (cs.Boxes[col].ops & (1 << AI)) != 0 {
				continue
			}

			if value != "" || value != 0 {
				cs.CastedBoxes = append(cs.CastedBoxes, col)
			}
		}
	}
	cs.ReflectSchema = rschema
	return cs
}

func (cs *ChangeSet) ValidInsert() bool {
	return cs.NotNullFields == 0
}

func (cs *ChangeSet) NotNullErrors() error {
	errs := fmt.Sprintf("Required Fields aren't Nullable (")
	errFields := []string{}
	for col, box := range cs.Boxes {
		if (cs.NotNullFields & (1 << box.id))!= 0 {
			errFields = append(errFields, col)
		}
	}

	for i, errField := range errFields {
		errs += errField
		if i < len(errFields) - 1 {
			errs += ", "
		}
	}
	errs += ")"
	return fmt.Errorf(errs)
}

func (cs *ChangeSet) Unique(nameFields ...string) {
	tb := strings.ToLower(cs.ReflectSchema.Type().Name())+ "s"
	where := "WHERE "
	args := []interface{}{}
	for i, nameField := range nameFields {
		where += fmt.Sprintf("`%v`.`%v` = ?", tb, nameField)
		args = append(args, cs.Boxes[nameField].val)
		if i < len(nameFields) {
			where += " AND"
		}
	}
}

func (cs *ChangeSet) SetRelValues(values map[string]interface{}) *ChangeSet {
	for col, value := range values {
		if cs.Boxes[col].UpdatedCol != "" {
			cs.Boxes[col].val = value
			cs.CastedBoxes = append(cs.CastedBoxes, col)
		}
	}
	return cs
}

func (cs *ChangeSet) AppendCastValue(interfaceSchema interface{}, values map[string]interface{}) {
	rschema := reflect.Indirect(reflect.ValueOf(interfaceSchema))
	for col, value := range values {
		if _, ok := cs.Boxes[col]; ok {
			if cs.NotNullFields & (1<<cs.Boxes[col].id) != 0 {
				if value != "" || value != 0 || value != nil || (cs.Boxes[col].ops & (1<<AI)) != 0{
					cs.NotNullFields &= ^(1<<cs.Boxes[col].id)
				}
			}

			if reflect.TypeOf(value).Kind() == reflect.String {
				if len(value.(string)) <= cs.Boxes[col].size {

				}
			} else {
				cs.Boxes[col].Val(value)
			}

			if cs.Boxes[col].val != nil {
				rschema.FieldByName(col).Set(reflect.ValueOf(value))
			}

			if (cs.Boxes[col].ops & (1 << AI)) != 0 {
				continue
			}

			if value != "" || value != 0 {
				cs.CastedBoxes = append(cs.CastedBoxes, col)
			}
		}
	}
}
