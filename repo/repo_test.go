package repo

import (
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/thaianhsoft/drm/domain"
	"testing"
)

var mysqlConfig *mysql.Config = &mysql.Config{
	User: "thaianh",
	Passwd: "thaianh1711",
	DBName: "mydb",
	Addr: "localhost:3306",
	Net: "tcp",
	AllowNativePasswords: true,
	MultiStatements: true,
}
func TestRepo(t *testing.T) {
	r := &Repo{}
	{
		builder := r.GetById(&domain.Driver{}, func() (to interface{}, fk string, pk string, inverse bool) {
			return &domain.Vehical{}, "driver_id", "id", false
		})
		builder.Select(Col("age", "driver")).
				Select(Col("license", "driver")).
				Where(P("age", "driver", GreaterEqual, 35)).
				Where(P("name", "driver", GreaterEqual, "%JohnSmith%"))
		query, args := builder.Query()
		fmt.Println(query, args)
	}

	{
		builder := r.GetById(&domain.Vehical{})
		builder.Select(Col("id", "createdDate")).
				Where(P("role", "vehical", Like, "xetai"))
		query, args := builder.Query()
		fmt.Println(query, args)
	}
}

func TestParseStruct(t *testing.T) {


	repo := NewRepo(mysqlConfig)
	/*
	{
		driver := &domain.Driver{}
		builder := repo.GetById(driver, func() (to interface{}, fk string, pk string, inverse bool) {
			return &domain.Vehical{}, "DriverId", "id", false
		})
		builder.Select(Col("Did", "drivers")).
			Select(Col("IsFree", "drivers")).
			Select(Col("DriverId", "vehicals").As("VehicalRel$DriverId")).
			Select(Col("Did", "vehicals").As("VehicalRel$Did")).
			Select(Col("Type", "vehicals").As("VehicalRel$Type"))
		query, args := builder.Query()
		drivers := []*domain.Driver{}
		t1 := time.Now()
		results, _ := repo.RawQuery(query, args, driver)
		t2 := time.Since(t1)
		for _, result := range results {
			switch r := result.(type) {
			case *domain.Driver:
				drivers = append(drivers, r)
			}
		}


		for _, driver := range drivers {
			for _, vehical := range driver.VehicalRel {
				fmt.Printf("DRIVER [Did=%v] have Vehical [Did=%v] Type [%v]\n", driver.Did, vehical.Did, vehical.Type)
			}
		}
		fmt.Println("query time driver: ", t2)
	}
	 */
	{
		// test driver partners
		builder := repo.GetById(&domain.Partner{}, func() (to interface{}, fk string, pk string, inverse bool) {
			return &domain.Driver{}, "PartnerId", "Did", false
		})
		builder.Select(Col("PcPrice", "partners")).
				Select(Col("Did", "partners")).
				Select(Col("Did", "drivers").As("DriverRel$Did")).
				OrderBy(Col("PcPrice", "partners"), ASC)

		query, args := builder.Query()
		results, _ := repo.RawQuery(query, args, &domain.Partner{})
		for _, p := range results {
			partner := p.(*domain.Partner)
			for _, driver := range partner.DriverRel {
				driver.PartnerRel = partner
				fmt.Println(driver.PartnerRel)
			}
		}
	}
}


func TestJoinBuilder(t *testing.T) {
	lb := &QueryBuilder{}
	lb.Select(Col("Did", "drivers")).OrderBy(Col("Date", "drivers"), DESC)
	rb := &QueryBuilder{}
	rb.Select(Col("Partner Did", "drivers")).Where(P("Did", "drivers", Equal, 1))
	JoinMultipleBuilder(lb, rb)
	fmt.Println(lb.Query())
}

func TestPreloads(t *testing.T) {
	repo := NewRepo(mysqlConfig)
	queryRel := &QueryRel{}
	queryRel.OpenRel(&Rel{
		from:    "drivers",
		to:      "users",
		fromKey: "Did",
		toKey:   "Did",
		builder: (&QueryBuilder{}).Select(Col("Did", "users")).Where(P("Did", "users", Equal, 10)),
	})
	queryRel.OpenRel(&Rel{
		from: "vehicals",
		fromKey: "DriverId",
		to: "drivers",
		toKey:   "Did",
	})
	queryRel.OpenRel(&Rel{
		from:    "drivers",
		to:      "partners",
		fromKey: "PartnerId",
		toKey:   "Did",
		builder: (&QueryBuilder{}).Select(Col("PPriceKm", "partners").As("PartnerRel$PPriceKm")).
			Select(Col("PPricePC", "partners").As("PartnerRel$PPricePC")),
	})
	query,args := queryRel.ParseToQuery()
	fmt.Println(args)
	entities, _ := repo.RawQuery(query, args, &domain.Driver{})
	for _, entity := range entities {
		driver := entity.(*domain.Driver)
		fmt.Println(driver.PartnerRel.Id)
	}
}

func TestReplaceAs(t *testing.T) {
	q := "SELECT id AS PartnerRel$Did, driverId AS driver$Did, vehicalId FROM INNER JOIN"
	ReplaceStringHaveAs(&q)
	fmt.Println(q)
}