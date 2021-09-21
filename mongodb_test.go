package db

import (
	"fmt"
	"testing"

	"intelligence_engine/config"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Family struct {
	ID          primitive.ObjectID `json:"_id" bson:"_id"` /* you need the bson:"_id" to be able to retrieve with ID filled */
	Name        string             `json:"name"`
	Description string             `json:"description"`
	OrgType     string             `json:"org_type"`
	Created     int                `json:"created"`
}

func TestConfig(t *testing.T)  {
	config.InitConfig()

	dsn := config.Gconf.MongoDsn
	num := config.Gconf.MongoMaxOpen
	fmt.Println(dsn)
	fmt.Println(num)
}

func TestOpen(t *testing.T)  {
	fmt.Println("test open start")
	config.InitConfig()
	db := Open()
	fmt.Println(db)
}

func TestWhere(t *testing.T)  {
	config.InitConfig()

	db := NewMongoService("family")

	names := [2]string{"GoldenEye","123"}
	w1 := db.Where("name", names, "in")
	fmt.Println(w1)

	w2 := db.Where("name", "1234")
	fmt.Println(w2)

	w3 := db.Where("name", "1234", "!=")
	fmt.Println(w3)
}

func TestAndWhere(t *testing.T)  {
	config.InitConfig()

	names := [2]string{"GoldenEye","123"}

	db := NewMongoService("family").Where("name", names, "not in").AndWhere("desc", "s", "=")

	fmt.Println(db)

}

func TestOrWhere(t *testing.T)  {
	config.InitConfig()

	db := NewMongoService("family").Where("name", "1234", "").OrWhere("name", "GoldenEye", "")

	fmt.Println(db)
}

func TestConnect(t *testing.T)  {
	config.InitConfig()

	db := NewMongoService("family").Connect()

	fmt.Println(db)
}

func TestOne(t *testing.T) {
	config.InitConfig()

	result,err := NewMongoService("family").Where("name", "cdsrc", "=").One()

	fmt.Println(err)
	fmt.Println(result)
}

func TestAll(t *testing.T)  {
	config.InitConfig()

	names := [2]string{"unknown_family","360safe_killing"}

	results, err := NewMongoService("family").Where("name", names, "in").All()

	fmt.Println(err)
	fmt.Println(results)
}

func TestCount(t *testing.T)  {
	config.InitConfig()

	names := [2]string{"GoldenEye","1234"}

	count, err := NewMongoService("family").Where("name", names, "in").Count()

	fmt.Println(err)
	fmt.Println(count)
}

func TestSelect(t *testing.T)()  {
	config.InitConfig()

	db := NewMongoService("family")

	s1 := db.Select("id")

	fmt.Println(s1)

	result, err := s1.One()

	fmt.Println(err)

	fmt.Println(result)
}