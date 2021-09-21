package db

import (
	"fmt"
	"time"
	"errors"
	"context"

	"strings"
	"reflect"

	"intelligence_engine/config"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoService interface {
	All() ([]bson.M, error)
	One() (bson.M, error)
	Count() (int64, error)
	Connect() MongoService
	Select(fields interface{}) MongoService
	Where(field interface{}, ops ...interface{}) MongoService
	AndWhere(field interface{}, ops ...interface{}) MongoService
	OrWhere(field interface{}, ops ...interface{}) MongoService
	//OrderBy(field string, sort string) MongoService
	//Limit(size int16) MongoService
	//Offset(num int32) MongoService
}

type Database struct {
	Mongo  * mongo.Client
}

type mongoService struct {
	//L *mongo.Client
	D string
	T string
	C *mongo.Collection
	Q bson.M
	F findOptions
}

type findOptions struct {
	Limit *int64
	Skip *int64
	Max interface{}
	Min interface{}
	Projection interface{}
	Sort interface{}
}

var ErrNotFound = errors.New("not found")

var _ MongoService = (*mongoService)(nil)

var DB *Database

var EventdataColl  *mongo.Collection
var EventColl  *mongo.Collection
var FamilyColl  *mongo.Collection
var OrganColl  *mongo.Collection
var EtagColl  *mongo.Collection

//初始化
//func Init() {
//	DB = &Database{
//		Mongo: Open(),
//	}
//}

func Open() *mongo.Client {
	return nil
	//dsn := config.Gconf.MongoDsn
	//num := config.Gconf.MongoMaxOpen
	//
	//ctx ,cancel := context.WithTimeout(context.Background(),5*time.Second)
	//defer cancel()
	//// @TODO 添加qcm监控和重连
	//clientOptions := options.Client().ApplyURI(dsn).SetMaxPoolSize(num)
	//
	//client, _ := mongo.Connect(ctx, clientOptions)

	//return client
}

func NewMongoService(table string) MongoService {
	var name = config.Gconf.MongoDb
	var limit int64 = 20
	var skip int64 = 0

	//s.C = client.Database(name).Collection(table)
	opts := findOptions{Limit:&limit, Skip: &skip}

	return &mongoService{D: name, T: table, Q: bson.M{}, F: opts}
}

func (s *mongoService) Connect() MongoService {
	//client := Open()
	//s.C = client.Database(s.D).Collection(s.T)
	if s.D == config.Gconf.MongoDb {
		switch s.T {
		case "event":
			s.C = EventColl
		case "eventdata":
			s.C = EventdataColl
		case "family":
			s.C = FamilyColl
		case "etag":
			s.C = EtagColl
		case "organ":
			s.C = OrganColl
		}
	}

	return s
}

func (s *mongoService) All() ([]bson.M, error) {
	s.Connect()

	var results []bson.M

	selector := &options.FindOptions{
		Limit: s.F.Limit,
		Skip: s.F.Skip,
		Projection: s.F.Projection,
		Sort: s.F.Sort,
	}

	ctx := context.TODO()
	cursor, err := s.C.Find(ctx, s.Q, selector)

	if err != nil {
		return results, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, &results); err != nil {
		return results,err
	}

	return results, nil
}

func (s *mongoService) One() (bson.M, error) {
	s.Connect()

	var result bson.M

	selector := &options.FindOneOptions{
		Projection: s.F.Projection,
	}

	err := s.C.FindOne(context.TODO(), s.Q, selector).Decode(&result)

	return result, err
}

func (s *mongoService) Count() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	opts := options.Count().SetMaxTime(2 * time.Second)
	count, err := s.C.CountDocuments(ctx, s.Q, opts)

	//s.close()

	if err != nil {
		return 0, err
	}

	return count, err
}

func (s *mongoService) Select(fields interface{}) MongoService {

	items := buildSelectOptions(fields)
	selector := make(bson.M)
	selector["_id"] = 0
	for _, item := range items{
		selector[item] = 1
	}

	findOptions := s.F
	findOptions.Projection = selector

	s.F = findOptions

	return s
}

func (s *mongoService) Where(field interface{}, ops ...interface{}) MongoService {

	condition := buildCondition(field, ops...)

	s.Q = condition

	return s
}

func (s *mongoService) AndWhere(field interface{}, ops ...interface{}) MongoService {

	condition := buildCondition(field, ops...)

	query := bson.M{"$and":bson.A{s.Q, condition}}

	s.Q = query

	return s
}

func (s *mongoService) OrWhere(field interface{}, ops ...interface{}) MongoService {

	condition := buildCondition(field, ops...)

	query := bson.M{"$or":bson.A{s.Q, condition}}

	s.Q = query

	return s
}

func matchID(id string) (bson.D, error) {
	objectID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return nil, err
	}

	filter := bson.D{{Key: "_id", Value: objectID}}

	return filter, nil
}

func buildSelectOptions(fields interface{}) []string {
	sType := reflect.TypeOf(fields).Kind().String()
	switch sType {
	case "string":
		items := strings.Split(fields.(string), ",")
		return items
	case "array":
		return fields.([]string)
	default:
		fmt.Println("select: "+sType+" not supported")
		panic("select: "+sType+" not supported")
	}

	return []string{}
}

func buildOperator(op string) string {
	op = strings.ToLower(op)
	switch op {
	case "eq","ne","gt","gte","lt","lte","not","in","nin","regex":
		op = "$"+op
	case "=","":
		op = "$eq"
	case "!=":
		op = "$ne"
	case ">":
		op = "$gt"
	case ">=":
		op = "$gte"
	case "<":
		op = "$lt"
	case "<=":
		op = "$lte"
	case "not in":
		op = "$nin"
	case "like":
		op = "$regex"
	default:
		fmt.Println(op + " not supported")
		panic(op + " not supported")
	}

	return op
}

func buildCondition(field interface{}, ops ...interface{}) bson.M {

	fType := reflect.TypeOf(field).Kind().String()

	if fType == "string" {
		return buildConditionBySingle(field, ops...)
	}else if fType == "map" {
		result := make(bson.M)

		for key,val := range field.(map[string]string) {
			result[key] = val
		}

		return result
	}else {
		fmt.Println("where'"+fType+" not supported")
		panic("where'"+fType+" not supported")
	}

	return bson.M{}
}

func buildConditionBySingle(field interface{}, ops ...interface{}) bson.M {
	q := initParam("q", ops)
	op := initParam("op", ops).(string)

	qType := reflect.TypeOf(q).Kind().String()
	op = buildOperator(op)

	if qType == "string" || qType == "int" {
		if field.(string) == "ID" {
			var _ string
			q, _ = primitive.ObjectIDFromHex(q.(string))
		}
	}else if qType == "array" || qType == "slice" {
		if op != "$nin" {
			op = "$in"
		}
	}else {
		fmt.Println("q:" + qType + " not supported")
		panic("q:" + qType + " not supported")
	}

	condition := bson.M{field.(string): bson.M{op: q}}

	return condition
}

func initParam(field string, dials []interface{}) interface{} {
	switch field {
	case "q":
		if len(dials) < 1 {
			fmt.Println("q not empty")
			panic("q not empty")
		}else {
			return dials[0]
		}
	case "op":
		if len(dials) < 2 {
			return "="
		}else {
			return dials[1]
		}
	}

	return ""
}

func CheckType(value interface{}) string {
	return reflect.TypeOf(value).Kind().String()
}