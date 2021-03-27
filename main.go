package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"

	"go.uber.org/zap"

	// Import the Elasticsearch library packages
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/log-service/lib/log_message"
)

// A function for marshaling structs to JSON string
func jsonStruct(docStruct log_message.LogMessage) (string, error) {

	//// Create struct instance of the Elasticsearch fields struct object
	//docStruct := &log_message.LogMessage{
	//	ServiceName:  doc.ServiceName,
	//	Message:  doc.Message,
	//	Payload: doc.Payload,
	//}
	//
	//fmt.Println("\ndocStruct:", docStruct)
	//fmt.Println("docStruct TYPE:", reflect.TypeOf(docStruct))

	// Marshal the struct to JSON and check for errors
	b, err := json.Marshal(docStruct)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func main() {
	l, err := NewLogger()
	if err != nil {
		log.Printf("create logger err = %s\n", err.Error())
		return
	}

	// Allow for custom formatting of log output
	//log.SetFlags(0)

	// Create a context object for the API calls
	ctx := context.Background()

	//// Create a mapping for the Elasticsearch documents
	//var (
	//	docMap map[string]interface{}
	//)
	//fmt.Println("docMap:", docMap)
	//fmt.Println("docMap TYPE:", reflect.TypeOf(docMap))

	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		//Username: "user",
		//Password: "pass",
	}

	// Instantiate a new Elasticsearch client object instance
	client, err := elasticsearch.NewClient(cfg)

	if err != nil {
		l.Error("error creating es client", zap.Error(err))
	}

	// Have the client instance return a response
	res, err := client.Info()

	if err != nil {
		l.Error("client info err", zap.Error(err))
	}
	l.Debug("es client info", zap.Any("info", res))

	// Declare empty array for the document strings
	var docs []string

	// Declare documents to be indexed using struct
	//doc1 := ElasticDocs{}
	//doc1.SomeStr = "Some Value"
	//doc1.SomeInt = 123456
	//doc1.SomeBool = true
	//
	//doc2 := ElasticDocs{}
	//doc2.SomeStr = "Another Value"
	//doc2.SomeInt = 42
	//doc2.SomeBool = false

	// Marshal Elasticsearch document struct objects to JSON string
	docStr1, err := jsonStruct(log_message.LogMessage{
		ServiceName: "test1",
		Message:     "test1",
		Payload:     "test1",
	})
	if err != nil {
		return
	}

	// Append the doc strings to an array
	docs = append(docs, docStr1)

	// Iterate the array of string documents
	for i, bod := range docs {
		fmt.Println("\nDOC _id:", i+1)
		fmt.Println(bod)

		// Instantiate a request object
		req := esapi.IndexRequest{
			Index:      "test_index",
			DocumentID: strconv.Itoa(i + 1),
			Body:       strings.NewReader(bod),
			Refresh:    "true",
		}
		fmt.Println(reflect.TypeOf(req))

		// Return an API response object from request
		res, err := req.Do(ctx, client)
		if err != nil {
			l.Error("do req err", zap.Error(err))
		}
		defer res.Body.Close()

		l.Debug("req result", zap.Any("res", res))
	}
}

//package main
//
//import (
//	"context"
//	"fmt"
//	"log"
//
//	"go.uber.org/zap"
//	"gopkg.in/olivere/elastic.v7"
//)
//
//const (
//	indexName  = "log_service_index"
//	elasticURL = "http://localhost:9200"
//)
//
//func main() {
//	l, err := NewLogger()
//	if err != nil {
//		log.Printf("create logger err = %s\n", err.Error())
//		return
//	}
//
//	client, err := GetESClient()
//	if err != nil {
//		l.Error("error creating es client", zap.Error(err))
//	}
//
//	exists, err := client.IndexExists(indexName).Do(context.Background())
//	if err != nil {
//		l.Error("error check if index exists", zap.Error(err))
//	}
//	if !exists {
//		if _, err := client.CreateIndex(indexName).Do(context.Background()); err != nil {
//			l.Error("error creating index", zap.Error(err))
//		}
//	}
//
//}
//
//func GetESClient() (*elastic.Client, error) {
//
//	client, err := elastic.NewClient(elastic.SetURL(elasticURL),
//		elastic.SetSniff(false),
//		elastic.SetHealthcheck(false))
//
//	fmt.Println("ES initialized...")
//
//	return client, err
//
//}
//
func NewLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{
		"./log_serivce.log",
	}
	return cfg.Build()
}
