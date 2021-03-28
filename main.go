package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/log-service/lib/kafka"
	"github.com/ory/viper"
	"go.uber.org/zap"
)

func main() {
	l, err := newLogger()
	if err != nil {
		log.Printf("create logger err = %s\n", err.Error())
		return
	}

	defer func() {
		if err := recover(); err != nil {
			l.Error("panic occurred:", zap.Any("err", err))
		}
	}()
	defer func() {
		l.Info("log-service stopped")
	}()

	l.Info("log-service started")

	viper.SetConfigFile("config.yaml")
	if err := viper.ReadInConfig(); err != nil {
		l.Error("read config", zap.Error(err))
		return
	}

	cfg := &kafka.Config{}
	if err := viper.GetViper().Unmarshal(cfg); err != nil {
		l.Error("unmarshal config", zap.Error(err))
		return
	}
	if err := cfg.Check(); err != nil {
		l.Error("config check err", zap.Error(err))
		return
	}

	startService(l, cfg)
}

func startService(l *zap.Logger, cfg *kafka.Config) {
	ctx, cancel := context.WithCancel(context.Background())

	if err := kafka.Run(ctx, l, cfg); err != nil {
		l.Error("kafka run finished with err", zap.Error(err))
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	s := <-c
	l.Info("Got signal, stopping", zap.String("signal", s.String()))

	cancel()
	l.Info("Service stopped")
}

func newLogger() (*zap.Logger, error) {
	cfg := zap.NewDevelopmentConfig()
	cfg.OutputPaths = []string{
		"./log_service.log",
	}
	return cfg.Build()
}

//cfg := elasticsearch.Config{
//	Addresses: []string{
//		"http://localhost:9200",
//	},
//	Transport: &http.Transport{
//		ResponseHeaderTimeout: 10 * time.Second,
//		IdleConnTimeout:       10 * time.Second,
//	},
//}
//
//// Instantiate a new Elasticsearch client object instance
//client, err := elasticsearch.NewClient(cfg)
//
//if err != nil {
//	l.Error("error creating es client", zap.Error(err))
//}
//
//// Have the client instance return a response
//res, err := client.Info()
//
//if err != nil {
//	l.Error("client info err", zap.Error(err))
//}
//l.Debug("es client info", zap.Any("info", res))
//
//var docs []string
//
//docStr1, err := jsonStruct(log_message.LogMessage{
//	ServiceName: "test2",
//	Message:     "test2",
//	Payload:     "test2",
//})
//if err != nil {
//	return
//}
//
//docs = append(docs, docStr1)
//
//for _, bod := range docs {
//
//	req := esapi.IndexRequest{
//		Index:      "test_index2",
//		DocumentID: strconv.Itoa(rand.Int()),
//		Body:       strings.NewReader(bod),
//		Refresh:    "true",
//	}
//	fmt.Println(reflect.TypeOf(req))
//
//	res, err := req.Do(context.Background(), client)
//	if err != nil {
//		l.Error("do req err", zap.Error(err))
//	}
//	defer res.Body.Close()
//
//	l.Debug("req result", zap.Any("res", res))
//}
//}

//func jsonStruct(docStruct log_message.LogMessage) (string, error) {
//
//	b, err := json.Marshal(docStruct)
//	if err != nil {
//		return "", err
//	}
//	return string(b), nil
//}
