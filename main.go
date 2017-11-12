package main

import (
	"database/sql"
	"net/http"
	"os"
	"strings"
	"time"

	healthz "github.com/MEDIGO/go-healthz"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/kelseyhightower/envconfig"
	"github.com/lib/pq"
	"github.com/tj/go-kinesis"
)

var usage = `
    PG Bridge: Send Postgres notifications to kinesis stream

    Usage:

      pg-bridge
			PGB_ROUTES, PGB_POSTGRESQL_URL and PGB_KINESIS_STREAM_NAME env vars at least must be setted
			defaults for health check is HOST:5000/health
			
    Options:

      -h, --help              Show this screen
      -v, --version           Get the version
`

type Route struct {
	Channel string
	Topic   string
}

type PgbridgeConfig struct {
	Routes               string        `required:"true"`
	PostgresqlUrl        string        `split_words:"true" required:"true"`
	KinesisStreamName    string        `split_words:"true" required:"true"`
	KinesisBufferSize    int           `split_words:"true" default:"10"`
	KinesisFlushInterval time.Duration `split_words:"true" default:"10s"`
}

func setupRoutesFromEnv() map[string]Route {
	routesEnv := os.Getenv("PGB_ROUTES")
	if len(routesEnv) == 0 {
		log.Fatal("PGB_ROUTES env var is mandatory to handle routing")
	}

	routing := strings.Split(routesEnv, ";")

	routes := make(map[string]Route)
	for i := range routing {
		rs := strings.Split(routing[i], "|")
		if len(rs) == 2 {
			cr := Route{Channel: rs[0], Topic: rs[1]}
			routes[cr.Channel] = cr
		}
	}

	return routes
}

func processNotification(notif *pq.Notification, routes map[string]Route, producer *kinesis.Producer) {
	notChan := notif.Channel
	_, exists := routes[notChan]
	if exists {
		currRoute := routes[notChan]
		ba := []byte(notif.Extra)
		err := producer.Put(ba, currRoute.Topic)
		if err != nil {
			log.WithError(err).Fatal("error producing")
		}
	}
}

func main() {

	var cfg PgbridgeConfig
	err := envconfig.Process("pgb", &cfg)
	if err != nil {
		log.Fatal(err.Error())
	}

	// setup Env Vars
	pgUrl := cfg.PostgresqlUrl
	if len(pgUrl) == 0 {
		println("PGB_POSTGRESQL_URL env var is mandatory")
		os.Exit(1)
	}
	streamName := cfg.KinesisStreamName
	if len(streamName) == 0 {
		println("PGB_STREAM_NAME env var is mandatory")
		os.Exit(1)
	}

	log.SetHandler(text.New(os.Stderr))

	routes := setupRoutesFromEnv()

	pg := ConnectPostgres(pgUrl, routes)
	defer pg.Close()

	// Setup Kinesis
	producer := kinesis.New(kinesis.Config{
		StreamName:    streamName,
		FlushInterval: cfg.KinesisFlushInterval,
		BufferSize:    cfg.KinesisBufferSize,
	})
	producer.Start()
	defer producer.Stop()

	//Activate health check
	go HTTP(pg)

	for {
		n := <-pg.Notify
		log.WithField("payload", n.Extra).Infof("notification received from %s", n.Channel)
		go processNotification(n, routes, producer)
	}
}

// ConnectPostgres connect to postgres
func ConnectPostgres(postgresUrl string, routes map[string]Route) *pq.Listener {

	log.Infof("connecting to postgres: %s...", postgresUrl)
	client, err := sql.Open("postgres", postgresUrl)
	if err != nil {
		log.WithError(err).Fatal("could not connect to postgres")
	}
	log.Infof("connected to postgres server")

	if err := client.Ping(); err != nil {
		log.WithError(err).Fatal("error connecting to postgres")
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.WithError(err).Fatal("error listening for notifications")
		}
	}

	log.Infof("setting up postgresql listener...")
	listener := pq.NewListener(postgresUrl, 10*time.Second, time.Minute, reportProblem)
	log.Infof("pg listener created ok")

	// listen on each channel
	for _, route := range routes {
		log.Infof("listening on channel: '%s'", route.Channel)
		err := listener.Listen(route.Channel)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	return listener
}

// HTTP Health simple healthcheck service
func HTTP(pg *pq.Listener) *http.ServeMux {

	healthz.Register("postgres", time.Second*5, func() error {
		return pg.Ping()
	})

	mux := http.NewServeMux()

	path := os.Getenv("PGB_HEALTH_PATH")
	if path == "" {
		path = "/health"
	}

	port := os.Getenv("PGB_HEALTH_PORT")
	if port == "" {
		port = "5000"
	}

	mux.Handle(path, healthz.Handler())
	http.ListenAndServe(":"+port, mux)
	return mux
}
