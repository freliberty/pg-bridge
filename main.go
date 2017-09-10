package main

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"strings"

	"net/http"
	"os"
	"time"

	healthz "github.com/MEDIGO/go-healthz"
	"github.com/apex/log"
	"github.com/apex/log/handlers/text"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/lib/pq"
)

var usage = `
    PG Bridge: Send Postgres notifications to SNS or to a webhook.

    Usage:

      pg-bridge

			PGB_ROUTES and PGB_POSTGRESQL_URL env vars at least must be setted
			defaults for health check is HOST:5000/health
			
    Options:

      -h, --help              Show this screen
      -v, --version           Get the version
`

type Route struct {
	Channel string
	Topic   string
}

func main() {
	log.SetHandler(text.New(os.Stderr))

	routesEnv := os.Getenv("PGB_ROUTES")
	if len(routesEnv) == 0 {
		log.Fatal("PGB_ROUTES env var is mandatory to handle routing")
	}

	routing := strings.Split(routesEnv, ";")
	routes := []Route{}
	for i := range routing {
		rs := strings.Split(routing[i], "|")
		if len(rs) == 2 {
			cr := Route{Channel: rs[0], Topic: rs[1]}
			routes = append(routes, cr)
		}
	}

	// setup Postgres
	pgUrl := os.Getenv("PGB_POSTGRESQL_URL")
	if len(pgUrl) == 0 {
		println("PGB_POSTGRESQL_URL env var is mandatory")
		os.Exit(1)
	}
	pg := ConnectPostgres(pgUrl, routes)
	defer pg.Close()

	// Setup SNS
	// @TODO figure out how to check that the required
	// variables are actually present in the session
	pub := sns.New(session.New())

	//Activate health check
	go HTTP(pg)

	// route the notifications
	for {
		n := <-pg.Notify
		log.WithField("payload", n.Extra).Infof("notification received from %s", n.Channel)

		for _, rt := range routes {
			if rt.Channel == n.Channel {
				topic := rt.Topic
				if strings.HasPrefix(topic, "http") {
					go publishHTTP(n.Channel, topic, n.Extra)
				} else {
					go publishSNS(pub, n.Channel, topic, n.Extra)
				}
			}
		}
	}
}

// publish payload to SNS
func publishSNS(pub *sns.SNS, channel string, topic string, payload string) {
	SNSPayload := &sns.PublishInput{
		Message:  aws.String(payload),
		TopicArn: aws.String(topic),
	}

	_, err := pub.Publish(SNSPayload)
	if err != nil {
		log.WithError(err).WithField("channel", channel).WithField("payload", payload).Error("unable to send payload to SNS")
	}

	log.Infof("delivered notification from %s to SNS", channel)
	return
}

func publishHTTP(channel string, topic string, payload string) {
	body := []byte(payload)

	req, err := http.NewRequest("POST", topic, bytes.NewBuffer(body))
	if err != nil {
		log.WithError(err).Error("error POSTing")
		return
	}

	req.Header.Set("Content-Type", "application/json")

	log.Info("POSTing...")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.WithError(err).Error("unable to POST")
		return
	}
	defer resp.Body.Close()

	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Error("cannot read body")
		return
	}

	log.Infof("delivered notification from %s to %s with this response: %s", channel, topic, resp.Status)
}

// ConnectPostgres connect to postgres
func ConnectPostgres(postgresUrl string, routes []Route) *pq.Listener {

	if postgresUrl == "" {
		log.Fatal("postgres url is mandatory")
	}

	log.Infof("connecting to postgres: %s...", postgresUrl)
	client, err := sql.Open("postgres", postgresUrl)
	if err != nil {
		log.WithError(err).Fatal("could not connect to postgres")
	}
	log.Infof("connected to postgres")

	if err := client.Ping(); err != nil {
		log.WithError(err).Fatal("error connecting to postgres")
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.WithError(err).Fatal("error listening for notifications")
		}
	}

	log.Infof("setting up a listener...")
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
