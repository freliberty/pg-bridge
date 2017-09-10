package main

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"strings"
	"strconv"

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
	"github.com/oleiade/lane"
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

func processNotification(notif *pq.Notification, routes map[string]Route, pub *sns.SNS) {
	notChan := notif.Channel
	_, exists := routes[notChan]
	if exists {
		currRoute := routes[notChan]
		if strings.HasPrefix(currRoute.Topic, "http") {
			go publishHTTP(notif.Channel, currRoute.Topic, notif.Extra)
		} else {
			go publishSNS(pub, notif.Channel, currRoute.Topic, notif.Extra)
		}
	}
}

func main() {
	log.SetHandler(text.New(os.Stderr))

	routes := setupRoutesFromEnv()

	//define max queue size
	maxQueueSize := 50
	queueSize := os.Getenv("PGB_MAX_QUEUESIZE")
	if queueSize != "" {
		maxQueueSize, _ = strconv.Atoi(queueSize)
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

	notifsQueue := lane.NewQueue()
	// route the notifications

	for {
		n := <-pg.Notify
		log.WithField("payload", n.Extra).Infof("notification received from %s", n.Channel)
		notifsQueue.Enqueue(n)

		if notifsQueue.Size() == maxQueueSize {
			for notifsQueue.Head() != nil {
				notif := notifsQueue.Dequeue().(*pq.Notification)
				processNotification(notif, routes, pub)
			}
		}
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
