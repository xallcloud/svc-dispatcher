package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"

	pbt "github.com/xallcloud/api/proto"
	gcp "github.com/xallcloud/gcp"
)

const (
	appName          = "svc-dispatcher"
	appVersion       = "0.0.1-alfa003-subscriptions"
	httpPort         = "8081"
	topicPubDispatch = "dispatch"
	topicSubNotify   = "notify"
	topicSubReply    = "reply"
	projectID        = "xallcloud"
)

var dsClient *datastore.Client
var psClient *pubsub.Client
var tcSubNot *pubsub.Topic
var sub *pubsub.Subscription

func main() {
	/////////////////////////////////////////////////////////////////////////
	// Setup
	/////////////////////////////////////////////////////////////////////////
	port := os.Getenv("PORT")
	if port == "" {
		port = httpPort
		log.Printf("Service: %s. Defaulting to port %s", appName, port)
	}

	var err error
	ctx := context.Background()

	// PUBSUB Initialization
	log.Println("Connect to Google 'pub/sub' on project: " + projectID)
	psClient, err = pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	//topic to subscribe
	tcSubNot, err = gcp.CreateTopic(topicPubDispatch, psClient)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Using topic %v to subscribe notify.\n", tcSubNot)

	// create subscriptions tp new topic
	sub, err = gcp.CreateSub(psClient, topicSubNotify, tcSubNot)
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Printf(" %v to subscribe notify.\n", sub)

	actionsChannel := make(chan *pbt.Action)

	go subscribe(actionsChannel)

	/////////////////////////////////////////////////////////////////////////
	// HTTP SERVER
	/////////////////////////////////////////////////////////////////////////

	go func() {
		for {
			select {
			case a := <-actionsChannel:
				log.Printf("[CHANNEL]: Got new action: [acID:%s] [cpID:%s] [action:%s] %s", a.AcID, a.CpID, a.Action)

				//apply some rules here by procesing event.

				// 1) Build notifyDevice Message
				/*
					n := pbt.Notification{
						Cmd:                "notifyDevice",
						EventId:            "newID+" + a.EventId,
						ParentEventId:      a.EventId,
						Priority:           a.Priority,
						TerminalType:       a.Destinations[0].Terminals[0].Type,
						DestinationLabel:   a.Destinations[0].Label,
						DestinationAddress: a.Destinations[0].Terminals[0].Properties[0].Value,
						Message:            a.Message,
						Options:            a.Options,
					}
					ctx := context.Background()

					PubNotifyDevice(ctx, client, &n)
				*/
			}
		}

	}()

	/////////////////////////////////////////////////////////////////////////
	// HTTP SERVER
	/////////////////////////////////////////////////////////////////////////
	router := mux.NewRouter()
	router.HandleFunc("/api/version", getVersionHanlder).Methods("GET")
	router.HandleFunc("/", getStatusHanlder).Methods("GET")

	go func() {
		log.Printf("Service: %s. Listening on port %s", appName, port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
	}()
	//temporary
	/////////////////////////////////////////////////////////////////////////
	//
	/////////////////////////////////////////////////////////////////////////
	select {}
}

func getVersionHanlder(w http.ResponseWriter, r *http.Request) {
	log.Println("[/version:GET] Requested api version.")

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, fmt.Sprintf(`{"service": "%s", "version": "%s"}`, appName, appVersion))
}

func getStatusHanlder(w http.ResponseWriter, r *http.Request) {
	log.Println("[/:GET] Requested service status.")

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, fmt.Sprintf(`{"service": "%s", "status":"running", version": "%s"}`, appName, appVersion))
}

func pullMsgs(client *pubsub.Client, sub *pubsub.Subscription, topic *pubsub.Topic) error {
	log.Printf("[pullMsgs] starting: %s | %s\n", sub.String(), topic.String())
	ctx := context.Background()

	var mu sync.Mutex
	received := 0
	cctx, cancel := context.WithCancel(ctx)

	log.Printf("[pullMsgs] before Receive %v\n", sub)

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		log.Printf("Got RAW message : %q\n", string(msg.Data))

		//decode message
		a, er := DecRawNotification(msg.Data)

		if er != nil {
			log.Printf("[sub.Receive] error decoding message: %v\n", er)
		}

		log.Printf("[sub.Receive] Process message [acID:%s]\n", a.AcID)

		mu.Lock()
		defer mu.Unlock()
		received++
		if received == 1 {
			cancel()
		}
	})

	if err != nil {
		return err
	}

	return nil
}

func subscribe(nc chan *pbt.Action) {
	log.Printf("[subscribe] starting goroutine: %s | %s\n", sub.String(), tcSubNot.String())

	var mu sync.Mutex
	received := 0
	failed := 0
	ctx := context.Background()
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()

		mu.Lock()
		received++
		mu.Unlock()

		log.Printf("Got RAW message [%d]: %q\n", received, string(msg.Data))

		//decode message
		notif, er := DecRawNotification(msg.Data)
		if er != nil {
			log.Printf("error decoding message: %v\n", er)

			mu.Lock()
			failed++
			mu.Unlock()
			return
		}

		log.Printf("Process message (KeyID=%d) (AcID=%s)\n", notif.KeyID, notif.AcID)

		e := ProcessNewNotification(notif)

		if e != nil {
			log.Printf("error processing message: %v\n", er)
			mu.Lock()
			failed++
			mu.Unlock()
			return
		}

		nc <- notif
	})

	if err != nil {
		log.Fatal(err)
	}
}

//DecRawNotification Will decode raw data into proto Notification
func DecRawNotification(d []byte) (*pbt.Action, error) {
	log.Println("[DecRawNotification] Unmarshal")
	m := new(pbt.Action)
	err := proto.Unmarshal(d, m)
	if err != nil {
		return m, fmt.Errorf("unable to unserialize data. %v", err)
	}
	return m, nil
}

// ProcessNewNotification will process new notifications from and start the process
//   of initializing it, and deliver it after
func ProcessNewNotification(n *pbt.Action) error {
	//first all, check the database for the record:
	log.Println("[ProcessNewNotification] TODO...")

	return nil
}

// PubNotifyDevice does something
func PubNotifyDevice(ctx context.Context, client *pubsub.Client, n *pbt.Notification) error {
	log.Printf("[PubNotifyDevice] [eventId=%s] New Notification.", n.NtID)

	/*
		m, err := proto.Marshal(n)
		if err != nil {
			return fmt.Errorf("unable to serialize data. %v", err)
		}

		msg := &pubsub.Message{
			Data: m,
		}
		var mID string
		mID, err = topicPub.Publish(ctx, msg).Get(ctx)
		if err != nil {
			return fmt.Errorf("could not publish message. %v", err)
		}

		log.Printf("[PubNotifyDevice] [eventId=%s] New NotifyDevice published. [mID=%s]", a.EventId, mID)
	*/
	return nil
}
