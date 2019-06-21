package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"github.com/gorilla/mux"

	pbt "github.com/xallcloud/api/proto"
	gcp "github.com/xallcloud/gcp"
)

const (
	appName          = "svc-dispatcher"
	appVersion       = "0.0.1-alfa.6-touch-db"
	httpPort         = "8081"
	topicPubDispatch = "dispatch"
	topicSubNotify   = "notify"
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
	// DATASTORE Initialization
	log.Println("Connect to Google 'datastore' on project: " + projectID)

	dsClient, err = datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create datastore client: %v", err)
	}

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

	notificationsChannel := make(chan *pbt.Notification)

	go subscribe(actionsChannel, notificationsChannel)

	/////////////////////////////////////////////////////////////////////////
	// HTTP SERVER
	/////////////////////////////////////////////////////////////////////////

	go func() {
		for {
			select {
			case a := <-actionsChannel:
				log.Printf("[CHANNEL]: New action received on this channel [acID:%s]\n", a.AcID)
				log.Printf("[CHANNEL]: New action: [acID:%s] [cpID:%s] [action:%s] END TEXT!\n", a.AcID, a.CpID, a.Action)

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
			case n := <-notificationsChannel:
				log.Printf("[CHANNEL]: New action received on this channel [ntID:%s]\n", n.NtID)
				log.Printf("[CHANNEL]: Publish new message to device [Message:%s]\n", n.Message)
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
	// select {}
	/////////////////////////////////////////////////////////////////////////
	// logic
	/////////////////////////////////////////////////////////////////////////

	log.Printf("wait for signal to terminate everything on client %s\n", appName)
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			log.Printf("\nReceived an interrupt! Tearing down...\n\n")

			// Delete the subscription.
			fmt.Printf("delete subscription %s\n", topicSubNotify)
			if err := delete(psClient, topicSubNotify); err != nil {
				log.Fatal(err)
			}

			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
