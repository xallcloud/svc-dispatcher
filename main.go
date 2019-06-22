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

	gcp "github.com/xallcloud/gcp"
)

const (
	appName          = "svc-dispatcher"
	appVersion       = "d.1.11-msgs"
	httpPort         = "8081"
	topicSubNotify   = "notify"
	topicPubDispatch = "dispatch"
	projectID        = "xallcloud"
)

var dsClient *datastore.Client
var psClient *pubsub.Client
var tcSubNot *pubsub.Topic
var tcPubDis *pubsub.Topic
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

	///////////////////////////////////////////////////////////////////
	// topic to publish messages
	tcPubDis, err = gcp.CreateTopic(topicPubDispatch, psClient)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Using topic %v to post notifications.\n", tcPubDis)

	///////////////////////////////////////////////////////////////////
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

	// subscribe to incoming message
	go subscribeTopicNotify()

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
