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
	appVersion       = "d.3.13-cleanup"
	httpDefaultPort  = "8081"
	topicSubNotify   = "notify"
	topicPubDispatch = "dispatch"
	projectID        = "xallcloud"
)

// global resources for service
var dsClient *datastore.Client
var psClient *pubsub.Client
var tcSubNot *pubsub.Topic
var tcPubDis *pubsub.Topic
var sub *pubsub.Subscription

func main() {
	// service initialization
	log.SetFlags(log.Lshortfile)

	log.Println("Starting", appName, "version", appVersion)

	port := os.Getenv("PORT")
	if port == "" {
		port = httpDefaultPort
		log.Printf("Service: %s. Defaulting to port %s", appName, port)
	}

	var err error
	ctx := context.Background()

	// DATASTORE initialization
	log.Println("Connect to Google 'datastore' on project: " + projectID)
	dsClient, err = datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create datastore client: %v", err)
	}

	// PUBSUB initialization
	log.Println("Connect to Google 'pub/sub' on project: " + projectID)
	psClient, err = pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// topic to publish messages of type dispatch
	tcPubDis, err = gcp.CreateTopic(topicPubDispatch, psClient)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("Using topic %v to post notifications.\n", tcPubDis)

	// topic to subscribe messages of type notify
	tcSubNot, err = gcp.CreateTopic(topicPubDispatch, psClient)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Using topic %v to subscribe notify.\n", tcSubNot)

	// create subscriptions to topic notify
	sub, err = gcp.CreateSub(psClient, topicSubNotify, tcSubNot)
	if err != nil {
		log.Fatal(err)
	}

	// subscribe to incoming message in a different goroutine
	go subscribeTopicNotify()

	// HTTP Server initialization
	// define all the routes for the HTTP server.
	//   The implementation is done on the "handlers.go" files
	router := mux.NewRouter()
	router.HandleFunc("/api/version", getVersionHanlder).Methods("GET")
	router.HandleFunc("/", getStatusHanlder).Methods("GET")

	// start HTTP Server in new goroutine to allow other code to execute after
	go func() {
		log.Printf("Service: %s. Listening on port %s", appName, port)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), router))
	}()

	// clean up resources when service stops
	log.Printf("wait for signal to terminate everything on client %s\n", appName)
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			log.Printf("\nReceived an interrupt! Tearing down...\n\n")
			// Delete the subscription.
			fmt.Printf("delete subscription %s\n", topicSubNotify)
			if err := gcp.DeleteSubscription(psClient, topicSubNotify); err != nil {
				log.Fatal(err)
			}
			cleanupDone <- true
		}
	}()
	// wait for service to terminate
	<-cleanupDone
}
