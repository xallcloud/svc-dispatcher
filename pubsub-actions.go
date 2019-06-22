package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/gogo/protobuf/proto"

	dst "github.com/xallcloud/api/datastore"
	pbt "github.com/xallcloud/api/proto"
	gcp "github.com/xallcloud/gcp"
)

//subscribeTopicNotify will create a subscription to new notify
//  and process the basic action
func subscribeTopicNotify() {
	log.Printf("[subscribe] starting goroutine: %s | %s\n", sub.String(), tcSubNot.String())
	ctx := context.Background()
	err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		// always acknowledge message
		msg.Ack()

		log.Printf("[subscribe] Got RAW message: %q\n", string(msg.Data))

		//decode notification message into proper format
		action, er := decodeRawAction(msg.Data)
		if er != nil {
			log.Printf("[subscribe] error decoding action message: %v\n", er)
			return
		}

		log.Printf("[subscribe] Process message (KeyID=%d) (AcID=%s)\n", action.KeyID, action.AcID)

		er = ProcessNewAction(action)
		if er != nil {
			log.Printf("[subscribe] error processing action: %v\n", er)
			return
		}
		log.Printf("[subscribe] DONE (KeyID=%d) (AcID=%s)\n", action.KeyID, action.AcID)
	})

	if err != nil {
		log.Fatal(err)
	}
}

//decodeRawAction will decode raw data into proto Action format
func decodeRawAction(d []byte) (*pbt.Action, error) {
	log.Println("[decodeRawAction] Unmarshal")
	m := new(pbt.Action)
	err := proto.Unmarshal(d, m)
	if err != nil {
		return m, fmt.Errorf("unable to unserialize data. %v", err)
	}
	return m, nil
}

//publishNotification will publish a new notification message to pubsub
func publishNotification(n *pbt.Notification) {

	log.Printf("[publishNotification] [acID=%s] [NtID=%s] New Dispatch notification...", n.AcID, n.NtID)

	//put message in raw format
	m, err := proto.Marshal(n)
	if err != nil {
		log.Printf("[publishNotification] unable to serialize data. %v", err)
		return
	}
	msg := &pubsub.Message{
		Data: m,
	}

	//publish action
	ctx := context.Background()
	var mID string
	mID, err = tcPubDis.Publish(ctx, msg).Get(ctx)
	if err != nil {
		log.Printf("[publishNotification] could not publish message. %v", err)
		return
	}
	log.Printf("[publishNotification] [acID=%s] [NtID=%s] New Dispatch notification %s. DONE!", n.AcID, n.NtID, mID)

	//add to events
	e := &dst.Event{
		NtID:          n.NtID,
		CpID:          n.CpID,
		DvID:          n.DvID,
		Visibility:    gcp.VisibilityServer,
		EvType:        gcp.EvTypeServices,
		EvSubType:     "pubsub",
		EvDescription: "Notification sent to svc-notify.",
	}
	addNewEvent(ctx, e)
}
