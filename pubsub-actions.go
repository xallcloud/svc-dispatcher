package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/gogo/protobuf/proto"

	dst "github.com/xallcloud/api/datastore"
	pbt "github.com/xallcloud/api/proto"
	gcp "github.com/xallcloud/gcp"
)

func subscribeTopicNotify() {
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

		log.Printf("[subscribe] Got RAW message [%d]: %q\n", received, string(msg.Data))

		//decode message
		action, er := decodeRawAction(msg.Data)
		if er != nil {
			log.Printf("[subscribe] error decoding action message: %v\n", er)

			mu.Lock()
			failed++
			mu.Unlock()
			return
		}

		log.Printf("[subscribe] Process message (KeyID=%d) (AcID=%s)\n", action.KeyID, action.AcID)

		er = ProcessNewAction(action)
		if er != nil {
			log.Printf("[subscribe] error processing action: %v\n", er)
			mu.Lock()
			failed++
			mu.Unlock()
			return
		}
		log.Printf("[subscribe] DONE (KeyID=%d) (AcID=%s)\n", action.KeyID, action.AcID)
	})

	if err != nil {
		log.Fatal(err)
	}
}

//decodeRawAction Will decode raw data into proto Action format
func decodeRawAction(d []byte) (*pbt.Action, error) {
	log.Println("[decodeRawAction] Unmarshal")
	m := new(pbt.Action)
	err := proto.Unmarshal(d, m)
	if err != nil {
		return m, fmt.Errorf("unable to unserialize data. %v", err)
	}
	return m, nil
}

func publishNotification(n *pbt.Notification) {

	log.Printf("[publishNotification] [acID=%s] [NtID=%s] New Dispatch notification...", n.AcID, n.NtID)

	m, err := proto.Marshal(n)
	if err != nil {
		log.Printf("[publishNotification] unable to serialize data. %v", err)
		return
	}

	msg := &pubsub.Message{
		Data: m,
	}

	ctx := context.Background()

	var mID string
	mID, err = tcPubDis.Publish(ctx, msg).Get(ctx)
	if err != nil {
		log.Printf("[publishNotification] could not publish message. %v", err)
		return
	}

	log.Printf("[publishNotification] [acID=%s] [NtID=%s] New Dispatch notification %s. DONE!", n.AcID, n.NtID, mID)

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
