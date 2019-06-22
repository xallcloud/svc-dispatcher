package main

import (
	"context"
	"fmt"
	"log"

	dst "github.com/xallcloud/api/datastore"
	pbt "github.com/xallcloud/api/proto"
	gcp "github.com/xallcloud/gcp"
)

// ProcessNewAction will process new notifications from and start the process
//   of initializing it, and deliver it after
func ProcessNewAction(a *pbt.Action) error {
	ctx := context.Background()

	// first check if action exists
	log.Println("[ProcessNewAction] check if action exists acID:", a.AcID)
	actions, errAcs := gcp.ActionGetByAcID(ctx, dsClient, a.AcID)
	if errAcs != nil {
		return errAcs
	}
	if len(actions) <= 0 {
		return fmt.Errorf("[acID=%s] not found in actions datastore", a.AcID)
	}

	// also check if there is a matching callpoint
	log.Println("[ProcessNewAction] check if callpoint exists acID:", a.CpID)
	callpoints, errCps := gcp.CallpointGetByCpID(ctx, dsClient, a.CpID)
	if errCps != nil {
		return errCps
	}
	if len(callpoints) <= 0 {
		return fmt.Errorf("[cpID=%s] not found in actions datastore", a.CpID)
	}

	// also check if this callpoint has assignmnets
	log.Println("[ProcessNewAction] check if assignment exists by cpID:", a.CpID)
	assignments, errAsgns := gcp.AssignmentsByCpID(ctx, dsClient, a.CpID)
	if errAsgns != nil {
		return errAsgns
	}
	if len(assignments) <= 0 {
		log.Println("[ProcessNewAction] No assigments! do nothing", a.AcID)
		return fmt.Errorf("no assigments found for this callpoint [cpID=%s]", a.CpID)
	}

	//create individual notifications for each individual assignment to device
	for _, as := range assignments {
		log.Println("[ProcessNewAction] creating new notification: ", len(assignments))

		n := &dst.Notification{
			AcID:          a.AcID,
			Priority:      as.CallpointObj.Priority,
			Category:      as.DeviceObj.Category,
			Destination:   as.DeviceObj.Settings,
			Message:       as.CallpointObj.Label + ". " + as.CallpointObj.Description,
			ResponseTitle: "Please select one of the options",
			Options:       "ack,cancel",
		}
		dsn, errN := gcp.NotificationAdd(ctx, dsClient, n)
		if errN != nil {
			log.Println("[ProcessNewAction] error inserting new notification: ", errN)
		} else {
			//insert into events
			e := &dst.Event{
				NtID:          dsn.NtID,
				CpID:          as.CpID,
				DvID:          as.DvID,
				Visibility:    gcp.VisibilityAll,
				EvType:        gcp.EvTypeStart,
				EvSubType:     gcp.EvSubTypeStartStep1,
				EvDescription: "Notification started",
			}
			addNewEvent(ctx, e)

			//publish new instruction to start notification of message
			pn := &pbt.Notification{
				NtID:          dsn.NtID,
				AcID:          dsn.AcID,
				Priority:      dsn.Priority,
				Category:      dsn.Category,
				Destination:   dsn.Destination,
				Message:       dsn.Message,
				ResponseTitle: dsn.ResponseTitle,
				Options:       dsn.Options,
				CpID:          as.CpID,
				DvID:          as.DvID,
			}
			publishNotification(pn)
		}
	}

	log.Println("[ProcessNewAction] DONE! No errors.")

	return nil
}

func addNewEvent(ctx context.Context, ev *dst.Event) {
	log.Println("[addNewEvent] add new event regarding notification: ", ev.NtID)
	gcp.EventAdd(ctx, dsClient, ev)
}
