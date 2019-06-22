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
	//first all, check the database for the record:
	log.Println("[ProcessNewAction] TODO...")

	ctx := context.Background()

	log.Println("[ProcessNewAction] check if action exists acID:", a.AcID)

	// Is in DB?
	actions, errAcs := gcp.ActionGetByAcID(ctx, dsClient, a.AcID)
	if errAcs != nil {
		return errAcs
	}

	if len(actions) <= 0 {
		return fmt.Errorf("[acID=%s] not found in actions datastore", a.AcID)
	}

	log.Println("[ProcessNewAction] check if callpoint exists acID:", a.CpID)

	// get callpoint
	callpoints, errCps := gcp.CallpointGetByCpID(ctx, dsClient, a.CpID)
	if errCps != nil {
		return errCps
	}

	if len(callpoints) <= 0 {
		return fmt.Errorf("[cpID=%s] not found in actions datastore", a.CpID)
	}

	log.Println("[ProcessNewAction] check if assignment exists by cpID:", a.CpID)

	// first check if there already exists this Callpoint ID:
	assignments, errAsgns := gcp.AssignmentsByCpID(ctx, dsClient, a.CpID)
	if errAsgns != nil {
		return errAsgns
	}

	log.Println("[ProcessNewAction] found assignments: ", len(assignments))

	if len(assignments) <= 0 {
		log.Println("[ProcessNewAction] No assigments! do nothing", a.AcID)
		return fmt.Errorf("no assigments found for this callpoint [cpID=%s]", a.CpID)
	}

	log.Println("[ProcessNewAction] check if notification exists acID:", a.AcID)

	notifications, errNts := gcp.NotificationsGetByAcID(ctx, dsClient, a.AcID)
	if errNts != nil {
		return errNts
	}

	log.Println("[ProcessNewAction] total notifications in datastore: ", len(notifications))

	//create individual notifications for each individual assignments to device
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

	log.Println("[ProcessNewAction] DONE! no errors.")

	return nil
}

func addNewEvent(ctx context.Context, ev *dst.Event) {
	log.Println("[addNewEvent] add new event regarding notification: ", ev.NtID)
	gcp.EventAdd(ctx, dsClient, ev)
}
