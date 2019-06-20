package main

import (
	"context"
	"fmt"
	"log"

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
	acs, err := gcp.ActionGetByAcID(ctx, dsClient, a.AcID)
	if err != nil {
		return err
	}

	if len(acs) <= 0 {
		return fmt.Errorf("[acID=%s] not found in actions datastore", a.AcID)
	}

	log.Println("[ProcessNewAction] check if callpoint exists acID:", a.CpID)

	// get callpoint
	cps, err := gcp.CallpointGetByCpID(ctx, dsClient, a.CpID)
	if err != nil {
		return err
	}

	if len(cps) <= 0 {
		return fmt.Errorf("[cpID=%s] not found in actions datastore", a.CpID)
	}

	log.Println("[ProcessNewAction] check if assignment exists by cpID:", a.CpID)

	// first check if there already exists this Callpoint ID:
	asgns, err := gcp.AssignmentsByCpID(ctx, dsClient, a.CpID)
	if err != nil {
		return err
	}

	if len(asgns) <= 0 {
		return fmt.Errorf("[cpID=%s] not found in assignments datastore", a.CpID)
	}

	log.Println("[ProcessNewAction] found assignments: ", len(asgns))

	log.Println("[ProcessNewAction] check if notification exists acID:", a.AcID)

	// Is in DB?
	acs, err := gcp.NotificationGetByAcID(ctx, dsClient, a.AcID)
	if err != nil {
		return err
	}

	if len(acs) <= 0 {
		return fmt.Errorf("[acID=%s] not found in actions datastore", a.AcID)
	}

	return nil
}
