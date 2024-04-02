package ctasks

import (
	"context"
	"log/slog"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/triggers"
	"github.com/nyaruka/mailroom/core/models"
	"github.com/nyaruka/mailroom/core/runner"
	"github.com/nyaruka/mailroom/core/tasks/handler"
	"github.com/nyaruka/mailroom/runtime"
	"github.com/pkg/errors"
)

const TypeTicketClosed = "ticket_closed"

func init() {
	handler.RegisterContactTask(TypeTicketClosed, func() handler.Task { return &TicketClosedTask{} })
}

type TicketClosedTask struct {
	TicketID models.TicketID `json:"ticket_id"`
}

func NewTicketClosed(ticketID models.TicketID) *TicketClosedTask {
	return &TicketClosedTask{TicketID: ticketID}
}

func (t *TicketClosedTask) Type() string {
	return TypeTicketClosed
}

func (t *TicketClosedTask) UseReadOnly() bool {
	return false
}

func (t *TicketClosedTask) Perform(ctx context.Context, rt *runtime.Runtime, oa *models.OrgAssets, contact *models.Contact) error {
	// if contact is deleted, nothing to do
	if contact == nil {
		return nil
	}

	// load our ticket
	tickets, err := models.LoadTickets(ctx, rt.DB, []models.TicketID{t.TicketID})
	if err != nil {
		return errors.Wrapf(err, "error loading ticket")
	}
	// ticket has been deleted ignore this event
	if len(tickets) == 0 {
		return nil
	}

	// build our flow contact
	flowContact, err := contact.FlowContact(oa)
	if err != nil {
		return errors.Wrapf(err, "error creating flow contact")
	}

	// do we have associated trigger?
	trigger := models.FindMatchingTicketClosedTrigger(oa, flowContact)

	// no trigger, noop, move on
	if trigger == nil {
		slog.Info("ignoring ticket closed event, no trigger found", "ticket_id", t.TicketID)
		return nil
	}

	// load our flow
	flow, err := oa.FlowByID(trigger.FlowID())
	if err == models.ErrNotFound {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "error loading flow for trigger")
	}

	// if this is an IVR flow, we need to trigger that start (which happens in a different queue)
	if flow.FlowType() == models.FlowTypeVoice {
		err = handler.TriggerIVRFlow(ctx, rt, oa.OrgID(), flow.ID(), []models.ContactID{contact.ID()}, nil)
		if err != nil {
			return errors.Wrapf(err, "error while triggering ivr flow")
		}
		return nil
	}

	// build our flow ticket
	ticket := tickets[0].FlowTicket(oa)

	// build our flow trigger
	flowTrigger := triggers.NewBuilder(oa.Env(), flow.Reference(), flowContact).
		Ticket(ticket, triggers.TicketEventTypeClosed).
		Build()

	_, err = runner.StartFlowForContacts(ctx, rt, oa, flow, []*models.Contact{contact}, []flows.Trigger{flowTrigger}, nil, flow.FlowType().Interrupts())
	if err != nil {
		return errors.Wrapf(err, "error starting flow for contact")
	}
	return nil
}
