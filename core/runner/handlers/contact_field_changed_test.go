package handlers_test

import (
	"testing"

	"github.com/nyaruka/goflow/assets"
	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/actions"
	"github.com/nyaruka/mailroom/core/runner/handlers"
	"github.com/nyaruka/mailroom/testsuite"
	"github.com/nyaruka/mailroom/testsuite/testdata"
)

func TestContactFieldChanged(t *testing.T) {
	ctx, rt := testsuite.Runtime()

	defer testsuite.Reset(testsuite.ResetAll)

	gender := assets.NewFieldReference("gender", "Gender")
	age := assets.NewFieldReference("age", "Age")

	// populate some field values on alexandria
	rt.DB.MustExec(`UPDATE contacts_contact SET fields = '{"903f51da-2717-47c7-a0d3-f2f32877013d": {"text":"34"}, "3a5891e4-756e-4dc9-8e12-b7a766168824": {"text":"female"}}' WHERE id = $1`, testdata.Alexandria.ID)

	tcs := []handlers.TestCase{
		{
			Actions: handlers.ContactActionMap{
				testdata.Cathy: []flows.Action{
					actions.NewSetContactField(handlers.NewActionUUID(), gender, "Male"),
					actions.NewSetContactField(handlers.NewActionUUID(), gender, "Female"),
					actions.NewSetContactField(handlers.NewActionUUID(), age, ""),
				},
				testdata.George: []flows.Action{
					actions.NewSetContactField(handlers.NewActionUUID(), gender, "Male"),
					actions.NewSetContactField(handlers.NewActionUUID(), gender, ""),
					actions.NewSetContactField(handlers.NewActionUUID(), age, "40"),
				},
				testdata.Bob: []flows.Action{
					actions.NewSetContactField(handlers.NewActionUUID(), gender, ""),
					actions.NewSetContactField(handlers.NewActionUUID(), gender, "Male"),
					actions.NewSetContactField(handlers.NewActionUUID(), age, "Old"),
				},
				testdata.Alexandria: []flows.Action{
					actions.NewSetContactField(handlers.NewActionUUID(), age, ""),
					actions.NewSetContactField(handlers.NewActionUUID(), gender, ""),
				},
			},
			SQLAssertions: []handlers.SQLAssertion{
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Female"}'::jsonb`,
					Args:  []any{testdata.Cathy.ID, testdata.GenderField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []any{testdata.Cathy.ID, testdata.AgeField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []any{testdata.George.ID, testdata.GenderField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"40", "number": 40}'::jsonb`,
					Args:  []any{testdata.George.ID, testdata.AgeField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Male"}'::jsonb`,
					Args:  []any{testdata.Bob.ID, testdata.GenderField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields->$2 = '{"text":"Old"}'::jsonb`,
					Args:  []any{testdata.Bob.ID, testdata.AgeField.UUID},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND NOT fields?$2`,
					Args:  []any{testdata.Bob.ID, "unknown"},
					Count: 1,
				},
				{
					SQL:   `select count(*) from contacts_contact where id = $1 AND fields = '{}'`,
					Args:  []any{testdata.Alexandria.ID},
					Count: 1,
				},
			},
		},
	}

	handlers.RunTestCases(t, ctx, rt, tcs)
}
