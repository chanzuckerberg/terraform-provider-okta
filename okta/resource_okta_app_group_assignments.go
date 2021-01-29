package okta

import (
	"context"
	"net/url"

	"github.com/chanzuckerberg/go-misc/sets"
	"github.com/hashicorp/terraform-plugin-sdk/helper/hashcode"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/okta/okta-sdk-golang/v2/okta"
	"github.com/okta/okta-sdk-golang/v2/okta/query"
	"github.com/peterhellberg/link"
	"github.com/pkg/errors"
)

func flattenGroupIDs(groupIDs []string) *schema.Set {
	flattened := []interface{}{}
	for _, groupID := range groupIDs {
		flattened = append(flattened, groupID)
	}
	return schema.NewSet(groupIDsHash, flattened)
}

func groupIDsHash(v interface{}) int {
	return hashcode.String(v.(string))
}

func listAppGroupAssignments(
	fetch func(context.Context, string, *query.Params) ([]*okta.ApplicationGroupAssignment, *okta.Response, error),
	ctx context.Context,
	appID string,
) ([]string, error) {

	var assignments []string
	qp := query.Params{
		Limit: 200, // Biggest page possible
	}

	for {
		assignmentsPage, resp, err := fetch(ctx, appID, &qp)
		if err != nil {
			return nil, errors.Wrapf(err, "error listing group assignments for %s", appID)
		}

		for _, assignment := range assignmentsPage {
			if assignment == nil {
				continue
			}

			// we only care about the id for now
			assignments = append(assignments, assignment.Id)
		}

		// Parse the link header and iterate
		links := link.ParseResponse(resp.Response)
		if links["next"] == nil {
			return assignments, nil // we're done, no next page
		}
		nextLink := links["next"].String()
		nextLinkURL, err := url.Parse(nextLink)
		if err != nil {
			return nil, errors.Wrap(err, "error parsing Link Header next url")
		}

		nextLinkMapping := nextLinkURL.Query()
		qp.After = nextLinkMapping.Get("after")
	}
}

func resourceAppGroupAssignments() *schema.Resource {
	return &schema.Resource{
		// No point in having an exist function, since only the group has to exist
		CreateContext: resourceAppGroupAssignmentsCreate,
		ReadContext:   resourceAppGroupAssignmentsRead,
		DeleteContext: resourceAppGroupAssignmentsDelete,
		UpdateContext: resourceAppGroupAssignmentsUpdate,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"app_id": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				Description: "App to associate groups with",
				ForceNew:    true,
			},
			"group_ids": &schema.Schema{
				Type: schema.TypeSet,
				// TODO(el): Do we need the priority + profile?
				Elem:        &schema.Schema{Type: schema.TypeString},
				Required:    true,
				Description: "Groups assigned to the application",
				Set:         groupIDsHash,
			},
		},
	}
}

func addGroupAssignments(
	add func(context.Context, string, string, okta.ApplicationGroupAssignment) (*okta.ApplicationGroupAssignment, *okta.Response, error),
	ctx context.Context,
	appID string,
	groupIDs []string,
) error {
	for _, groupID := range groupIDs {
		_, _, err := add(ctx, appID, groupID, okta.ApplicationGroupAssignment{})
		if err != nil {
			return errors.Wrapf(err, "could not assign group %s, to application %s", groupID, appID)
		}
	}
	return nil
}
func deleteGroupAssignments(
	delete func(context.Context, string, string) (*okta.Response, error),
	ctx context.Context,
	appID string,
	groupIDs []string,
) error {
	for _, groupID := range groupIDs {
		_, err := delete(ctx, appID, groupID)
		if err != nil {
			return errors.Wrapf(err, "could not delete assignment for group %s, to application %s", groupID, appID)
		}
	}
	return nil
}

func resourceAppGroupAssignmentsCreate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	_, ok := d.GetOk("group_ids")
	if !ok {
		return diag.Errorf("group_ids is required")
	}
	appID := d.Get("app_id").(string)
	groupIDs := sets.NewStringSet()
	for _, groupID := range d.Get("group_ids").(*schema.Set).List() {
		groupIDs.Add(groupID.(string))
	}

	err := addGroupAssignments(
		getOktaClientFromMetadata(m).Application.CreateApplicationGroupAssignment,
		ctx,
		appID,
		groupIDs.List(),
	)
	if err != nil {
		return diag.FromErr(err)
	}
	return resourceAppGroupAssignmentsRead(ctx, d, m)
}

func resourceAppGroupAssignmentsUpdate(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	if !d.HasChange("group_ids") {
		return nil // no change we're good
	}

	appID := d.Get("app_id").(string)
	client := getOktaClientFromMetadata(m)

	old, new := d.GetChange("group_ids")
	oldSet := &sets.StringSet{}
	newSet := &sets.StringSet{}

	for _, o := range old.(*schema.Set).List() {
		oldSet.Add(o.(string))
	}
	for _, n := range new.(*schema.Set).List() {
		newSet.Add(n.(string))
	}

	toAdd := newSet.Subtract(oldSet)
	toRemove := oldSet.Subtract(newSet)

	err := addGroupAssignments(
		client.Application.CreateApplicationGroupAssignment,
		ctx,
		appID,
		toAdd.List(),
	)
	if err != nil {
		return diag.FromErr(err)
	}

	err = deleteGroupAssignments(
		client.Application.DeleteApplicationGroupAssignment,
		ctx,
		appID,
		toRemove.List(),
	)
	if err != nil {
		return diag.FromErr(err)
	}

	return resourceAppGroupAssignmentsRead(ctx, d, m)
}

func resourceAppGroupAssignmentsRead(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	appID := d.Get("app_id").(string)

	assignments, err := listAppGroupAssignments(
		getOktaClientFromMetadata(m).Application.ListApplicationGroupAssignments,
		ctx,
		appID,
	)
	if err != nil {
		return diag.FromErr(err)
	}

	d.SetId(appID)
	d.Set("group_ids", flattenGroupIDs(assignments))
	return nil
}

func resourceAppGroupAssignmentsDelete(ctx context.Context, d *schema.ResourceData, m interface{}) diag.Diagnostics {
	_, ok := d.GetOk("group_ids")
	if !ok {
		return nil // no group ids to delete
	}

	groupIDs := []string{}
	for _, groupID := range d.Get("group_ids").(*schema.Set).List() {
		groupIDs = append(groupIDs, groupID.(string))
	}

	err := deleteGroupAssignments(
		getOktaClientFromMetadata(m).Application.DeleteApplicationGroupAssignment,
		ctx,
		d.Get("app_id").(string),
		groupIDs,
	)
	if err != nil {
		return diag.FromErr(err)
	}
	return nil
}
