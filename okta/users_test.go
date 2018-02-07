package okta

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccOktaUsers_create(t *testing.T) {
	resourceName := "okta_users.test"
	ri := acctest.RandInt()

	config := testOktaUsers(ri)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testOktaUsersDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testOktaUsersExists(resourceName),
					resource.TestCheckResourceAttr(resourceName, "firstname", "terraform_acc_test"),
					resource.TestCheckResourceAttr(resourceName, "lastname", strconv.Itoa(ri)),
					resource.TestCheckResourceAttr(resourceName, "email", "mmeyer+acceptancetest@articulate.com"),
				),
			},
		},
	})
}

func testOktaUsersExists(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		// Ensure we have enough information in state to look up in API
		rs, ok := s.RootModule().Resources[name]
		if !ok {
			return fmt.Errorf("Not found: %s", name)
		}

		email, hasEmail := rs.Primary.Attributes["email"]
		if !hasEmail {
			return fmt.Errorf("Error: no email found in state for resource: %s", name)
		}
		_, hasFirstName := rs.Primary.Attributes["firstname"]
		if !hasFirstName {
			return fmt.Errorf("Error: no firstname found in state for user: %s", email)
		}
		_, hasLastName := rs.Primary.Attributes["lastname"]
		if !hasLastName {
			return fmt.Errorf("Error: no lastname found in state for user: %s", email)
		}

		client := testAccProvider.Meta().(*Config).oktaClient

		_, _, err := client.Users.GetByID(email)
		if err != nil {
			if client.OktaErrorCode == "E0000007" {
				return fmt.Errorf("Error: User %s does not exist", email)
			}
			return fmt.Errorf("Error: GetByID: %v", err)
		}
		return nil
	}
	return nil
}

func testOktaUsersDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Config).oktaClient

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "okta_users" {
			continue
		}

		email, hasEmail := rs.Primary.Attributes["email"]
		if !hasEmail {
			return fmt.Errorf("Error: no email found in state for user")
		}

		_, _, err := client.Users.GetByID(email)
		if err != nil {
			if client.OktaErrorCode == "E0000007" {
				return nil
			}
			return fmt.Errorf("Error: GetByID: %v", err)
		}
		return fmt.Errorf("User still exists: %s", email)
	}
	return nil
}

func testOktaUsers(rInt int) string {
	return fmt.Sprintf(`
resource "okta_users" "test" {
  firstname = "terraform_acc_test"
  lastname  = "%d"
  email     = "mmeyer+acceptancetest@articulate.com"
}
`, rInt)
}
