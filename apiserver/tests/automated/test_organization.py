from apiserver.tests.automated import TestService


class TestOrganization(TestService):
    def test_get_user_companies(self):
        company = self.api.organization.get_user_companies().companies[0]
        self.assertEqual(len(company.owners), company.allocated)
        users = company.owners
        self.assertTrue(users)
        self.assertTrue(u1.name < u2.name for u1, u2 in zip(users, users[1:]))
        for user in company.owners:
            self.assertTrue(user.id)
            self.assertTrue(user.name)
            self.assertIn("avatar", user)
