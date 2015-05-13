from django.test import TestCase
from django.core.urlresolvers import resolve

from rest_framework.test import APIRequestFactory, force_authenticate

from metadata.models import kive_user


class ApiTestCase(TestCase):
    """
    Base test case used for all API testing.

    Such test cases should provide tests of:
     - authentication
     - list
     - detail
     - creation (if applicable)
     - redaction
     - removal
     - any other detail or list routes

    In addition, inheriting classes must provide appropriate values for
    self.model_list_path and self.model_list_view in their setUps.
    """
    def setUp(self):
        self.factory = APIRequestFactory()
        self.kive_user = kive_user()

        self.model_list_path = None
        self.model_list_view = None

    def test_auth(self):
        """
        Test that the API URL is correctly defined and requires a logged-in user.
        """
        # First try to access while not logged in.
        request = self.factory.get(self.model_list_path)
        response = self.model_list_view(request)
        self.assertEquals(response.data["detail"], "Authentication credentials were not provided.")

        # Now log in and check that "detail" is not passed in the response.
        force_authenticate(request, user=self.kive_user)
        response = self.model_list_view(request)
        self.assertNotIn('detail', response.data)

    def test_list(self, expected_entries=0, user=None):
        """
        Test retrieval from the list API.
        """
        if not user:
            user = self.kive_user

        request = self.factory.get(self.model_list_path)
        force_authenticate(request, user=user)
        response = self.model_list_view(request)

        self.assertEquals(len(response.data), expected_entries)