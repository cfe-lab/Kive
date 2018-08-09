from django.test import TestCase
from django_mock_queries.mocks import mocked_relations

from container.models import Container, ContainerFamily


@mocked_relations(Container, ContainerFamily)
class ContainerMockTests(TestCase):

    def test_str(self):
        family = ContainerFamily(name='Spline Reticulator')
        container = Container(tag='v1.0.7', family=family)

        s = str(container)

        self.assertEqual("Spline Reticulator:v1.0.7", s)
