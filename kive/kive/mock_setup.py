import django_mock_queries.utils


# This can be removed after this pull request is merged and published.
# https://github.com/stphivos/django-mock-queries/pull/54
original_convert_to_pks = django_mock_queries.utils.convert_to_pks


def convert_to_pks(query):
    try:
        return original_convert_to_pks(query)
    except AttributeError:
        return query


django_mock_queries.utils.convert_to_pks = convert_to_pks
