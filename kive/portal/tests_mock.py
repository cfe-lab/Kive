from django.test.testcases import TestCase

from portal.models import parse_file_size


class ParseFileSizeTests(TestCase):
    def test_valid(self):
        for text, expected_size in [('42', 42),
                                    ('4096', 4096),
                                    ('4K', 4096),
                                    ('4k', 4096),
                                    ('4KB', 4096),
                                    (' 4 KB ', 4096),
                                    ('1.5k', 1024+512),
                                    ('1MB', 1 << 20),
                                    ('1GB', 1 << 30),
                                    ('1TB', 1 << 40)]:
            size = parse_file_size(text)

            self.assertEqual(expected_size, size)

    def test_invalid(self):
        for text in ['42X', '4 2', 'k', 'kb', 'b', '']:
            expected_message = 'Invalid file size: {!r}'.format(text)
            with self.assertRaisesRegex(ValueError, expected_message):
                parse_file_size(text)
