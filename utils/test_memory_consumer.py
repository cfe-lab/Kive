from unittest import TestCase

from memory_consumer import Consumer


class ConsumerTest(TestCase):
    def test_simple(self):
        consumer = Consumer(size=6, grow=3, hold=2)
        expected_sizes = [2, 4, 6, 6, 6]

        sizes = [len(consumer.data) for _ in consumer]

        self.assertEqual(expected_sizes, sizes)

    def test_uneven_multiple(self):
        consumer = Consumer(size=5, grow=3, hold=2)
        expected_sizes = [1, 3, 5, 5, 5]

        sizes = [len(consumer.data) for _ in consumer]

        self.assertEqual(expected_sizes, sizes)
