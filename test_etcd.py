#!/usr/bin/env python
"""
Test suite for etcd-py
"""

import sys
import uuid
import unittest

from etcd import *


class TestEtcd(unittest.TestCase):
    def setUp(self):
        self.e = etcd.Etcd("10.0.1.110")

    def test_crud(self):
        test_key = uuid.uuid4().hex
        first_value = uuid.uuid4().hex
        self.assertEqual(self.e.set(test_key, first_value).prevValue, None)
        self.assertEqual(self.e.get(test_key).value, first_value)
        second_value = uuid.uuid4().hex
        self.assertEqual(self.e.set(test_key, second_value).prevValue,
                first_value)
        self.assertEqual(self.e.delete(test_key).prevValue, second_value)

    def test_watch(self):
        test_key = uuid.uuid4().hex
        test_value = uuid.uuid4().hex
        self.e.set(test_key, test_value, 1)
        self.assertEquals(self.e.watch(test_key).action, "DELETE") 

if __name__ == '__main__':
    unittest.main()
