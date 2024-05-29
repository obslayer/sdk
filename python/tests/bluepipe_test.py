import unittest

from lib import bluepipe


class BluePipeTest(unittest.TestCase):
    client = bluepipe.BluePipe('http://127.0.0.1/api/v1////', 'etl', 'yZz6XTYSaiyx5q2u')

    def test_search_lineage(self):
        result = self.client.search_lineage('i.am.not.exist')
        self.assertListEqual(result, [])

        result = self.client.search_lineage('tpch.lineitem')
        self.assertTrue(len(result) > 0)

        first = result[0]
        self.assertIsNotNone(first.get('dst_table'))
        self.assertIsNotNone(first.get('job_guid'))
