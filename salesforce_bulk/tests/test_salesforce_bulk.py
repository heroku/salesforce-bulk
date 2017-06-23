from __future__ import absolute_import
from __future__ import print_function

import os
import re
import time

try:
    # Python 2.6
    import unittest2 as unittest
except ImportError:
    import unittest

from itertools import islice

from six.moves import range
from six.moves import input

import unicodecsv

from salesforce_bulk import SalesforceBulk, BulkApiError
from salesforce_bulk import CsvDictsAdapter


def batches(iterator, n=10000):
    while True:
        batch = list(islice(iterator, n))
        if not batch:
            return

        yield batch

class SalesforceBulkTests(unittest.TestCase):


    def setUp(self):
        request_patcher = patch('simple_salesforce.api.requests')
        self.mockrequest = request_patcher.start()
        self.addCleanup(request_patcher.stop)
        self.bulk = Salesforce('12345', 'https://example.com')


class SalesforceBulkIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        username = os.environ.get('SALESFORCE_BULK_TEST_USERNAME')
        password = os.environ.get('SALESFORCE_BULK_TEST_PASSWORD')
        security_token = os.environ.get('SALESFORCE_BULK_TEST_SECURITY_TOKEN')
        sandbox = os.environ.get('SALESFORCE_BULK_TEST_SANDBOX')

        if not all(x for x in [username, password, security_token]):
            raise unittest.SkipTest('Missing Configuration for logged in tests')
        
        sessionId, endpoint = SalesforceBulk.login_to_salesforce(
            username, password, sandbox, security_token)

        cls.endpoint = endpoint
        cls.sessionId = sessionId

        print(endpoint)

    def setUp(self):
        self.jobs = []

    def tearDown(self):
        if hasattr(self, 'bulk'):
            job_id = self.bulk.create_query_job("Contact")
            self.jobs.append(job_id)
            batch_id = self.bulk.query(job_id, "SELECT Id FROM Contact WHERE FirstName LIKE 'BulkTestFirst%'")
            self.bulk.close_job(job_id)
            results = self.bulk.get_all_results_for_query_batch(batch_id)
            results = (record for result in results
                       for record in unicodecsv.DictReader(result, encoding='utf-8'))
            
            job_id = self.bulk.create_delete_job('Contact')
            self.jobs.append(job_id)
            
            for batch in batches(results):
                content = CsvDictsAdapter(iter(batch))
                self.bulk.post_batch(job_id, content)

            self.bulk.close_job(job_id)

            for job_id in self.jobs:
                print("Closing job: %s" % job_id)
                try:
                    self.bulk.close_job(job_id)
                except BulkApiError:
                    pass

    def test_raw_query(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_query_job("Contact")
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        batch_id = bulk.query(job_id, "Select Id,Name,Email from Contact Limit 1000")
        self.assertIsNotNone(re.match("\w+", batch_id))

        while not bulk.is_batch_done(batch_id):
            print("Job not done yet...")
            print(bulk.batch_status(batch_id))
            time.sleep(2)

        all_results = []
        results = bulk.get_all_results_for_query_batch(batch_id)
        for result in results:
            reader = unicodecsv.DictReader(result, encoding='utf-8')
            all_results.extend(reader)


        self.assertTrue(len(all_results) > 0)


    def test_csv_upload(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_insert_job("Contact")
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        batch_ids = []
        data = [
            {
                'FirstName': 'BulkTestFirst%s' % i,
                'LastName': 'BulkLastName',
                'Phone': '555-555-5555',
            } for i in range(50)
        ]
        for i in range(5):
            content = CsvDictsAdapter(iter(data))
            batch_id = bulk.post_batch(job_id, content)
            self.assertIsNotNone(re.match("\w+", batch_id))
            batch_ids.append(batch_id)

        bulk.close_job(job_id)

        for batch_id in batch_ids:
            bulk.wait_for_batch(job_id, batch_id, timeout=120)

        for batch_id in batch_ids:
            batch_result = bulk.get_batch_results(batch_id)
            reader = unicodecsv.DictReader(batch_result, encoding='utf-8')
            results = list(reader)

            self.assertTrue(len(results) > 0)
            self.assertTrue(isinstance(results,list))
            self.assertEqual(sorted(results[0].keys()), ['Created', 'Error', 'Id','Success'])
            self.assertEqual(len(results), 50)
