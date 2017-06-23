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

from six.moves import range
from six.moves import input

import unicodecsv

from salesforce_bulk import SalesforceBulk

class SalesforceBulkTests(unittest.TestCase):


    def setUp(self):
        request_patcher = patch('simple_salesforce.api.requests')
        self.mockrequest = request_patcher.start()
        self.addCleanup(request_patcher.stop)


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
            for job_id in self.jobs:
                print("Closing job: %s" % job_id)
                self.bulk.close_job(job_id)

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
        results = bulk.get_all_results_for_batch(batch_id)
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
        content = open("example.csv").read()
        for i in range(5):
            batch_id = bulk.query(job_id, content)
            self.assertIsNotNone(re.match("\w+", batch_id))
            batch_ids.append(batch_id)

        for batch_id in batch_ids:
            bulk.wait_for_batch(job_id, batch_id, timeout=120)

        self.results = None
        def save_results1(rows, failed, remaining):
            self.results = rows

        for batch_id in batch_ids:
            flag = bulk.get_upload_results(job_id, batch_id, callback = save_results1)
            self.assertTrue(flag)
            results = self.results
            self.assertTrue(len(results) > 0)
            self.assertTrue(isinstance(results,list))
            self.assertEqual(results[0], UploadResult('Id','Success','Created','Error'))
            self.assertEqual(len(results), 3)

        self.results = None
        self.callback_count = 0
        def save_results2(rows, failed, remaining):
            self.results = rows
            self.callback_count += 1

        batch = len(results) / 3
        self.callback_count = 0
        flag = bulk.get_upload_results(job_id, batch_id, callback = save_results2, batch_size=batch)
        self.assertTrue(self.callback_count >= 3)
