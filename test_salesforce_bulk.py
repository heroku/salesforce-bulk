import re
import unittest

import salesforce_oauth_request

from salesforce_bulk import SalesforceBulk

class SalesforceBulkTest(unittest.TestCase):
    def __init__(self, testName, endpoint, sessionId):
        super(SalesforceBulkTest, self).__init__(testName)
        self.endpoint = endpoint
        self.sessionId = sessionId

    def setUp(self):
        self.jobs = []

    def tearDown(self):
        if hasattr(self, 'bulk'):
            for job_id in self.jobs:
                print "Closing job: %s" % job_id
                self.bulk.close_job(job_id)

    def test_raw_query(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_query_job("Contact")
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        batch_id = bulk.query(job_id, "Select Id,Name,Email from Contact Limit 1000")
        self.assertIsNotNone(re.match("\w+", batch_id))

        while not bulk.is_batch_done(job_id, batch_id):
            print "Job not done yet..."
            print bulk.batch_status(job_id, batch_id)
            time.sleep(2)

        self.results = ""
        def save_results(tfile, **kwargs):
            print "in save results"
            self.results = tfile.read()

        flag = bulk.get_batch_results(job_id, batch_id, callback = save_results)
        self.assertTrue(flag)
        self.assertTrue(len(self.results) > 0)
        self.assertIn('"', self.results)


    def test_csv_query(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_query_job("Account")
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        batch_id = bulk.query(job_id, "Select Id,Name,Description from Account Limit 10000")
        self.assertIsNotNone(re.match("\w+", batch_id))
        bulk.wait_for_batch(job_id, batch_id, timeout=120)

        self.results = None
        def save_results1(rows, **kwargs):
            self.results = rows

        flag = bulk.get_batch_results(job_id, batch_id, callback = save_results1, parse_csv=True)
        self.assertTrue(flag)
        results = self.results
        self.assertTrue(len(results) > 0)
        self.assertTrue(isinstance(results,list))
        self.assertEqual(results[0], ['Id','Name','Description'])
        self.assertTrue(len(results) > 3)

        self.results = None
        self.callback_count = 0
        def save_results2(rows, **kwargs):
            self.results = rows
            print rows
            self.callback_count += 1

        batch = len(results) / 3
        self.callback_count = 0
        flag = bulk.get_batch_results(job_id, batch_id, callback = save_results2, parse_csv=True, batch_size=batch)
        self.assertTrue(self.callback_count >= 3)


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


if __name__ == '__main__':
    username = raw_input("Salesforce username: ")
    password = raw_input("Salesforce password: ")

    login = salesforce_oauth_request.login(username=username, password=password, cache_session=True)
    endpoint = login['endpoint']
    sessionId = login['access_token']

    suite = unittest.TestSuite()
    suite.addTest(SalesforceBulkTest("test_csv_upload", endpoint, sessionId))
    unittest.TextTestRunner().run(suite)





