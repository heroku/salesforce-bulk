from __future__ import absolute_import
from __future__ import print_function

import io
import json
import mock
import os
import pickle
import re
import time
import xml.etree.ElementTree as ET

try:
    # Python 2.6
    import unittest2 as unittest
except ImportError:
    import unittest

from itertools import islice

from six.moves import range

import unicodecsv

from salesforce_bulk import SalesforceBulk, BulkApiError, UploadResult
from salesforce_bulk import CsvDictsAdapter, bulk_states
from salesforce_bulk.salesforce_bulk import BulkJobAborted, BulkBatchFailed

nsclean = re.compile('{.*}')


def batches(iterator, n=10000):
    while True:
        batch = list(islice(iterator, n))
        if not batch:
            return

        yield batch


class SalesforceBulkTests(unittest.TestCase):

    def setUp(self):
        request_patcher = mock.patch('simple_salesforce.api.requests')
        self.mockrequest = request_patcher.start()
        self.addCleanup(request_patcher.stop)
        self.sessionId = '12345'
        self.host = 'https://example.com'
        self.bulk = SalesforceBulk(self.sessionId, self.host)

    def test_headers_default(self):
        self.assertEqual(
            self.bulk.headers(),
            {
                'X-SFDC-Session': self.sessionId,
                'Content-Type': 'application/xml; charset=UTF-8',
                'Accept-Encoding': 'gzip',
            }
        )

    def test_headers_json(self):
        self.assertEqual(
            self.bulk.headers(content_type='application/json'),
            {
                'X-SFDC-Session': self.sessionId,
                'Content-Type': 'application/json; charset=UTF-8',
                'Accept-Encoding': 'gzip',
            }
        )

    def test_create_job_doc(self):
        doc = self.bulk.create_job_doc(
            'Contact', 'insert'
        )
        tree = ET.fromstring(doc)

        operation = tree.findtext('{%s}operation' % self.bulk.jobNS)
        self.assertEqual(operation, 'insert')

        obj = tree.findtext('{%s}object' % self.bulk.jobNS)
        self.assertEqual(obj, 'Contact')

        contentType = tree.findtext('{%s}contentType' % self.bulk.jobNS)
        self.assertEqual(contentType, 'CSV')

        concurrencyMode = tree.findtext('{%s}concurrencyMode' % self.bulk.jobNS)
        self.assertIsNone(concurrencyMode)

        extIdField = tree.findtext('{%s}externalIdFieldName' % self.bulk.jobNS)
        self.assertIsNone(extIdField)

    def test_create_job_doc_concurrency(self):
        doc = self.bulk.create_job_doc(
            'Contact', 'insert', concurrency='Serial'
        )
        tree = ET.fromstring(doc)

        operation = tree.findtext('{%s}operation' % self.bulk.jobNS)
        self.assertEqual(operation, 'insert')

        obj = tree.findtext('{%s}object' % self.bulk.jobNS)
        self.assertEqual(obj, 'Contact')

        contentType = tree.findtext('{%s}contentType' % self.bulk.jobNS)
        self.assertEqual(contentType, 'CSV')

        concurrencyMode = tree.findtext('{%s}concurrencyMode' % self.bulk.jobNS)
        self.assertEqual(concurrencyMode, 'Serial')

        extIdField = tree.findtext('{%s}externalIdFieldName' % self.bulk.jobNS)
        self.assertIsNone(extIdField)

    def test_create_job_doc_external_id(self):
        doc = self.bulk.create_job_doc(
            'Contact', 'upsert', external_id_name='ext_id__c'
        )
        tree = ET.fromstring(doc)

        operation = tree.findtext('{%s}operation' % self.bulk.jobNS)
        self.assertEqual(operation, 'upsert')

        obj = tree.findtext('{%s}object' % self.bulk.jobNS)
        self.assertEqual(obj, 'Contact')

        contentType = tree.findtext('{%s}contentType' % self.bulk.jobNS)
        self.assertEqual(contentType, 'CSV')

        concurrencyMode = tree.findtext('{%s}concurrencyMode' % self.bulk.jobNS)
        self.assertIsNone(concurrencyMode)

        extIdField = tree.findtext('{%s}externalIdFieldName' % self.bulk.jobNS)
        self.assertEqual(extIdField, 'ext_id__c')

    def test_create_job_doc_json(self):
        doc = self.bulk.create_job_doc(
            'Contact', 'insert', contentType='JSON'
        )
        tree = ET.fromstring(doc)

        operation = tree.findtext('{%s}operation' % self.bulk.jobNS)
        self.assertEqual(operation, 'insert')

        obj = tree.findtext('{%s}object' % self.bulk.jobNS)
        self.assertEqual(obj, 'Contact')

        contentType = tree.findtext('{%s}contentType' % self.bulk.jobNS)
        self.assertEqual(contentType, 'JSON')

        concurrencyMode = tree.findtext('{%s}concurrencyMode' % self.bulk.jobNS)
        self.assertIsNone(concurrencyMode)

        extIdField = tree.findtext('{%s}externalIdFieldName' % self.bulk.jobNS)
        self.assertIsNone(extIdField)

    def test_create_close_job_doc(self):
        doc = self.bulk.create_close_job_doc()
        tree = ET.fromstring(doc)

        state = tree.findtext('{%s}state' % self.bulk.jobNS)
        self.assertEqual(state, 'Closed')

    def test_create_abort_job_doc(self):
        doc = self.bulk.create_abort_job_doc()
        tree = ET.fromstring(doc)

        state = tree.findtext('{%s}state' % self.bulk.jobNS)
        self.assertEqual(state, 'Aborted')

    def test_pickle_roundtrip_bulk_api_error_no_status(self):
        s = pickle.dumps(BulkApiError('message'))
        e = pickle.loads(s)
        assert e.__class__ is BulkApiError
        assert e.args[0] == 'message'
        assert e.status_code is None

    def test_pickle_roundtrip_bulk_api_error_no_status_code(self):
        s = pickle.dumps(BulkApiError('message', 400))
        e = pickle.loads(s)
        assert e.__class__ is BulkApiError
        assert e.args[0] == 'message'
        assert e.status_code == 400

    def test_pickle_roundtrip_bulk_job_aborted(self):
        orig = BulkJobAborted('sfid1234')
        s = pickle.dumps(orig)
        e = pickle.loads(s)
        assert e.__class__ is BulkJobAborted
        assert e.job_id == 'sfid1234'
        assert 'sfid1234' in e.args[0]
        assert e.args[0] == orig.args[0]

    def test_pickle_roundtrip_bulk_batch_failed(self):
        orig = BulkBatchFailed('sfid1234', 'sfid5678', 'some thing happened')
        s = pickle.dumps(orig)
        e = pickle.loads(s)
        assert e.__class__ is BulkBatchFailed
        assert e.job_id == 'sfid1234'
        assert e.batch_id == 'sfid5678'
        assert e.state_message == 'some thing happened'
        assert 'sfid1234' in e.args[0]
        assert 'sfid5678' in e.args[0]
        assert 'some thing happened' in e.args[0]
        assert orig.args[0] == e.args[0]


class SalesforceBulkIntegrationTestCSV(unittest.TestCase):

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

    def setUp(self):
        self.jobs = []

    def tearDown(self):
        if hasattr(self, 'bulk'):
            job_id = self.bulk.create_query_job("Contact")
            self.jobs.append(job_id)
            batch_id = self.bulk.query(
                job_id, "SELECT Id FROM Contact WHERE FirstName LIKE 'BulkTestFirst%'")
            self.bulk.wait_for_batch(job_id, batch_id)
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

    contentType = 'CSV'

    def generate_content(self, data):
        return CsvDictsAdapter(iter(data))

    def parse_results(self, results):
        reader = unicodecsv.DictReader(results, encoding='utf-8')
        return list(reader)

    def test_query(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_query_job("Contact", contentType=self.contentType)
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
            all_results.extend(self.parse_results(result))

        self.assertTrue(len(all_results) > 0)
        self.assertEqual(
            sorted(all_results[0].keys()),
            ['Email', 'Id', 'Name']
        )

    def test_query_pk_chunk(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_query_job("Contact", contentType=self.contentType, pk_chunking=True)
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        query = "Select Id,Name,Email from Contact"
        batch_id = bulk.query(job_id, query)
        self.assertIsNotNone(re.match("\w+", batch_id))

        try:
            i = 0
            while not bulk.is_batch_done(batch_id):
                print("Job not done yet...")
                print(bulk.batch_status(batch_id))
                time.sleep(2)
                i += 1
                if i == 20:
                    raise Exception
        except BulkBatchFailed as e:
            if e.state != bulk_states.NOT_PROCESSED:
                raise

        batches = bulk.get_batch_list(job_id)
        print (batches)
        batch_ids = [x['id'] for x in batches if x['state'] != bulk_states.NOT_PROCESSED]
        requests = [bulk.get_query_batch_request(x, job_id) for x in batch_ids]
        print (requests)
        for request in requests:
            self.assertTrue(request.startswith(query))

        all_results = []

        i = 0
        while not all(bulk.is_batch_done(j, job_id) for j in batch_ids):
            print("Job not done yet...")
            print(bulk.batch_status(batch_id, job_id))
            time.sleep(2)
            i += 1
            if i == 20:
                raise Exception

        for batch_id in batch_ids:
            results = bulk.get_all_results_for_query_batch(batch_id, job_id)
            for result in results:
                all_results.extend(self.parse_results(result))

            self.assertTrue(len(all_results) > 0)
            self.assertEqual(
                sorted(all_results[0].keys()),
                ['Email', 'Id', 'Name']
            )

    def test_upload(self):
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_insert_job("Contact", contentType=self.contentType)
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
        for i in range(2):
            content = self.generate_content(data)
            batch_id = bulk.post_batch(job_id, content)
            self.assertIsNotNone(re.match("\w+", batch_id))
            batch_ids.append(batch_id)

        bulk.close_job(job_id)

        for batch_id in batch_ids:
            bulk.wait_for_batch(job_id, batch_id, timeout=120)

        for batch_id in batch_ids:
            results = bulk.get_batch_results(batch_id)

            print(results)
            self.assertTrue(len(results) > 0)
            self.assertTrue(isinstance(results, list))
            self.assertTrue(isinstance(results[0], UploadResult))
            self.assertEqual(len(results), 50)

    def test_upload_with_mapping_file(self):
        if self.contentType != 'CSV':
            print('Mapping file can only be used with CSV content')
            return
        bulk = SalesforceBulk(self.sessionId, self.endpoint)
        self.bulk = bulk

        job_id = bulk.create_insert_job("Contact", contentType=self.contentType)
        self.jobs.append(job_id)
        self.assertIsNotNone(re.match("\w+", job_id))

        batch_ids = []
        data = [
            {
                'Not FirstName': 'BulkTestFirst%s' % i,
                'Arbitrary Field': 'BulkLastName',
                'Phone': '555-555-5555',
            } for i in range(50)
        ]

        mapping_data = [
            {
                "Salesforce Field": "FirstName",
                "Csv Header": "NotFirstName",
                "Value": "",
                "Hint": ""
            },
            {
                "Salesforce Field": "Phone",
                "Csv Header": "Phone",
                "Value": "",
                "Hint": ""
            },
            {
                "Salesforce Field": "LastName",
                "Csv Header": "Arbitrary Field",
                "Value": "",
                "Hint": ""
            }
        ]
        mapping_data = self.generate_content(mapping_data)

        bulk.post_mapping_file(job_id,mapping_data)
        for i in range(2):
            content = self.generate_content(data)
            batch_id = bulk.post_batch(job_id, content)
            self.assertIsNotNone(re.match("\w+", batch_id))
            batch_ids.append(batch_id)

        bulk.close_job(job_id)

        for batch_id in batch_ids:
            bulk.wait_for_batch(job_id, batch_id, timeout=120)

        for batch_id in batch_ids:
            results = bulk.get_batch_results(batch_id)

            print(results)
            self.assertTrue(len(results) > 0)
            self.assertTrue(isinstance(results, list))
            self.assertTrue(isinstance(results[0], UploadResult))
            self.assertEqual(len(results), 50)


class SalesforceBulkIntegrationTestJSON(SalesforceBulkIntegrationTestCSV):
    contentType = 'JSON'

    def generate_content(self, data):
        return json.dumps(data)

    def parse_results(self, results):
        result = json.load(io.TextIOWrapper(results, 'utf-8'))
        for row in result:
            row.pop('attributes', None)
        return result


class SalesforceBulkIntegrationTestXML(SalesforceBulkIntegrationTestCSV):
    contentType = 'XML'

    def generate_content(self, data):
        root = ET.Element('sObjects', xmlns=self.bulk.jobNS)
        for row in data:
            obj = ET.SubElement(root, 'sObject')
            for name, value in row.items():
                ET.SubElement(obj, name).text = value
        return (b'<?xml version="1.0" encoding="UTF-8"?>\n' +
                ET.tostring(root, 'utf-8'))

    def parse_results(self, results):
        result = ET.parse(results)
        type_tag = '{%s}type' % self.bulk.jobNS
        result = [
            {
                nsclean.sub('', child.tag): child.text
                for child in row
                if child.tag != type_tag
            }
            for row in result.getroot()
        ]

        return result
