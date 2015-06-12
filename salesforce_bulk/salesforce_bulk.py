from __future__ import absolute_import

# Interface to the Salesforce BULK API
import os
from collections import namedtuple
from httplib2 import Http
import requests
import urllib2
import urlparse
import requests
import xml.etree.ElementTree as ET
from tempfile import TemporaryFile, NamedTemporaryFile
import StringIO
import re
import time
import csv

from . import bulk_states

UploadResult = namedtuple('UploadResult', 'id success created error')


class BulkApiError(Exception):

    def __init__(self, message, status_code=None):
        super(BulkApiError, self).__init__(message)
        self.status_code = status_code


class BulkJobAborted(BulkApiError):

    def __init__(self, job_id):
        self.job_id = job_id

        message = 'Job {0} aborted'.format(job_id)
        super(BulkJobAborted, self).__init__(message)


class BulkBatchFailed(BulkApiError):

    def __init__(self, job_id, batch_id, state_message):
        self.job_id = job_id
        self.batch_id = batch_id
        self.state_message = state_message

        message = 'Batch {0} of job {1} failed: {2}'.format(batch_id, job_id,
                                                            state_message)
        super(BulkBatchFailed, self).__init__(message)


class SalesforceBulk(object):

    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 exception_class=BulkApiError, API_version="29.0"):
        if not sessionId and not username:
            raise RuntimeError(
                "Must supply either sessionId/instance_url or username/password")
        if not sessionId:
            sessionId, endpoint = SalesforceBulk.login_to_salesforce(
                username, password)
            host = urlparse.urlparse(endpoint)
            host = host.hostname.replace("-api", "")

        if host[0:4] == 'http':
            self.endpoint = host
        else:
            self.endpoint = "https://" + host
        self.endpoint += "/services/async/%s" % API_version
        self.sessionId = sessionId
        self.jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
        self.jobs = {}  # dict of job_id => job_id
        self.batches = {}  # dict of batch_id => job_id
        self.batch_statuses = {}
        self.exception_class = exception_class

    @staticmethod
    def login_to_salesforce(username, password):
        env_vars = (
            'SALESFORCE_CLIENT_ID',
            'SALESFORCE_CLIENT_SECRET',
            'SALESFORCE_REDIRECT_URI',
        )
        missing_env_vars = [e for e in env_vars if e not in os.environ]
        if missing_env_vars:
            raise RuntimeError(
                "You must set {0} to use username/pass login".format(
                    ', '.join(missing_env_vars)))

        try:
            import salesforce_oauth_request
        except ImportError:
            raise ImportError(
                "You must install salesforce-oauth-request to use username/password")

        packet = salesforce_oauth_request.login(
            username=username, password=password)
        return packet['access_token'], packet['instance_url']

    def headers(self, values={}):
        default = {"X-SFDC-Session": self.sessionId,
                   "Content-Type": "application/xml; charset=UTF-8"}
        for k, val in values.iteritems():
            default[k] = val
        return default

    # Register a new Bulk API job - returns the job id
    def create_query_job(self, object_name, **kwargs):
        return self.create_job(object_name, "query", **kwargs)

    def create_insert_job(self, object_name, **kwargs):
        return self.create_job(object_name, "insert", **kwargs)

    def create_upsert_job(self, object_name, external_id_name, **kwargs):
        return self.create_job(object_name, "upsert", external_id_name=external_id_name, **kwargs)

    def create_update_job(self, object_name, **kwargs):
        return self.create_job(object_name, "update", **kwargs)

    def create_delete_job(self, object_name, **kwargs):
        return self.create_job(object_name, "delete", **kwargs)

    def create_job(self, object_name=None, operation=None, contentType='CSV',
                   concurrency=None, external_id_name=None):
        assert(object_name is not None)
        assert(operation is not None)

        doc = self.create_job_doc(object_name=object_name,
                                  operation=operation,
                                  contentType=contentType,
                                  concurrency=concurrency,
                                  external_id_name=external_id_name)

        http = Http()
        resp, content = http.request(self.endpoint + "/job",
                                     "POST",
                                     headers=self.headers(),
                                     body=doc)

        self.check_status(resp, content)

        tree = ET.fromstring(content)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id

        return job_id

    def check_status(self, resp, content):
        if resp.status >= 400:
            msg = "Bulk API HTTP Error result: {0}".format(content)
            self.raise_error(msg, resp.status)

    def close_job(self, job_id):
        doc = self.create_close_job_doc()
        http = Http()
        url = self.endpoint + "/job/%s" % job_id
        resp, content = http.request(url, "POST", headers=self.headers(),
                                     body=doc)
        self.check_status(resp, content)

    def create_job_doc(self, object_name=None, operation=None,
                       contentType='CSV', concurrency=None, external_id_name=None):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        op = ET.SubElement(root, "operation")
        op.text = operation
        obj = ET.SubElement(root, "object")
        obj.text = object_name
        if external_id_name:
            ext = ET.SubElement(root, 'externalIdFieldName')
            ext.text = external_id_name

        if concurrency:
            con = ET.SubElement(root, "concurrencyMode")
            con.text = concurrency
        ct = ET.SubElement(root, "contentType")
        ct.text = contentType

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    def create_close_job_doc(self, object_name=None, operation=None,
                             contentType='CSV'):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Closed"

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    # Add a BulkQuery to the job - returns the batch id
    def query(self, job_id, soql):
        if job_id is None:
            job_id = self.create_job(
                re.search(re.compile("from (\w+)", re.I), soql).group(1),
                "query")
        http = Http()
        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers({"Content-Type": "text/csv"})
        resp, content = http.request(uri, method="POST", body=soql,
                                     headers=headers)

        self.check_status(resp, content)

        tree = ET.fromstring(content)
        batch_id = tree.findtext("{%s}id" % self.jobNS)

        self.batches[batch_id] = job_id

        return batch_id

    def split_csv(self, csv, batch_size):
        csv_io = StringIO.StringIO(csv)
        batches = []

        for i, line in enumerate(csv_io):
            if not i:
                headers = line
                batch = headers
                continue
            if not i % batch_size:
                batches.append(batch)
                batch = headers

            batch += line

        batches.append(batch)

        return batches

    # Add a BulkUpload to the job - returns the batch id
    def bulk_csv_upload(self, job_id, csv, batch_size=2500):
        # Split a large CSV into manageable batches
        batches = self.split_csv(csv, batch_size)
        batch_ids = []

        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers({"Content-Type": "text/csv"})
        for batch in batches:
            resp = requests.post(uri, data=batch, headers=headers)
            content = resp.content

            if resp.status_code >= 400:
                self.raise_error(content, resp.status)

            tree = ET.fromstring(content)
            batch_id = tree.findtext("{%s}id" % self.jobNS)

            self.batches[batch_id] = job_id
            batch_ids.append(batch_id)

        return batch_ids

    def raise_error(self, message, status_code=None):
        if status_code:
            message = "[{0}] {1}".format(status_code, message)

        if self.exception_class == BulkApiError:
            raise self.exception_class(message, status_code=status_code)
        else:
            raise self.exception_class(message)

    def post_bulk_batch(self, job_id, csv_generator):
        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers({"Content-Type": "text/csv"})
        resp = requests.post(uri, data=csv_generator, headers=headers)
        content = resp.content

        if resp.status_code >= 400:
            self.raise_error(content, resp.status_code)

        tree = ET.fromstring(content)
        batch_id = tree.findtext("{%s}id" % self.jobNS)
        return batch_id

    # Add a BulkDelete to the job - returns the batch id
    def bulk_delete(self, job_id, object_type, where, batch_size=2500):
        query_job_id = self.create_query_job(object_type)
        soql = "Select Id from %s where %s Limit 10000" % (object_type, where)
        query_batch_id = self.query(query_job_id, soql)
        self.wait_for_batch(query_job_id, query_batch_id, timeout=120)

        results = []

        def save_results(tf, **kwargs):
            results.append(tf.read())

        flag = self.get_batch_results(
            query_job_id, query_batch_id, callback=save_results)

        if job_id is None:
            job_id = self.create_job(object_type, "delete")
        http = Http()
        # Split a large CSV into manageable batches
        batches = self.split_csv(csv, batch_size)
        batch_ids = []

        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers({"Content-Type": "text/csv"})
        for batch in results:
            resp = requests.post(uri, data=batch, headers=headers)
            content = resp.content

            if resp.status_code >= 400:
                self.raise_error(content, resp.status)

            tree = ET.fromstring(content)
            batch_id = tree.findtext("{%s}id" % self.jobNS)

            self.batches[batch_id] = job_id
            batch_ids.append(batch_id)

        return batch_ids

    def lookup_job_id(self, batch_id):
        try:
            return self.batches[batch_id]
        except KeyError:
            raise Exception(
                "Batch id '%s' is uknown, can't retrieve job_id" % batch_id)

    def job_status(self, job_id=None):
        job_id = job_id or self.lookup_job_id(batch_id)
        uri = urlparse.urljoin(self.endpoint +"/",
            'job/{0}'.format(job_id))
        response = requests.get(uri, headers=self.headers())
        if response.status_code != 200:
            self.raise_error(response.content, response.status_code)

        tree = ET.fromstring(response.content)
        result = {}
        for child in tree:
            result[re.sub("{.*?}", "", child.tag)] = child.text
        return result

    def job_state(self, job_id):
        status = self.job_status(job_id)
        if 'state' in status:
            return status['state']
        else:
            return None

    def batch_status(self, job_id=None, batch_id=None, reload=False):
        if not reload and batch_id in self.batch_statuses:
            return self.batch_statuses[batch_id]

        job_id = job_id or self.lookup_job_id(batch_id)

        http = Http()
        uri = self.endpoint + \
            "/job/%s/batch/%s" % (job_id, batch_id)
        resp, content = http.request(uri, headers=self.headers())
        self.check_status(resp, content)

        tree = ET.fromstring(content)
        result = {}
        for child in tree:
            result[re.sub("{.*?}", "", child.tag)] = child.text

        self.batch_statuses[batch_id] = result
        return result

    def batch_state(self, job_id, batch_id, reload=False):
        status = self.batch_status(job_id, batch_id, reload=reload)
        if 'state' in status:
            return status['state']
        else:
            return None

    def is_batch_done(self, job_id, batch_id):
        batch_state = self.batch_state(job_id, batch_id, reload=True)
        if batch_state in bulk_states.ERROR_STATES:
            status = self.batch_status(job_id, batch_id)
            raise BulkBatchFailed(job_id, batch_id, status['stateMessage'])
        return batch_state == bulk_states.COMPLETED

    # Wait for the given batch to complete, waiting at most timeout seconds
    # (defaults to 10 minutes).
    def wait_for_batch(self, job_id, batch_id, timeout=60 * 10,
                       sleep_interval=10):
        waited = 0
        while not self.is_batch_done(job_id, batch_id) and waited < timeout:
            time.sleep(sleep_interval)
            waited += sleep_interval

    def get_batch_result_ids(self, batch_id, job_id=None):
        job_id = job_id or self.lookup_job_id(batch_id)
        if not self.is_batch_done(job_id, batch_id):
            return False

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result".format(
                job_id, batch_id),
        )
        resp = requests.get(uri, headers=self.headers())
        if resp.status_code != 200:
            return False

        tree = ET.fromstring(resp.content)
        return [str(r.text) for r in
                tree.iterfind("{{{0}}}result".format(self.jobNS))]

    def get_batch_results(self, batch_id, result_id, job_id=None,
                          parse_csv=False, logger=None):
        job_id = job_id or self.lookup_job_id(batch_id)
        logger = logger or (lambda message: None)

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result/{2}".format(
                job_id, batch_id, result_id),
        )
        logger('Downloading bulk result file id=#{0}'.format(result_id))
        resp = requests.get(uri, headers=self.headers(), stream=True)

        if not parse_csv:
            iterator = resp.iter_lines()
        else:
            iterator = csv.reader(resp.iter_lines(), delimiter=',',
                                  quotechar='"')

        BATCH_SIZE = 5000
        for i, line in enumerate(iterator):
            if i % BATCH_SIZE == 0:
                logger('Loading bulk result #{0}'.format(i))
            yield line

    def get_batch_result_iter(self, job_id, batch_id, parse_csv=False,
                              logger=None):
        """
        Return a line interator over the contents of a batch result document. If
        csv=True then parses the first line as the csv header and the iterator
        returns dicts.
        """
        status = self.batch_status(job_id, batch_id)
        if status['state'] != 'Completed':
            return None
        elif logger:
            if 'numberRecordsProcessed' in status:
                logger("Bulk batch %d processed %s records" %
                       (batch_id, status['numberRecordsProcessed']))
            if 'numberRecordsFailed' in status:
                failed = int(status['numberRecordsFailed'])
                if failed > 0:
                    logger("Bulk batch %d had %d failed records" %
                           (batch_id, failed))

        uri = self.endpoint + \
            "/job/%s/batch/%s/result" % (job_id, batch_id)
        r = requests.get(uri, headers=self.headers(), stream=True)

        result_id = r.text.split("<result>")[1].split("</result>")[0]

        uri = self.endpoint + \
            "/services/async/29.0/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
        r = requests.get(uri, headers=self.headers(), stream=True)
        
        if parse_csv:
            return csv.DictReader(r.iter_lines(chunk_size=2048), delimiter=",",
                                  quotechar='"')
        else:
            return r.iter_lines(chunk_size=2048)

    def get_upload_results(self, job_id, batch_id,
                           callback=(lambda *args, **kwargs: None),
                           batch_size=0, logger=None):
        job_id = job_id or self.lookup_job_id(batch_id)

        if not self.is_batch_done(job_id, batch_id):
            return False
        http = Http()
        uri = self.endpoint + \
            "/job/%s/batch/%s/result" % (job_id, batch_id)
        resp, content = http.request(uri, method="GET", headers=self.headers())

        tf = TemporaryFile()
        tf.write(content)

        total_remaining = self.count_file_lines(tf)
        if logger:
            logger("Total records: %d" % total_remaining)
        tf.seek(0)

        records = []
        line_number = 0
        col_names = []
        reader = csv.reader(tf, delimiter=",", quotechar='"')
        for row in reader:
            line_number += 1
            records.append(UploadResult(*row))
            if len(records) == 1:
                col_names = records[0]
            if batch_size > 0 and len(records) >= (batch_size + 1):
                callback(records, total_remaining, line_number)
                total_remaining -= (len(records) - 1)
                records = [col_names]
        callback(records, total_remaining, line_number)

        tf.close()

        return True

    def parse_csv(self, tf, callback, batch_size, total_remaining):
        records = []
        line_number = 0
        col_names = []
        reader = csv.reader(tf, delimiter=",", quotechar='"')
        for row in reader:
            line_number += 1
            records.append(row)
            if len(records) == 1:
                col_names = records[0]
            if batch_size > 0 and len(records) >= (batch_size + 1):
                callback(records, total_remaining, line_number)
                total_remaining -= (len(records) - 1)
                records = [col_names]
        return records, total_remaining

    def count_file_lines(self, tf):
        tf.seek(0)
        buffer = bytearray(2048)
        lines = 0

        quotes = 0
        while tf.readinto(buffer) > 0:
            quoteChar = ord('"')
            newline = ord('\n')
            for c in buffer:
                if c == quoteChar:
                    quotes += 1
                elif c == newline:
                    if (quotes % 2) == 0:
                        lines += 1
                        quotes = 0

        return lines
