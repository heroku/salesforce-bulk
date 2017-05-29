from __future__ import absolute_import

# Interface to the Salesforce BULK API
import os
from collections import namedtuple
import requests
import urlparse
import xml.etree.ElementTree as ET
import StringIO
import re
import time

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


job_to_http_content_type = {
    'CSV', 'text/csv',
    'XML', 'application/xml',
    'JSON', 'application/json',
}


class SalesforceBulk(object):

    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 exception_class=BulkApiError, API_version="39.0", sandbox=False):
        if not sessionId and not username:
            raise RuntimeError(
                "Must supply either sessionId/instance_url or username/password")
        if not sessionId:
            sessionId, endpoint = SalesforceBulk.login_to_salesforce(
                username, password, sandbox=sandbox)
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
        self.job_content_types = {}  # dict of job_id => contentType
        self.batch_statuses = {}
        self.exception_class = exception_class

    @staticmethod
    def login_to_salesforce(username, password, sandbox=False):
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
            username=username, password=password, sandbox=sandbox)
        return packet['access_token'], packet['instance_url']

    def headers(self, values={}, content_type='application/xml'):
        default = {"X-SFDC-Session": self.sessionId,
                   "Content-Type": "{}; charset=UTF-8".format(content_type)}
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

        resp = requests.post(self.endpoint + "/job",
                             headers=self.headers(),
                             data=doc)
        self.check_status(resp)

        tree = ET.fromstring(resp.text)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id
        self.job_content_types[job_id] = contentType

        return job_id

    def check_status(self, resp):
        if resp.status >= 400:
            msg = "Bulk API HTTP Error result: {0}".format(resp.text)
            self.raise_error(msg, resp.status)

    def close_job(self, job_id):
        doc = self.create_close_job_doc()
        url = self.endpoint + "/job/%s" % job_id
        resp = requests.post(url, headers=self.headers(), data=doc)
        self.check_status(resp)

    def abort_job(self, job_id):
        """Abort a given bulk job"""
        doc = self.create_abort_job_doc()
        url = self.endpoint + "/job/%s" % job_id
        resp = requests.post(
            url,
            headers=self.headers(),
            data=doc
        )
        self.check_status(resp)

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

    def create_close_job_doc(self):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Closed"

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    def create_abort_job_doc(self):
        """Create XML doc for aborting a job"""
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Aborted"

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    # Add a BulkQuery to the job - returns the batch id
    def query(self, job_id, soql, contentType='CSV'):
        if job_id is None:
            job_id = self.create_job(
                re.search(re.compile("from (\w+)", re.I), soql).group(1),
                "query", contentType=contentType)

        job_content_type = self.job_content_types[job_id]
        http_content_type = job_to_http_content_type[job_content_type]

        headers = self.headers(content_type=http_content_type)

        uri = self.endpoint + "/job/%s/batch" % job_id
        resp = requests.post(uri, data=soql, headers=headers)

        self.check_status(resp)

        tree = ET.fromstring(self.text)
        batch_id = tree.findtext("{%s}id" % self.jobNS)

        self.batches[batch_id] = job_id

        return batch_id

    def raise_error(self, message, status_code=None):
        if status_code:
            message = "[{0}] {1}".format(status_code, message)

        if self.exception_class == BulkApiError:
            raise self.exception_class(message, status_code=status_code)
        else:
            raise self.exception_class(message)

    def post_batch(self, job_id, data_generator):
        job_content_type = self.job_content_types[job_id]
        http_content_type = job_to_http_content_type[job_content_type]

        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers(content_type=http_content_type)
        resp = requests.post(uri, data=data_generator, headers=headers)
        content = resp.content

        if resp.status_code >= 400:
            self.raise_error(content, resp.status_code)

        tree = ET.fromstring(content)
        batch_id = tree.findtext("{%s}id" % self.jobNS)
        return batch_id

    def lookup_job_id(self, batch_id):
        try:
            return self.batches[batch_id]
        except KeyError:
            raise Exception(
                "Batch id '%s' is uknown, can't retrieve job_id" % batch_id)

    def job_status(self, job_id=None):
        job_id = job_id
        uri = urlparse.urljoin(self.endpoint + "/", 'job/{0}'.format(job_id))
        response = requests.get(uri, headers=self.headers())
        self.check_status(response)
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

        uri = self.endpoint + \
            "/job/%s/batch/%s" % (job_id, batch_id)
        resp = requests.get(uri, headers=self.headers())
        self.check_status(resp)

        tree = ET.fromstring(resp.content)
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
        find_func = getattr(tree, 'iterfind', tree.findall)
        return [str(r.text) for r in
                find_func("{{{0}}}result".format(self.jobNS))]

    def get_all_results_for_batch(self, batch_id, job_id=None):
        """
        Gets result ids and generates each result set from the batch and returns it
        as an generator fetching the next result set when needed

        Args:
            batch_id: id of batch
            job_id: id of job, if not provided, it will be looked up
        """
        result_ids = self.get_batch_result_ids(batch_id, job_id=job_id)
        if not result_ids:
            raise RuntimeError('Batch is not complete')
        for result_id in result_ids:
            yield self.get_batch_results(
                batch_id,
                result_id,
                job_id=job_id)

    def get_batch_results(self, batch_id, result_id, job_id=None, chunk_size=None):
        job_id = job_id or self.lookup_job_id(batch_id)

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result/{2}".format(
                job_id, batch_id, result_id),
        )
        resp = requests.get(uri, headers=self.headers(), stream=True)
        self.check_status(resp)

        return resp.iter_lines(chunk_size=chunk_size)
