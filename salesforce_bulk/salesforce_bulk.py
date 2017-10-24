# Interface to the Salesforce BULK API
from __future__ import absolute_import

import io
import json
import re
import time
import xml.etree.ElementTree as ET

from collections import namedtuple
from itertools import islice
from operator import itemgetter

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

from six import BytesIO as StringIO
from six import text_type

import requests
from simple_salesforce import SalesforceLogin
import unicodecsv

from . import util
from . import bulk_states

UploadResult = namedtuple('UploadResult', 'id success created error')

nsclean = re.compile('{.*}')


class BulkApiError(Exception):

    def __init__(self, message, status_code=None):
        super(BulkApiError, self).__init__(message)
        self.status_code = status_code

    def __reduce__(self):
        return BulkApiError, (self.args[0], self.status_code)


class BulkJobAborted(BulkApiError):

    def __init__(self, job_id):
        self.job_id = job_id

        message = 'Job {0} aborted'.format(job_id)
        super(BulkJobAborted, self).__init__(message)

    def __reduce__(self):
        return BulkJobAborted, (self.job_id,)


class BulkBatchFailed(BulkApiError):

    def __init__(self, job_id, batch_id, state_message, state=None):
        self.job_id = job_id
        self.batch_id = batch_id
        self.state_message = state_message
        self.state = state

        message = 'Batch {0} of job {1} failed: {2}'.format(batch_id, job_id,
                                                            state_message)
        super(BulkBatchFailed, self).__init__(message)

    def __reduce__(self):
        return BulkBatchFailed, (self.job_id, self.batch_id, self.state_message, self.state)


job_to_http_content_type = {
    'CSV': 'text/csv',
    'XML': 'application/xml',
    'JSON': 'application/json',
}

DEFAULT_CLIENT_ID_PREFIX = 'PySFBulk'
DEFAULT_API_VERSION = "40.0"


class SalesforceBulk(object):

    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 API_version=DEFAULT_API_VERSION, sandbox=False,
                 security_token=None, organizationId=None, client_id=None):
        if not sessionId and not username:
            raise RuntimeError(
                "Must supply either sessionId/instance_url or username/password")
        if not sessionId:
            sessionId, host = SalesforceBulk.login_to_salesforce(
                username, password, sandbox=sandbox, security_token=security_token,
                organizationId=organizationId, API_version=API_version, client_id=client_id)

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
        self.API_version = API_version

    @staticmethod
    def login_to_salesforce(username, password, sandbox=False, security_token=None,
                            organizationId=None, client_id=None, API_version=DEFAULT_API_VERSION):
        if client_id:
            client_id = "{prefix}/{app_name}".format(
                prefix=DEFAULT_CLIENT_ID_PREFIX,
                app_name=client_id)
        else:
            client_id = DEFAULT_CLIENT_ID_PREFIX

        if all(arg is not None for arg in (
                username, password, security_token)):

            # Pass along the username/password to our login helper
            return SalesforceLogin(
                username=username,
                password=password,
                security_token=security_token,
                sandbox=sandbox,
                sf_version=API_version,
                client_id=client_id)

        elif all(arg is not None for arg in (
                username, password, organizationId)):

            # Pass along the username/password to our login helper
            return SalesforceLogin(
                username=username,
                password=password,
                organizationId=organizationId,
                sandbox=sandbox,
                sf_version=API_version,
                client_id=client_id)

        else:
            raise TypeError(
                'You must provide login information or an instance and token'
            )

    def headers(self, values={}, content_type='application/xml'):
        default = {
            "X-SFDC-Session": self.sessionId,
            "Content-Type": "{}; charset=UTF-8".format(content_type),
            'Accept-Encoding': "gzip",
        }
        default.update(values)
        return default

    # Register a new Bulk API job - returns the job id
    def create_query_job(self, object_name, **kwargs):
        return self.create_job(object_name, "query", **kwargs)

    def create_queryall_job(self, object_name, **kwargs):
        """ only supported since version 39.0 """
        return self.create_job(object_name, "queryAll", **kwargs)

    def create_insert_job(self, object_name, **kwargs):
        return self.create_job(object_name, "insert", **kwargs)

    def create_upsert_job(self, object_name, external_id_name, **kwargs):
        return self.create_job(object_name, "upsert", external_id_name=external_id_name, **kwargs)

    def create_update_job(self, object_name, **kwargs):
        return self.create_job(object_name, "update", **kwargs)

    def create_delete_job(self, object_name, **kwargs):
        return self.create_job(object_name, "delete", **kwargs)

    def create_job(self, object_name=None, operation=None, contentType='CSV',
                   concurrency=None, external_id_name=None, pk_chunking=False):
        assert(object_name is not None)
        assert(operation is not None)

        extra_headers = {}
        if pk_chunking:
            if pk_chunking is True:
                pk_chunking = u'true'
            elif isinstance(pk_chunking, int):
                pk_chunking = u'chunkSize=%d;' % pk_chunking
            else:
                pk_chunking = text_type(pk_chunking)

            extra_headers['Sforce-Enable-PKChunking'] = pk_chunking

        doc = self.create_job_doc(object_name=object_name,
                                  operation=operation,
                                  contentType=contentType,
                                  concurrency=concurrency,
                                  external_id_name=external_id_name)

        resp = requests.post(self.endpoint + "/job",
                             headers=self.headers(extra_headers),
                             data=doc)
        self.check_status(resp)

        tree = ET.fromstring(resp.content)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id
        self.job_content_types[job_id] = contentType

        return job_id

    def check_status(self, resp):
        if resp.status_code >= 400:
            msg = "Bulk API HTTP Error result: {0}".format(resp.text)
            self.raise_error(msg, resp.status_code)

    def get_batch_list(self, job_id):
        url = self.endpoint + "/job/{}/batch".format(job_id)
        resp = requests.get(url, headers=self.headers())
        self.check_status(resp)
        results = self.parse_response(resp)
        if isinstance(results, dict):
            return results['batchInfo']

        return results

    def get_query_batch_request(self, batch_id, job_id=None):
        """ Fetch the request sent for the batch. Note should only used for query batches """
        if not job_id:
            job_id = self.lookup_job_id(batch_id)

        url = self.endpoint + "/job/{}/batch/{}/request".format(job_id, batch_id)
        resp = requests.get(url, headers=self.headers())
        self.check_status(resp)
        return resp.text

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

        buf = StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    def create_close_job_doc(self):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Closed"

        buf = StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding="UTF-8")
        return buf.getvalue()

    def create_abort_job_doc(self):
        """Create XML doc for aborting a job"""
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text = "Aborted"

        buf = StringIO()
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

        result = self.parse_response(resp)
        batch_id = result['id']

        self.batches[batch_id] = job_id

        return batch_id

    def raise_error(self, message, status_code=None):
        if status_code:
            message = "[{0}] {1}".format(status_code, message)

        raise BulkApiError(message, status_code=status_code)

    def post_batch(self, job_id, data_generator):
        job_content_type = self.job_content_types[job_id]
        http_content_type = job_to_http_content_type[job_content_type]

        uri = self.endpoint + "/job/%s/batch" % job_id
        headers = self.headers(content_type=http_content_type)
        resp = requests.post(uri, data=data_generator, headers=headers)
        self.check_status(resp)

        result = self.parse_response(resp)

        batch_id = result['id']
        self.batches[batch_id] = job_id
        return batch_id

    def post_mapping_file(self, job_id, mapping_data):
        job_content_type = 'CSV'
        http_content_type = job_to_http_content_type[job_content_type]
        uri = self.endpoint + "/job/%s/spec" % job_id
        headers = self.headers(content_type=http_content_type)
        resp = requests.post(uri, data=mapping_data, headers=headers)
        self.check_status(resp)

        if resp.status_code != 201:
            raise Exception("Unable to upload mapping file")

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

        tree = ET.fromstring(response.content)
        result = {}
        for child in tree:
            result[nsclean.sub("", child.tag)] = child.text
        return result

    def job_state(self, job_id):
        status = self.job_status(job_id)
        if 'state' in status:
            return status['state']
        else:
            return None

    def parse_response(self, resp):
        if resp.headers['Content-Type'] == 'application/json':
            return resp.json()

        tree = ET.fromstring(resp.content)
        if nsclean.sub("", tree.tag) == 'batchInfoList':
            results = []
            for subtree in tree:
                result = {}
                results.append(result)
                for child in subtree:
                    result[nsclean.sub("", child.tag)] = child.text

            return results

        result = {}
        for child in tree:
            result[nsclean.sub("", child.tag)] = child.text

        return result

    def batch_status(self, batch_id=None, job_id=None, reload=False):
        if not reload and batch_id in self.batch_statuses:
            return self.batch_statuses[batch_id]

        job_id = job_id or self.lookup_job_id(batch_id)

        uri = self.endpoint + \
            "/job/%s/batch/%s" % (job_id, batch_id)
        resp = requests.get(uri, headers=self.headers())
        self.check_status(resp)

        result = self.parse_response(resp)

        self.batch_statuses[batch_id] = result
        return result

    def batch_state(self, batch_id, job_id=None, reload=False):
        status = self.batch_status(batch_id, job_id, reload=reload)
        if 'state' in status:
            return status['state']
        else:
            return None

    def is_batch_done(self, batch_id, job_id=None):
        batch_state = self.batch_state(batch_id, job_id=job_id, reload=True)
        if batch_state in bulk_states.ERROR_STATES:
            status = self.batch_status(batch_id, job_id)
            raise BulkBatchFailed(job_id, batch_id, status.get('stateMessage'), batch_state)
        return batch_state == bulk_states.COMPLETED

    # Wait for the given batch to complete, waiting at most timeout seconds
    # (defaults to 10 minutes).
    def wait_for_batch(self, job_id, batch_id, timeout=60 * 10,
                       sleep_interval=10):
        waited = 0
        while not self.is_batch_done(batch_id, job_id) and waited < timeout:
            time.sleep(sleep_interval)
            waited += sleep_interval

    def get_query_batch_result_ids(self, batch_id, job_id=None):
        job_id = job_id or self.lookup_job_id(batch_id)
        if not self.is_batch_done(batch_id, job_id):
            return False

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result".format(
                job_id, batch_id),
        )
        resp = requests.get(uri, headers=self.headers())
        self.check_status(resp)

        if resp.headers['Content-Type'] == 'application/json':
            return resp.json()

        tree = ET.fromstring(resp.content)
        find_func = getattr(tree, 'iterfind', tree.findall)
        return [str(r.text) for r in
                find_func("{{{0}}}result".format(self.jobNS))]

    def get_all_results_for_query_batch(self, batch_id, job_id=None, chunk_size=2048):
        """
        Gets result ids and generates each result set from the batch and returns it
        as an generator fetching the next result set when needed

        Args:
            batch_id: id of batch
            job_id: id of job, if not provided, it will be looked up
        """
        result_ids = self.get_query_batch_result_ids(batch_id, job_id=job_id)
        if not result_ids:
            raise RuntimeError('Batch is not complete')
        for result_id in result_ids:
            yield self.get_query_batch_results(
                batch_id,
                result_id,
                job_id=job_id,
                chunk_size=chunk_size
            )

    def get_query_batch_results(self, batch_id, result_id, job_id=None, chunk_size=2048, raw=False):
        job_id = job_id or self.lookup_job_id(batch_id)

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result/{2}".format(
                job_id, batch_id, result_id),
        )

        resp = requests.get(uri, headers=self.headers(), stream=True)
        self.check_status(resp)
        if raw:
            return resp.raw

        iter = (x.replace(b'\0', b'') for x in resp.iter_content(chunk_size=chunk_size))
        return util.IteratorBytesIO(iter)

    def get_batch_results(self, batch_id, job_id=None):
        job_id = job_id or self.lookup_job_id(batch_id)

        uri = urlparse.urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result".format(
                job_id, batch_id),
        )

        resp = requests.get(uri, headers=self.headers(), stream=True)
        self.check_status(resp)

        iter = (x.replace(b'\0', b'') for x in resp.iter_content())
        fd = util.IteratorBytesIO(iter)
        if resp.headers['Content-Type'] == 'application/json':

            result = json.load(io.TextIOWrapper(fd, 'utf-8'))
            getter = itemgetter('id', 'success', 'created', 'errors')
            return [UploadResult(*getter(row)) for row in result]

        elif resp.headers['Content-Type'] == 'text/csv':
            reader = unicodecsv.reader(
                fd, encoding='utf-8'
            )
            results = islice(reader, 1, None)
            results = [
                UploadResult(*row)
                for row in results
            ]
            return results
        elif resp.headers['Content-Type'] == 'application/xml':
            tree = ET.parse(fd)

            def getid(x):
                x is not None and x.text

            results = [
                UploadResult(
                    getid(result.find('{%s}id' % self.jobNS)),
                    result.find('{%s}success' % self.jobNS).text == 'true',
                    result.find('{%s}created' % self.jobNS).text == 'true',
                    [
                        self.parse_error_result_xml(x)
                        for x in result.findall('{%s}errors' % self.jobNS)
                    ]
                )
                for result in tree.getroot()
            ]
            return results

        # NOTE raise exception if we get here

    def parse_error_result_xml(self, error_xml):
        return {
            'fields': [x.text for x in error_xml.findall('{%s}fields' % self.jobNS)],
            'message': error_xml.find('{%s}message' % self.jobNS).text,
            'statusCode': error_xml.find('{%s}statusCode' % self.jobNS).text,
        }
