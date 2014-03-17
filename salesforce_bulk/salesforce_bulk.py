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

UploadResult = namedtuple('UploadResult', 'id success created error')


def dump_results(data, failed, count):
    if hasattr(data, 'read'):
        print data.read()
    else:
        print data

class BulkApiError(Exception):
    pass

class SalesforceBulk(object):
    def __init__(self, sessionId = None, host=None, username=None, password=None):
        if not sessionId and not username:
            raise RuntimeError("Must supply either sessionId/instance_url or username/password")
        if not sessionId:
            sessionId, endpoint = SalesforceBulk.login_to_salesforce(username, password)
            host = urlparse.urlparse(self.h._location)
            host = host.hostname.replace("-api","")

        if host[0:4] == 'http':
            self.endpoint = host
        else:
            self.endpoint = "https://" + host
        self.sessionId = sessionId
        #print "Creating bulk adapter, endpoint %s, id: %s" % (endpoint, sessionId)
        self.jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
        self.jobs = {} # dict of job_id => job_id
        self.batches = {} # dict of batch_id => job_id
        self.batch_statuses = {}

    @staticmethod
    def login_to_salesforce(username, password):
        for key in ['SALESFORCE_CLIENT_ID','SALESFORCE_CLIENT_SECRET','SALESFORCE_REDIRECT_URI']:
            if key not in os.environ:
                raise RuntimeError("You must set %s to use username/pass login" % key)

        try:
            import salesforce_oauth_request
        except ImportError:
            raise ImportError("You must install salesforce-oauth-request to use username/password")

        packet = salesforce_oauth_request.login(username=username, password=password)
        return packet['access_token'],packet['instance_url']

    def headers(self, values = {}):
        default = {"X-SFDC-Session" : self.sessionId, "Content-Type" : "application/xml; charset=UTF-8"}
        for k, val in values.iteritems():
            default[k] = val
        return default

    # Register a new Bulk API job - returns the job id
    def create_query_job(self, object_name, **kwargs):
        return self.create_job(object_name, "query", **kwargs)

    def create_insert_job(self, object_name, **kwargs):
        return self.create_job(object_name, "insert", **kwargs)

    def create_update_job(self, object_name, **kwargs):
        return self.create_job(object_name, "update", **kwargs)

    def create_delete_job(self, object_name, **kwargs):
        return self.create_job(object_name, "delete", **kwargs)

    def create_job(self, object_name = None, operation = None, contentType = 'CSV', concurrency=None):
        assert(object_name is not None)
        assert(operation is not None)

        doc = self.create_job_doc(object_name=object_name,
                                  operation=operation,
                                  contentType=contentType,
                                  concurrency=concurrency)

        http = Http()
        resp, content = http.request(self.endpoint + "/services/async/29.0/job", "POST", headers=self.headers(), body=doc)

        self.check_status(resp, content)

        tree = ET.fromstring(content)
        job_id = tree.findtext("{%s}id" % self.jobNS)
        self.jobs[job_id] = job_id

        return job_id

    def check_status(self, resp, content):
        if resp.status >= 400:
            print "Non-200 status: %d" % resp.status
            raise Exception(content)

    def close_job(self, job_id):
        doc = self.create_close_job_doc()
        http = Http()
        resp, content = http.request(self.endpoint + "/services/async/29.0/job/%s" % job_id, "POST",
            headers=self.headers(), body=doc)
        self.check_status(resp, content)


    def create_job_doc(self, object_name = None, operation = None, contentType = 'CSV', concurrency=None):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        op = ET.SubElement(root, "operation")
        op.text  = operation
        obj = ET.SubElement(root, "object")
        obj.text = object_name
        if concurrency:
            con = ET.SubElement(root, "concurrencyMode")
            con.text = concurrency
        ct = ET.SubElement(root, "contentType")
        ct.text = contentType

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding ="UTF-8")
        return buf.getvalue()

    def create_close_job_doc(self, object_name = None, operation = None, contentType = 'CSV'):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)
        state = ET.SubElement(root, "state")
        state.text  = "Closed"

        buf = StringIO.StringIO()
        tree = ET.ElementTree(root)
        tree.write(buf, encoding ="UTF-8")
        return buf.getvalue()

    # Add a BulkQuery to the job - returns the batch id
    def query(self, job_id, soql):
        if job_id is None:
            job_id = self.create_job(re.search(re.compile("from (\w+)",re.I), soql).group(1), "query")
        http = Http()
        uri = self.endpoint + "/services/async/29.0/job/%s/batch" % job_id
        resp, content = http.request(uri, method="POST", body=soql, headers=self.headers({"Content-Type":"text/csv"}))

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
            if not i % 100:
                print i / 100
            if not i % batch_size:
                print 'New batch at %s' % i
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

        uri = self.endpoint + "/services/async/29.0/job/%s/batch" % job_id
        for batch in batches:
            print len(batch)
            resp = requests.post(uri, data=batch, headers=self.headers({"Content-Type":"text/csv"}))
            content = resp.content

            if resp.status_code >= 400:
                print "Non-200 status: %d" % resp.status
                raise Exception(content)

            tree = ET.fromstring(content)
            batch_id = tree.findtext("{%s}id" % self.jobNS)

            self.batches[batch_id] = job_id
            batch_ids.append(batch_id)

        return batch_ids

    def post_bulk_batch(self, job_id, csv_generator):
        uri = self.endpoint + "/services/async/29.0/job/%s/batch" % job_id
        resp = requests.post(uri, data=csv_generator, headers=self.headers({"Content-Type":"text/csv"}))
        content = resp.content

        if resp.status_code >= 400:
            print "Non-200 status: %d" % resp.status_code
            raise Exception(content)

        tree = ET.fromstring(content)
        batch_id = tree.findtext("{%s}id" % self.jobNS)
        return batch_id

    # Add a BulkDelete to the job - returns the batch id
    def bulk_delete(self, job_id, object_type, where, batch_size=2500):
        query_job_id = self.create_query_job(object_type)
        soql = "Select Id from %s where %s Limit 10000" % (object_type, where)
        print soql
        query_batch_id = self.query(query_job_id, soql)
        self.wait_for_batch(query_job_id, query_batch_id, timeout=120)

        results = []
        def save_results(tf, **kwargs):
            results.append(tf.read())

        flag = self.get_batch_results(query_job_id, query_batch_id, callback = save_results)

        if job_id is None:
            job_id = self.create_job(object_type, "delete")
        http = Http()
        # Split a large CSV into manageable batches
        batches = self.split_csv(csv, batch_size)
        batch_ids = []

        uri = self.endpoint + "/services/async/29.0/job/%s/batch" % job_id
        for batch in results:
            print len(batch)
            print batch
            print batch.split('\n')
            resp = requests.post(uri, data=batch, headers=self.headers({"Content-Type":"text/csv"}))
            content = resp.content

            if resp.status_code >= 400:
                print "Non-200 status: %d" % resp.status
                raise Exception(content)

            tree = ET.fromstring(content)
            batch_id = tree.findtext("{%s}id" % self.jobNS)

            self.batches[batch_id] = job_id
            batch_ids.append(batch_id)

        return batch_ids

    def lookup_job_id(self, batch_id):
        try:
            return self.batches[batch_id]
        except KeyError:
            raise Exception("Batch id '%s' is uknown, can't retrieve job_id" % batch_id)

    def batch_status(self, job_id = None, batch_id = None, reload=False):
        if not reload and batch_id in self.batch_statuses:
            return self.batch_statuses[batch_id]

        job_id = job_id or self.lookup_job_id(batch_id)

        http = Http()
        uri = self.endpoint + "/services/async/29.0/job/%s/batch/%s" % (job_id, batch_id)
        resp, content = http.request(uri, headers=self.headers())
        self.check_status(resp, content)

        tree = ET.fromstring(content)
        result = {}
        for child in tree:
            result[re.sub("{.*?}","",child.tag)] = child.text

        self.batch_statuses[batch_id] = result
        return result

    def batch_state(self, job_id, batch_id, reload=False):
        status = self.batch_status(job_id, batch_id, reload=reload)
        if 'state' in status:
            return status['state']
        else:
            return None

    def is_batch_done(self, job_id, batch_id):
        state = self.batch_state(job_id, batch_id, reload=True)
        print "BULK STATE IS: %s" % state
        if state == 'Failed' or state == 'Not Processed':
            status = self.batch_status(job_id, batch_id)
            if state == 'Failed':
                self.close_job(job_id)
            raise BulkApiError("Batch %s of job %s failed: %s" % (batch_id, job_id, status['stateMessage']))
        return state == 'Completed'

    # Wait for the given batch to complete, waiting at most timeout seconds (defaults to 10 minutes).
    def wait_for_batch(self, job_id, batch_id, timeout = 60*10, sleep_interval=10):
        waited = 0
        while not self.is_batch_done(job_id, batch_id) and waited < timeout:
            time.sleep(sleep_interval)
            waited += sleep_interval


    # Retrieve the results of a batch query. Your callback will be invoked 0 or more times
    # with a TemporaryFile object from which you can read the returned data. If the batch is not
    # ready then your callback may not be invoked.
    # Returns False if the batch isn't Completed, or True if it is and was processed.
    #
    # By default your callback will receive a File-like object with the raw results. You can
    # pass 'parse_csv = True' to request that CSV data be parsed. In this case your callback
    # will receive two parameters. The first will be a list of successfully parsed rows.
    # Each row will contained a list of values. The column names will appear as the first row.
    # So for csv data:
    #
    #   "field1","field2"
    #   "row1col1","row1col2"
    #
    # You will receive: [["field1","field2"],["row1col1","row2col2"]]
    #
    # The second argument will be a list of records which failed parsing. The format of each row is:
    #    [<line number>,<error message>, <csv record>]

    # You can pass batch_size=N to have your callback invoked with batches of at most N rows. Note
    # that row[0] of *every* batch will hold the list of columns names.
    #
    # This call is resumable. You should record the 'line' (int) and 'result_id' (str) passed to your callback,
    # and if you need to restart in the middle you can pass those values back as 'skip_lines' and
    # 'skip_to_result' in the call. We will move ahead to the indicated result file, then skip
    # the count of lines before sending additional rows to your callback.

    def get_batch_results(self, job_id, batch_id, callback = dump_results, parse_csv=False, batch_size=0,
                          skip_lines=None, skip_to_result=None, logger=None):
        job_id = job_id or self.lookup_job_id(batch_id)

        if not self.is_batch_done(job_id, batch_id):
            return False
        http = Http()
        uri = self.endpoint + "/services/async/29.0/job/%s/batch/%s/result" % (job_id, batch_id)
        resp, content = http.request(uri, method="GET", headers=self.headers())

        tree = ET.fromstring(content)
        for result in tree.iterfind("{%s}result" % self.jobNS):
            result_id = str(result.text)

            if skip_to_result and result_id != skip_to_result:
                logger("Skipping processed bulk result '%s'" % result_id)
                continue

            ruri = self.endpoint + "/services/async/29.0/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
            if logger:
                logger("Downloading bulk result file: %s" % result_id)
                #print "$$$$$$$$$$ Downloading bulk result file: %s" % ruri
            resp = requests.get(ruri, headers=self.headers(), stream=True)
            tf = TemporaryFile()
            for chunk in resp.iter_content(1024):
                tf.write(chunk) # assume we're writing utf8
                print "##"
            resp.close()

            total_remaining = self.count_file_lines(tf)
            if skip_lines:
                total_remaining -= skip_lines
                logger("Skipping %d lines of bulk file" % skip_lines)

            if logger:
                logger("Total records: %d" % total_remaining)

            tf.seek(0)
            if not parse_csv:
                callback(tf)
            else:
                records = []
                line_number = 0
                col_names = []
                reader = csv.reader(tf, delimiter=",", quotechar='"')
                for row in reader:
                    line_number += 1
                    if line_number > 1 and skip_lines and line_number <= skip_lines:
                        continue
                    records.append(row)
                    if len(records) == 1:
                        col_names = records[0]
                    if batch_size > 0 and len(records) >= (batch_size+1):
                        callback(records, remaining=total_remaining, line=line_number, result_id=result_id)
                        total_remaining -= (len(records)-1)
                        records = [col_names]
                callback(records, remaining=total_remaining, line=line_number, result_id=result_id)

            tf.close()

        return True

    def get_batch_result_iter(self, job_id, batch_id, parse_csv=False, logger=None):
        """Return a line interator over the contents of a batch result document. If csv=True then parses the first line
        as the csv header and the iterator returns dicts."""
        status = self.batch_status(job_id, batch_id)
        if status['state'] != 'Completed':
            return None
        elif logger:
            if 'numberRecordsProcessed' in status:
                logger("Bulk batch %d processed %s records" % (batch_id, status['numberRecordsProcessed']))
            if 'numberRecordsFailed' in status:
                failed = int(status['numberRecordsFailed'])
                if failed > 0:
                    logger("Bulk batch %d had %d failed records" % (batch_id, failed))

        uri = self.endpoint + "/services/async/29.0/job/%s/batch/%s/result" % (job_id, batch_id)
        r = requests.get(uri, headers=self.headers(), stream=True)
        if parse_csv:
            return csv.DictReader(r.iter_lines(chunk_size=2048), delimiter=",", quotechar='"')
        else:
            return r.iter_lines(chunk_size=2048)


    def get_upload_results(self, job_id, batch_id, callback = dump_results, batch_size=0, logger=None):
        job_id = job_id or self.lookup_job_id(batch_id)

        if not self.is_batch_done(job_id, batch_id):
            return False
        http = Http()
        uri = self.endpoint + "/services/async/29.0/job/%s/batch/%s/result" % (job_id, batch_id)
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
            if batch_size > 0 and len(records) >= (batch_size+1):
                callback(records, total_remaining, line_number)
                total_remaining -= (len(records)-1)
                records = [col_names]
        callback(records, total_remaining, line_number)

        tf.close()

        return True

    def parse_csv(self, tf, callback, batch_size, total_remaining):
        print batch_size, total_remaining
        records = []
        line_number = 0
        col_names = []
        reader = csv.reader(tf, delimiter=",", quotechar='"')
        for row in reader:
            line_number += 1
            records.append(row)
            if len(records) == 1:
                col_names = records[0]
            if batch_size > 0 and len(records) >= (batch_size+1):
                callback(records, total_remaining, line_number)
                total_remaining -= (len(records)-1)
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
        