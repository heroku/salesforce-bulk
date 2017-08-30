![travis-badge](https://travis-ci.org/heroku/salesforce-bulk.svg?branch=master)

# Salesforce Bulk

Python client library for accessing the asynchronous Salesforce.com Bulk API.

## Installation

As of right now these docs are out of sync with the version in pypi. To install this version please run:
```pip install 'https://github.com/heroku/salesforce-bulk/archive/v2.0.0dev6.zip#egg=salesforce-bulk==2.0.0dev6'```
or add
```https://github.com/heroku/salesforce-bulk/archive/v2.0.0dev6.zip#egg=salesforce-bulk==2.0.0dev6```
to your requirements.txt file. Once the 2.0.0 final is released you'll be able to install as normal using:

```pip install salesforce-bulk```

## Authentication

To access the Bulk API you need to authenticate a user into Salesforce. The easiest
way to do this is just to supply `username`, `password` and `security_token`. This library
will use the `simple-salesforce` package to handle password based authentication.

```
from salesforce_bulk import SalesforceBulk

bulk = SalesforceBulk(username=username, password=password, security_token=security_token)
...
```

Alternatively if you run have access to a session ID and instance_url you can use
those directly:

```
from urlparse import urlparse
from salesforce_bulk import SalesforceBulk

bulk = SalesforceBulk(sessionId=sessionId, host=urlparse(instance_url).hostname)
...
```

## Operations

The basic sequence for driving the Bulk API is:

1. Create a new job
2. Add one or more batches to the job
3. Close the job
4. Wait for each batch to finish


## Bulk Query

`bulk.create_query_job(object_name, contentType='JSON')`

Using API v39.0 or higher, you can also use the queryAll operation:

`bulk.create_queryall_job(object_name, contentType='JSON')`

Example
```
from salesforce_bulk.util import IteratorBytesIO
import json
job = bulk.create_query_job("Contact", contentType='JSON')
batch = bulk.query(job, "select Id,LastName from Contact")
bulk.close_job(job)
while not bulk.is_batch_done(batch):
    sleep(10)

for result in bulk.get_all_results_for_query_batch(batch):
    result = json.load(IteratorBytesIO(result))
    for row in result:
        print row # dictionary rows
```

Same example but for CSV:

```
import unicodecsv
job = bulk.create_query_job("Contact", contentType='CSV')
batch = bulk.query(job, "select Id,LastName from Contact")
bulk.close_job(job)
while not bulk.is_batch_done(batch):
    sleep(10)

for result in bulk.get_all_results_for_query_batch(batch):
    reader = unicodecsv.DictReader(result, encoding='utf-8')
    for row in reader:
        print row # dictionary rows
```

Note that while CSV is the default for historical reasons, JSON should be prefered since CSV
has some drawbacks including its handling of NULL vs empty string.


## Bulk Insert, Update, Delete

All Bulk upload operations work the same. You set the operation when you create the
job. Then you submit one or more documents that specify records with columns to
insert/update/delete. When deleting you should only submit the Id for each record.

For efficiency you should use the `post_batch` method to post each batch of
data. (Note that a batch can have a maximum 10,000 records and be 1GB in size.)
You pass a generator or iterator into this function and it will stream data via
POST to Salesforce. For help sending CSV formatted data you can use the
salesforce_bulk.CsvDictsAdapter class. It takes an iterator returning dictionaries
and returns an iterator which produces CSV data.

Full example:

```
from salesforce_bulk import CsvDictsAdapter

job = bulk.create_insert_job("Account", contentType='CSV')
accounts = [dict(Name="Account%d" % idx) for idx in xrange(5)]
csv_iter = CsvDictsAdapter(iter(accounts))
batch = bulk.post_batch(job, csv_iter)
bulk.wait_for_batch(job, batch)
bulk.close_job(job)
print "Done. Accounts uploaded."
```

### Concurrency mode

When creating the job, pass `concurrency='Serial'` or `concurrency='Parallel'` to set the
concurrency mode for the job.
