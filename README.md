# Salesforce Bulk

Python client library for accessing the asynchronous Salesforce.com Bulk API.

## Installation

```pip install salesforce-bulk```

## Authentication

To access the Bulk API you need to authenticate a user into Salesforce. The easiest
way to do this is just to supply `username` and `password`. This library
will use the `salesforce-oauth-request` package (which you must install) to run
the Salesforce OAUTH2 Web flow and return an access token.

```
from salesforce_bulk import SalesforceBulk

bulk = SalesforceBulk(username=username,password=password)
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
3. Wait for each batch to finish
4. Close the job


## Bulk Query

`SalesforceBulk.create_query_job(object_name, contentType='CSV', concurrency=None)`

Example

```
job = bulk.create_query_job("Contact", contentType='CSV')
batch = bulk.query(job, "select Id,LastName from Contact")
while not bulk.is_batch_done(job, batch):
	sleep(10)
bulk.close_job(job)

for row in bulk.get_batch_result_iter(job, batch, parse_csv=True):
	print row   #row is a dict
```

## Bulk Insert, Update, Delete

All Bulk upload operations work the same. You set the operation when you create the
job. Then you submit one or more documents that specify records with columns to
insert/update/delete. When deleting you should only submit the Id for each record.

For efficiency you should use the `post_bulk_batch` method to post each batch of
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

batch = bulk.post_bulk_batch(job, csv_iter)

bulk.wait_for_batch(job, batch)

bulk.close_job(job)

print "Done. Accounts uploaded."
```

### Concurrency mode

When creating the job, pass `concurrency=Serial` or `concurrency=Parallel` to set the
concurrency mode for the job.





