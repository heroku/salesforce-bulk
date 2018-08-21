.. figure:: https://travis-ci.org/heroku/salesforce-bulk.svg?branch=master
   :alt: travis-badge

Salesforce Bulk
===============

Python client library for accessing the asynchronous Salesforce.com Bulk
API.

Installation
------------
.. code-block:: bash

    pip install salesforce-bulk

Authentication
--------------

To access the Bulk API you need to authenticate a user into Salesforce.
The easiest way to do this is just to supply ``username``, ``password``
and ``security_token``. This library will use the ``simple-salesforce``
package to handle password based authentication.

.. code-block:: python

    from salesforce_bulk import SalesforceBulk

    bulk = SalesforceBulk(username=username, password=password, security_token=security_token)
    ...

Alternatively if you run have access to a session ID and instance\_url
you can use those directly:

.. code-block:: python

    from urlparse import urlparse
    from salesforce_bulk import SalesforceBulk

    bulk = SalesforceBulk(sessionId=sessionId, host=urlparse(instance_url).hostname)
    ...

Operations
----------

The basic sequence for driving the Bulk API is:

1. Create a new job
2. Add one or more batches to the job
3. Close the job
4. Wait for each batch to finish

Bulk Query
----------

``bulk.create_query_job(object_name, contentType='JSON')``

Using API v39.0 or higher, you can also use the queryAll operation:

``bulk.create_queryall_job(object_name, contentType='JSON')``

Example

.. code-block:: python

    import json
    from salesforce_bulk.util import IteratorBytesIO

    job = bulk.create_query_job("Contact", contentType='JSON')
    batch = bulk.query(job, "select Id,LastName from Contact")
    bulk.close_job(job)
    while not bulk.is_batch_done(batch):
        sleep(10)

    for result in bulk.get_all_results_for_query_batch(batch):
        result = json.load(IteratorBytesIO(result))
        for row in result:
            print row # dictionary rows

Same example but for CSV:

.. code-block:: python

    import unicodecsv
    
    job = bulk.create_query_job("Contact", contentType='CSV')
    batch = bulk.query(job, "select Id,LastName from Contact")
    bulk.close_job(job)
    while not bulk.is_batch_done(batch):
        sleep(10)

    for result in bulk.get_all_results_for_query_batch(batch):
        reader = unicodecsv.DictReader(result, encoding='utf-8')
        for row in reader:
            print(row) # dictionary rows

Note that while CSV is the default for historical reasons, JSON should
be prefered since CSV has some drawbacks including its handling of NULL
vs empty string.

PK Chunk Header
^^^^^^^^^^^^^^^

If you are querying a large number of records you probably want to turn on `PK Chunking
<https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/async_api_headers_enable_pk_chunking.htm>`_:

``bulk.create_query_job(object_name, contentType='CSV', pk_chunking=True)``

That will use the default setting for chunk size. You can use a different chunk size by providing a
number of records per chunk:

``bulk.create_query_job(object_name, contentType='CSV', pk_chunking=100000)``

Additionally if you want to do something more sophisticated you can provide a header value:

``bulk.create_query_job(object_name, contentType='CSV', pk_chunking='chunkSize=50000; startRow=00130000000xEftMGH')``

Bulk Insert, Update, Delete
---------------------------

All Bulk upload operations work the same. You set the operation when you
create the job. Then you submit one or more documents that specify
records with columns to insert/update/delete. When deleting you should
only submit the Id for each record.

For efficiency you should use the ``post_batch`` method to post each
batch of data. (Note that a batch can have a maximum 10,000 records and
be 1GB in size.) You pass a generator or iterator into this function and
it will stream data via POST to Salesforce. For help sending CSV
formatted data you can use the salesforce\_bulk.CsvDictsAdapter class.
It takes an iterator returning dictionaries and returns an iterator
which produces CSV data.

Full example:

.. code-block:: python

    from salesforce_bulk import CsvDictsAdapter

    job = bulk.create_insert_job("Account", contentType='CSV')
    accounts = [dict(Name="Account%d" % idx) for idx in xrange(5)]
    csv_iter = CsvDictsAdapter(iter(accounts))
    batch = bulk.post_batch(job, csv_iter)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    print("Done. Accounts uploaded.")

Concurrency mode
^^^^^^^^^^^^^^^^

When creating the job, pass ``concurrency='Serial'`` or
``concurrency='Parallel'`` to set the concurrency mode for the job.
