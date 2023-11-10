# GCP_pipeline
Build End-to-End Data Pipelines

Use case:

An online food-selling company has huge transactional data on its orders. It is a daily batch file of the whole day's transactions.

- Customer_id: Who placed the order
- date: On which the order was placed, it will be the same for all the rows since it is a daily file.
- time: time in which the order was placed.
- order id
- items: items ordered per order, separated by a colon
- mode: In which the order was paid
- restaurant: where the order was placed
- status: if the order was delivered, not delivered, canceled, or on hold
- ratings: provided by the customer out of 5
- feedback: from the customer

So daily our client is going to provide us with this batch file which we have to ingest, process, and report. we need to generate daily and monthly reports. 

- Daily Report: The business chief should be able to see the percent split of the payment mode, and see the breakdown of ratings received by restaurants.
- Monthly report: Total ratings of the platform, and breakdown of feedback.

### GCP approach to case study.

- Approach:
    - Input data: As the client is providing us with a file, let's use cloud storage as our input layer
    - Processing data: for this layer, we will use cloud dataflow, but we can also use cloud dataproc if spark is more appealing.
    - Storage data: we will use big query
    - visualization: Cloud Data studio
    - scheduler: Cloud Composer.
