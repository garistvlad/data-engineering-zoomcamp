## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

**Answer**

```shell
cd ./prefect-src
python ./flows/etl_web_to_gcs.py --taxi_color="green" --year=2020 --month=1

...
15:18:51.913 | INFO    | Flow run 'lurking-ferret' - Created task run 'pandas_to_parquet-0' for task 'pandas_to_parquet'
15:18:51.913 | INFO    | Flow run 'lurking-ferret' - Executing 'pandas_to_parquet-0' immediately...
15:18:52.649 | INFO    | Task run 'pandas_to_parquet-0' - There are 447,770 rows saved to green_taxi_trips_2020_01.parquet
15:18:52.662 | INFO    | Task run 'pandas_to_parquet-0' - Finished in state Completed()
...
```


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

**Answer**

- initialize deployment:
```shell
prefect deployment build ./flows/etl_web_to_gcs.py:etl_web_to_gcs -n monthly 
```

- modify `etl_web_to_gcs-deployment.yaml` by setting new values for `parameters` and `schedule`:
```
parameters: {"taxi_color": "green", "year": 2020, "month": 1}
schedule:
  cron: 0 5 1 * *
```

- apply changes:
```shell
prefect deployment apply etl_web_to_gcs-deployment.yaml

...
Successfully loaded 'monthly'
Deployment 'etl-web-to-gcs/monthly' successfully created with id 'dcb30e5a-a5db-4282-8ba8-538e40ac35e7'.
...
```

- To run the deployment:
```shell
prefect deployment run 'etl-web-to-gcs/monthly'
prefect agent start -q 'default'
```

- Check that everything was set correctly on the `Deployment` tab of prefet orion UI:
```
Schedule:
At 05:00 AM on day 1 of the month
```


## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483


**Answer**

- initialize deployment:
```shell
prefect deployment build ./flows/etl_gcs_to_bq.py:etl_gcs_to_bq -n adhoc
```

- modify `parameters`:
```
parameters: {
  "taxi_color": "yellow",
  "years": [2019],
  "months": [2,3]
}
```

- apply deployment:
```shell
prefect deployment apply ./etl_gcs_to_bq-deployment.yaml
```

- Run deployment
```
...
20:04:07.523 | INFO    | Task run 'pandas_to_bigquery-0' - Saved 7,019,375 rows to BigQuery table `taxi_trips.yellow_taxi_trips`
...
20:06:13.850 | INFO    | Task run 'pandas_to_bigquery-1' - Saved 7,832,545 rows to BigQuery table `taxi_trips.yellow_taxi_trips`
...
```

- Check data was inserted into BigQuery:
```sql
select
    count(*) as rows_cnt
from taxi_trips.yellow_taxi_trips

-- Output: 14.851.920
```


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225


**Answer**

- Create GitHub block via `Prefect Orion UI`
- Create deployment using previously specified GitHub block. You could find deployment source code [here](./prefect-src/deployments/github_deployment_web_to_gcs.py)
```shell
python ./deployments/github_deployment_web_to_gcs.py
```
- Run an agent:
```shell
prefect agent start -q 'github'
```

- Specify parameters needed
```json
{
  "taxi_color": "green",
  "year": 2020,
  "month": 11
}
```

- Run deployment and check log
```
...
00:12:58.863 | INFO    | Task run 'web_to_pandas-0' - Loaded 88,605 rows to pandas DataFrame.
...
00:12:59.331 | INFO    | Task run 'pandas_to_parquet-0' - There are 88,605 rows saved to green_taxi_trips_2020_11.parquet
...
00:13:01.170 | INFO    | Task run 'parquet_to_gcs_bucket-0' - Successfully loaded to GCS Bucket. Path: `taxi/green_taxi_trips_2020_11.parquet`
```

## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`

**Answer**

- Setup Slack notification via `Prefect Orion UI`
- Specify required flow parameters:
```json
{
  "taxi_color": "green",
  "year": 2019,
  "month": 4
}
```

- Run workflow and check the result:
```
...
00:27:11.753 | INFO    | Task run 'pandas_to_parquet-0' - There are 514,392 rows saved to green_taxi_trips_2019_04.parquet
...
```


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10

**Answer**

- Password example: `1234567890`
- Masked password in the UI: `********` | 8 symbols

