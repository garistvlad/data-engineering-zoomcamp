/*
1.
docker build --help -> --iidfile string          Write the image ID to the file
*/

/*
2.
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

*/


/*
3.
How many taxi trips were totally made on January 15?
Tip: started and finished on 2019-01-15.
Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.
*/

-- 20530
select count(*) as trip_cnt
from green_trip_data
where true
    and lpep_pickup_datetime::date = '2019-01-15'::date
    and lpep_dropoff_datetime::date = '2019-01-15'::date
;


/*
4.
Which was the day with the largest trip distance?
Use the pick up time for your calculations.
*/

-- 2019-01-15
select lpep_pickup_datetime::date as d_date,
       max(trip_distance) as max_tip_distance
from green_trip_data
group by d_date
order by max_tip_distance desc
limit 1
;


/*
5.
In 2019-01-01 how many trips had 2 and 3 passengers?
*/

-- 2,1282 | 3,254
select passenger_count,
       count(*) as trip_cnt
from green_trip_data
where true
    and lpep_pickup_datetime::date = '2019-01-01'::date
    and passenger_count in (2,3)
group by 1
order by 1
;


/*
6.
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
Note: it's not a typo, it's tip , not trip
*/

-- Long Island City/Queens Plaza
select
    gtd.pulocationid,
    pu_tz.zone as pu_zone,
    gtd.dolocationid,
    do_tz.zone as do_zone,
    gtd.tip_amount
from green_trip_data gtd
join taxi_zone do_tz
    on gtd.dolocationid = do_tz.locationid
join taxi_zone pu_tz
    on gtd.pulocationid = pu_tz.locationid
where true
    and pu_tz.zone = 'Astoria'
order by tip_amount desc
limit 1


/*
7. Terraform GCP apply:
*/

/*
Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.dataset will be created
  + resource "google_bigquery_dataset" "dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "trips_data_all"
      + delete_contents_on_destroy = false
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + labels                     = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "europe-west6"
      + project                    = "hip-plexus-374912"
      + self_link                  = (known after apply)

      + access {
          + domain         = (known after apply)
          + group_by_email = (known after apply)
          + role           = (known after apply)
          + special_group  = (known after apply)
          + user_by_email  = (known after apply)

          + dataset {
              + target_types = (known after apply)

              + dataset {
                  + dataset_id = (known after apply)
                  + project_id = (known after apply)
                }
            }

          + routine {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + routine_id = (known after apply)
            }

          + view {
              + dataset_id = (known after apply)
              + project_id = (known after apply)
              + table_id   = (known after apply)
            }
        }
    }

  # google_storage_bucket.data-lake-bucket will be created
  + resource "google_storage_bucket" "data-lake-bucket" {
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "EUROPE-WEST6"
      + name                        = "de_zoomcamp_data_lake_hip-plexus-374912"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + uniform_bucket_level_access = true
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "Delete"
            }

          + condition {
              + age                   = 30
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }

      + versioning {
          + enabled = true
        }

      + website {
          + main_page_suffix = (known after apply)
          + not_found_page   = (known after apply)
        }
    }

Plan: 2 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.dataset: Creating...
google_storage_bucket.data-lake-bucket: Creating...
google_bigquery_dataset.dataset: Creation complete after 1s [id=projects/hip-plexus-374912/datasets/trips_data_all]
google_storage_bucket.data-lake-bucket: Creation complete after 1s [id=de_zoomcamp_data_lake_hip-plexus-374912]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
*/
