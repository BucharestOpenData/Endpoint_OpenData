# Intro
- Repository hosting the downlink application for the provider pipe
# Description
HTTPS endpoint with the following routes: "/", "/psg". It works with MariaDB to upload positions and vehicle status.

## Arguments

The HTTPS endpoint accepts the following arguments:
- `--port`:Default 8080 over HTTPS.
- `-dp`:Database Connection Port Default 3306.
- `-ip`:Database Connection IP.
-  `-u`: MariaDB user  `db_user`.
- `-pw`: MariDB password `db_password`.
- `-t`: Update rate for collector threads(request frequency for links) `update_rate_in_sec`.
- `--th_user`: Th Alert username `th_user`.
- `--th_password`:Th Alert password `th_password`.
- `--th_psg_user`:Th Passenger API username `th_psg_user`.
- `--th_psg_password`: Th Passenger API password `th_psg_password`.
- `--debug False`: Debug Swith accepts text: True/False  `debug`.

## Available Endpoint
- `\`:  Root  Endpoint(Full Data)
- `\psg`:  Passenger Enabled Vehicles Data Only  Endpoint
## Response Structure 
The response structure has several fields.When fields are not populated,they will still exist ,but will be filled with `null` value.
### Description
#### Outer Data
- `id`:message id of the sample
- `timestamp`:timestamp in ISO format of the message update.Used to date the information in the message(does not apply to position and passenger data)
- `src`:Update Source 
    * 0 :` rd API`
    * 1 : `TH ALERT`
    * 2 : `TH_PSG_API`
    * 3 : `rd_AVL`
    * 4 : `internal_SRV(rd processed data)`
#### Vehicle(inner)
- `id`:Vehicle ID(differs from Th to rd) meaning that a bus/tram will have `two` different id's , one from THb and one from rd. The given vehicle id is the rd one `for now` 
- `license_plate`:Vehicle license plate
#### Passenger Info
- `on_board`:Number of passengers in the vehicle 
- `timestamp`:ISO format timestamp for the passenger data only
- `in`:number of passengers that entered the vehicle
- `out`:number of passengers that existed the vehicle
- [ ] :warning: **Warning** Data is valid only for last stop.`in` and `out` are reset after stop/during stop.Be sure to correlate with position and stop location to ensure validity.
#### Trip Data
- `trip_id`:unique trip identifier.Correlate with `GTFS Schedule` data
- `route_id`:int type the route the vehicle is assigned to  
- `direction_id`:int type.To define direction correlate with `GTFS Schedule`
- `start_time`:Start time for trip                               
### Response Snippet
```
[
    {
        "id": 11616,
        "timestamp": "2024-07-15T08:47:49.640544+00:00",
        "src": 4,
        "vehicle": {
            "trip": {
                "trip_id": "None",
                "route_id": 113,
                "direction_id": 1,
                "start_time": null
            },
            "vehicle": {
                "id": 1616,
                "license_plate": "B-400-STB"
            },
            "position": {
                "latitude": 44.489078521728516,
                "longitude": 26.08683967590332,
                "timestamp": "2024-07-15T08:47:49.640544+00:00"
            },
            "passenger_info": {
                "on_board": 0,
                "timestamp": "2024-07-15T08:47:38+00:00",
                "in": 336,
                "out": 300
            }
        }
    },
    {
        "id": 11627,
        "timestamp": "2024-07-15T08:47:49.640578+00:00",
        "src": 4,
        "vehicle": {
            "trip": {
                "trip_id": "None",
                "route_id": 232,
                "direction_id": 1,
                "start_time": null
            },
            "vehicle": {
                "id": 1627,
                "license_plate": "B-419-STB"
            },
            "position": {
                "latitude": 44.46638107299805,
                "longitude": 26.15648078918457,
                "timestamp": "2024-07-15T08:47:49.640578+00:00"
            },
            "passenger_info": {
                "on_board": 1,
                "timestamp": "2024-07-15T08:47:38+00:00",
                "in": 487,
                "out": 461
            }
        }
    }
  ]
```

## Description
This is a multi-threaded python web-server application intended to query,collect and update a database with real-time vehicle data.
The database tables for positions are cycled automatically every month with naming convention:"Vehicle_Positions_M_YYYY" ex:`Vehicle_Positions_7_2024`
The application `creates` the table if it does not exists.
Vehicle data is uploaded into `Vehicles` table.
The `THb_id` ,for some reason,can be received as text sometimes "PxVy" where x and y are numbers,in the cases that strings are encountered,the app_id is derived by applying the Python `hash` function.

Since the application is multi-threaded and a thread is dispatched / request-client the response times are generally under 200ms.However delays might happen if the load is above 1000 concurrent connections.


### Database Field Descriptions
####  `Vehicles` Table
- `vehicle_db_id`: `auto_increment` `primary key` type: `int`
    * Unique Database ID
- `vehicle_rd_app_id: `unique` type:`int` can be `null`
    * rd ID
- `vehicle_THb_app_id`: `unique` type:`int`
    * THb ID
- `vehicle_license_plate_nr`: `unqiue` type:`varchar(16)`
    * License Plate
- `route_db_id`: type:`int` 
    * [ ] :warning: **Field might be dropped** Do not use for now
    * Used to assign a vehicle to a route.
####`Vehicle_Positions_M_YYYY` Table
- `vehicle_db_id`:type:`int` `foreign key` 
    * foreign key from `Vehicles` table
- `position_db_id`:`unique` `primary key` type:`int`
    * primary key for positions.Identifies uniquely a record
- `position_long`: type:`float` 
    * GPS longitude
- `position_lat`:type:`float`
    * GPS latitude
- `mariadb_point`:type `WKB Point`
    * MariaDB point entry for DataBase computations
- `time_stamp_location_info`:type:`timestamp(4)`
    * Location aquisition timestamp
- `aquisition_mode`:type:`varchar(16)`
    * data source 
   
