# dms_etl_script
Etl script for simple csv update


`last_update_time.json` - file contains the timestamp of latest data in the dms csv, updates/uses it for time reference

`if data["message"] != "No new data to update":` line in etl.py - used to know if the data in datasource api is updated.

```   
dmaps_api_token = ""
ckan_api_token =""
```
required token for the ckan and datasource apis

```
    buildings_fields = [
        "id","house_no", "tole_name", "association_type", "ward_no", 
        "ward_no_informal", "associate_road_name", "road_width", 
        "plus_code", "updated_date", "building_sp_use", "lat_field", "long_field" 
    ]
    roads_fields = [
        "id","road_id", "road_name_en","road_type", "road_category", "bbox", "updated_date", "ward_no"
    ]
```
 These fields available in the dataset that needs update. 


