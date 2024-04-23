import requests
import uuid
import json
import os

def fetch_data_from_api(api_url, dmaps_api_token):
    headers = {
        "Authorization": f"Token {dmaps_api_token}"
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        result = response.json()
        return result

    else:
        print(response.status_code)
        print("Failed to fetch data")
        return None

def fetch_id_list(resource_id, uniqueKey):
    page =1
    all_data = []
    while True:
        params = {'resource_id':resource_id,'limit':100, 'offset':(page-1)*100}
        response = requests.get("https://dms.janakpurmun.gov.np//api/3/action/datastore_search", params)
        data = response.json().get("result",{}).get("records",[])
        if not data:
            break
        all_data.extend(data)
        page += 1
    id_list = {item[uniqueKey]:item["_id"] for item in all_data}
    return id_list
    
def extract_data(data, fields_to_extract, last_update_date):
    # toggle this line to test to data["message"] == "No new data to update" for test
    if data["message"] != "No new data to update":
        updated_records = [record for record in data["data"] if record["updated_date"] > last_update_date]
        if updated_records:
            extracted_records = []
            for record in updated_records:
                record_id = uuid.uuid4().int & (1<<64)-1
                extracted_record = {
                    "_id": record_id ,
                    **{field: record.get(field, "") for field in fields_to_extract}
                    }
                extracted_records.append(extracted_record)
            if "house_no" in fields_to_extract:
                dateTimeOf = ["buildings",max(record["updated_date"] for record in updated_records)]
            else:
                dateTimeOf = ["roads",max(record["updated_date"] for record in updated_records)]
            return extracted_records, dateTimeOf
        else:
            print("No records updated since the last update date")
            return False, False
    else:
        print("No new data to update")
        return False, False

        
    

def update_data(extracted_records, resource_id, updateDateTime, uniqueKey, apiToken):
    endpoint = "/api/3/action/datastore_upsert"
    url = f"https://dms.janakpurmun.gov.np/{endpoint}"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': apiToken
    }

    
    id_list = fetch_id_list(resource_id, uniqueKey )
    final_extracted_records = update_extracted_data_id(extracted_records, id_list, uniqueKey)
    data = {
        "resource_id": resource_id,
        "records": final_extracted_records,
        "method": "upsert",
        "force": True
    }
    
         
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        save_last_updated_time(updateDateTime[1],updateDateTime[0])
        print("Data updated successfully")
    else:
        print("Failed to update data")
        print(response.status_code)

def save_last_updated_time(updated_date, roadOrBuilding):
    filename = "last_update_time.json"
    try:
        if os.path.exists(filename):
            with open(filename, "r") as file:
                data = json.load(file)
        else:
            data = {}

        data["last_update_time_"+roadOrBuilding] = str(updated_date)
        with open(filename, "w") as file:
            json.dump(data, file)
    except FileNotFoundError:
        print('error')
    finally:
        file.close()
        

def load_last_update_time(roadOrBuilding):
    try:
        with open("last_update_time.json", "r") as file:
            data = json.load(file)
            if roadOrBuilding == "buildings":
                return data["last_update_time_buildings"]
            elif roadOrBuilding == "roads":
                return data["last_update_time_roads"]
    except FileNotFoundError:
        return "2023-03-15T00:00:00Z"
    finally:
        file.close()

def update_extracted_data_id(extracted_records, id_list, uniqueKey):
    for item in extracted_records:
        unique_id = item[uniqueKey]
        if str(unique_id) in id_list:
            item["_id"] = id_list[str(unique_id)]
    return extracted_records
    
  

if __name__ == "__main__":
    buildings_data_api = "https://dma-dev.naxa.com.np/api/v1/core/buildings-data/"
    roads_data_api = "https://dma-dev.naxa.com.np/api/v1/core/roads-data/"
    building_resource_id = "1142db92-8b9a-41ce-8db1-69c97f19cccc"
    road_resource_id = "e366492b-623b-4c29-98e9-236c4629f373"
    dmaps_api_token = ""
    ckan_api_token =""
    
    buildings_fields = [
        "id","house_no", "tole_name", "association_type", "ward_no", 
        "ward_no_informal", "associate_road_name", "road_width", 
        "plus_code", "updated_date", "building_sp_use", "lat_field", "long_field" 
    ]
    roads_fields = [
        "id","road_id", "road_name_en","road_type", "road_category", "bbox", "updated_date", "ward_no"
    ]
    
    
    fetched_buildings_data = fetch_data_from_api(buildings_data_api, dmaps_api_token)
    new_buildings_records, updateDateTime = extract_data(fetched_buildings_data, buildings_fields, last_update_date=load_last_update_time("roads"))
    if new_buildings_records:
        update_data(new_buildings_records, building_resource_id, updateDateTime, uniqueKey="id", apiToken=ckan_api_token)
    
    fetched_roads_data = fetch_data_from_api(roads_data_api, dmaps_api_token)
    new_roads_records, updateDateTime = extract_data(fetched_roads_data,roads_fields, last_update_date=load_last_update_time("buildings"))
    if new_roads_records:
        update_data(new_roads_records, road_resource_id, updateDateTime, uniqueKey="road_id",apiToken=ckan_api_token)
    