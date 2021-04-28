import sys
sys.path.append('./populartimes/')
import populartimes
import requests
import json
import pandas as pd
import time
import os
import csv

### Dejar prendida variable single_coord para obtener los lugares cercanos de una ubicacion ###
single_coord = False
### Dejar prendida la variable popular_times para obtener informacion sobre trafico de personas en el lugar durante el dia ###
porpular_times = False

### Nombre del archivo y carpeta de salida ###
file = "monterrey" # De preferencia el nombre del lugar
carpeta = "Negocios" # Carpeta donde se guardaran todos los json obtenidos.

### Si no existe el nombre de la carpeta, se crea. ###
if not os.path.exists(carpeta):
    os.mkdir(carpeta)

# Requiere
radius = 2000 # Metros
loc = (25.7435305555556, -100.4189) ### Latitud y longitud del lugar a examinar. ###
# Optional
keyword = ''
rankby = ''


'''La busqueda sale mejor si se usan types ya que puede ignorar ciertos negocios aunque esten a lado
    Por default viene la opcion prominence
    
    This option sorts results based on their importance. 
    Ranking will favor prominent places within the specified area. 
    Prominence can be affected by a place's ranking in Google's index, global popularity, and other factors. 
    
    --Types
    https://developers.google.com/places/web-service/supported_types?hl=es-419
'''

### Tipos de locales que se quieren buscar con el populartimes.py ### 
types = ["bar", "church"]
#types = ["accounting", "airport", "amusement_park" ,"aquarium", "art_gallery", "atm", "bakery" ,"bank" ,"bar" ,"beauty_salon" ,"bicycle_store" ,"book_store" ,"bowling_alley" ,"bus_station" ,"cafe" ,"campground" ,"car_dealer" ,"car_rental" ,"car_repair" ,"car_wash" ,"casino" ,"cemetery" ,"church" ,"city_hall" ,"clothing_store" ,"convenience_store" ,"courthouse" ,"dentist" ,"department_store" ,"doctor" ,"drugstore" ,"electrician" ,"electronics_store" ,"embassy" ,"fire_station" ,"florist" ,"funeral_home" ,"furniture_store" ,"gas_station" ,"grocery_or_supermarket" ,"gym" ,"hair_care" ,"hardware_store" ,"hindu_temple" ,"home_goods_store" ,"hospital" ,"insurance_agency" ,"jewelry_store","laundry" ,"lawyer" ,"library" ,"light_rail_station" ,"liquor_store" ,"local_government_office" ,"locksmith" ,"lodging" ,"meal_delivery" ,"meal_takeaway" ,"mosque" ,"movie_rental" ,"movie_theater" ,"moving_company" ,"museum" ,"night_club" ,"painter" ,"park" ,"parking" ,"pet_store" ,"pharmacy" ,"physiotherapist" ,"plumber" ,"pólice" ,"post_office" ,"primary_school" ,"real_estate_agency" ,"restaurant" ,"roofing_contractor" ,"rv_park" ,"school" ,"secondary_school" ,"shoe_store" ,"shopping_mall" ,"spa" ,"stadium" ,"storage" ,"store" ,"subway_station" ,"supermarket" ,"synagogue" ,"taxi_stand" ,"tourist_attraction" ,"train_station" ,"transit_station" ,"travel_agency" ,"university" ,"veterinary_care" ,"zoo"] # Todos los negocios

s = pd.DataFrame() # Inicializar el dataframe
i = 0 # Manejo de la primera fila

### APIKEY para correr el script. "REQUERIDO"
# APIKEY Datlas
APIKEY = ''


def findPlaces(loc=loc, radius=radius, pagetoken = None, file="data", type="", rankby="", keyword=""):
    global i
    global s

    lat, lng = loc
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&type={type}&keyword={keyword}&rankby={rankby}&key={APIKEY}{pagetoken}".format(lat = lat, lng = lng, radius = radius, type = type, keyword = keyword, rankby = rankby, APIKEY = APIKEY, pagetoken = "&pagetoken=" + pagetoken if pagetoken else "")
    print(url)
    response = requests.get(url)
    res = json.loads(response.text)
   
    for result in res["results"]:
        info = "    ".join(map(str,[result["place_id"], result["name"],result["vicinity"],result["geometry"]["location"]["lat"],result["geometry"]["location"]["lng"], result["types"],result.get("rating",0)]))
        titles = ["id", "Nombre","Direccion","Latitud","Longitud","Tipo","Rating"]
        data = info.split("    ")
        zipObj = zip(titles, data)
        data = dict(zipObj)
        if(i == 0):
            s = pd.DataFrame(data=data, index=[0])
            i = 1
        else:
            ### Debido a que tenemos un append, el ultimo archivo creado, tiene todos los datos de los demas csv ###
            s = s.append(data, ignore_index=[True])
        
        s.to_csv('./'+carpeta+'/'+file+type+".csv")
        print(len(s))
        
    pagetoken = res.get("next_page_token",None)
    return pagetoken


pagetoken = None


### Si single_coord esta prendido se hace una llamada al api de google places y trae info del lugar ###
if (single_coord):
    for t in types:
        pagetoken = findPlaces(loc=loc, radius=radius, pagetoken=pagetoken, file=file, type=t, rankby=rankby, keyword=keyword)
        time.sleep(2)

        if not pagetoken:
            pass
    
### OBTIENE LOS POPULARTIMES DEL ID SELECCIONADO ###
### Debido a los archivos creados con el single_cood, solo necesitamos leer el ultimo archivo, ya que tiene todos los datos ###
if (porpular_times):
    places = pd.read_csv('./'+carpeta+'/'+file+types[len(types)-1]+'.csv')
    places.drop_duplicates(subset="id", keep = 'first', inplace = True) 
    for i, place in places.iterrows():
        res = populartimes.get_id(APIKEY, place['id'])
        print(res)
        app_json = json.dumps(res, sort_keys=True)
        parsed = json.loads(app_json)
        with open('./'+carpeta+'/'+file+str(i)+'.json', 'w') as json_file:
            json.dump(res, json_file)




### CONVIERTE TODOS LOS JSON DEl DIRECTORIO PRINCIPAL A UN SOLO CSV ###

carpeta = "./"+carpeta+"/"
columns = ['id','name','address','types','lat','long','rating','rating_n','time_spent','populartimes.name','populartimes.data','populartimes.name.1','populartimes.data.1','populartimes.name.2','populartimes.data.2','populartimes.name.3','populartimes.data.3','populartimes.name.4','populartimes.data.4','populartimes.name.5','populartimes.data.5','populartimes.name.6','populartimes.data.6','time_wait.name','time_wait.data','time_wait.name.1','time_wait.data.1','time_wait.name.2','time_wait.data.2','time_wait.name.3','time_wait.data.3','time_wait.name.4','time_wait.data.4','time_wait.name.5','time_wait.data.5','time_wait.name.6','time_wait.data.6', 'time', 'file']

all_files = os.listdir("./"+carpeta)
txt_files = filter(lambda x:x.endswith(".json"), all_files)
df = pd.DataFrame(columns=columns)

### Opcion 1 ###
"""
def create_list_from_json(jsonfile):

    with open(jsonfile) as f:
        place = json.load(f)

    row = []  # create an empty list

    # append the items to the list in the same order.
    row.append(place["id"])
    row.append(place["name"])
    try:
        row.append(place["address"])
    except:
        row.append(place["NA"])
    row.append(place["types"])
    row.append(place["coordinates"]["lat"])
    row.append(place["coordinates"]["lng"])
    try:
        row.append(place["rating"])
    except:
        row.append('NA')
    try:
        row.append(place["rating_n"])
    except:
        row.append('NA')
    try:
        row.append(place["international_phone_number"])
    except:
        row.append('NA')
    try:
        row.append(place["current_popularity"])
    except:
        row.append('NA')

    try:
        for time in place["populartimes"]:
            try:
                row.append(time["name"])
            except:
                row.append('NA')
            try:
                for hour in time["data"]:
                    try:
                        row.append(hour)
                    except:
                        row.append('NA')
            except:
                row.append('NA')
    except:
        for i in range(175):
            row.append('NA')
        
    try:
        row.append(place["time_spent"])
    except:
        row.append('NA')
    return row


with open('popular_times.csv', 'a') as c:
    writer = csv.writer(c)
    writer.writerow(["ID", "name", "address", "types", "lat", "lng", "rating", "rating_n", "international_phone_number", "current_popularity",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "populartimes.name","00:00","01:00","02:00","03:00","04:00","05:00","06:00","07:00","08:00","09:00","10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00","20:00","21:00","22:00","23:00",
    "time_spend"])
    for file in txt_files:
        row = create_list_from_json(f'{carpeta}/{file}')  # create the row to be added to csv for each file (json-file)
        writer.writerow(row)
c.close()

"""
### Opcion 2 ###

def create_list_from_json(jsonfile):

    with open(jsonfile) as f:
        place = json.load(f)

    row = []  # create an empty list


    print(place)

    # append the items to the list in the same order.
    row.append(place["id"])
    row.append(place["name"])
    try:
        row.append(place["address"])
    except:
        row.append(place["NA"])
    row.append(place["types"])
    row.append(place["coordinates"]["lat"])
    row.append(place["coordinates"]["lng"])
    try:
        row.append(place["rating"])
    except:
        row.append('NA')
    try:
        row.append(place["rating_n"])
    except:
        row.append('NA')
    try:
        row.append(place["international_phone_number"])
    except:
        row.append('NA')
    try:
        row.append(place["current_popularity"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][0]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][1]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][2]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][3]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][4]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][5]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["populartimes"][6]["data"])
    except:
        row.append('NA')
    try:
        row.append(place["time_spent"])
    except:
        row.append('NA')
    return row


with open('popular_times.csv', 'a') as c:
    writer = csv.writer(c)
    writer.writerow(["ID", "name", "address", "types", "lat", "lng", "rating", "rating_n", "international_phone_number", "current_popularity",
    "monday","tuesday","wednesday","thursday","friday","saturday","sunday", "time_spend"])
    for file in txt_files:
        row = create_list_from_json(f'{carpeta}/{file}')  # create the row to be added to csv for each file (json-file)
        writer.writerow(row)
c.close()


print("Listo!")
