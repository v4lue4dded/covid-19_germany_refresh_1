import json
import os
os.chdir('K:/repos/covid_19_germany/')


with open('covid-19-germany-landkreise_simple.json') as json_file:
    data_raw = json.load(json_file)

data = data_raw


for i_o in range(len(data['objects']['covid-19-germany-landkreise']['geometries'])):
    print(i_o)
    properties_old = data['objects']['covid-19-germany-landkreise']['geometries'][i_o]['properties']

    properties_new = dict()
    properties_new['IDLandkreis'] = properties_old['rs']
    data['objects']['covid-19-germany-landkreise']['geometries'][i_o]['properties'] = properties_new

#     coordinates_old = data['features'][i_f]['geometry']['coordinates']
#     coordinates_new = list()
#     for i in range(len(coordinates_old)):
#         lat = coordinates_old[i][0][1]
#         lng = coordinates_old[i][0][0]
#         if ((lat >= 30) | (lng >= -15) | (lng >= 30)): # in europe
#             coordinates_new.append(coordinates_old[i])
#     data['features'][i_f]['geometry']['coordinates'] = coordinates_new


# (lng >= -15) | (lng >= 30)

# (lat >= 30)

with open('covid-19-germany-landkreise_simple_rp.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)