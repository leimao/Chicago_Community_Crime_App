
import os
import re
import json
import csv

def extractPolygons(polygons_str):

    polygons = re.findall('(\([^\(]*[^\)]\))', polygons_str)
    polygons_parsed = list()
    for polygon in polygons:
        vertices_list = polygon.lstrip('(').rstrip(')').split(', ')
        vertices_list_parsed = list()
        for vertices in vertices_list:
            [x, y] = vertices.split(' ')
            x = float(x)
            y = float(y)
            vertices_list_parsed.append((x,y))
        polygons_parsed.append(vertices_list_parsed)
    return polygons_parsed

def readPolygons(filepath='./CommAreas.csv'):
    filename = os.path.basename(filepath)
    extension = os.path.splitext(filename)[1]
    assert extension == '.csv', 'Not CSV file as expected!'
    json_data = dict()
    with open(filepath, 'r') as csv_file:
        #csv_reader = csv.reader(csv_file, delimiter=',')
        csv_reader = csv.DictReader(csv_file, delimiter=',')
        header = next(csv_reader)
        line_counter = 0
        for row in csv_reader:
            print(line_counter)
            community_name = row['COMMUNITY']
            community_id = int(row['AREA_NUMBE'])
            polygons_str = row['the_geom']
            polygons = extractPolygons(polygons_str=polygons_str)
            json_data[community_id] = {'communityName': community_name, 'polygons': polygons}
            line_counter += 1

    with open('community_polygons.json', 'w') as fhand:
        json.dump(json_data, fhand)

def main():

    readPolygons();

if __name__ == '__main__':
    
    main()