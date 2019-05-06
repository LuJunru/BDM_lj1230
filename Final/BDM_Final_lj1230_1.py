from pyspark import SparkContext
import sys


def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(5070))
    index = rtree.Rtree()
    for idx, geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return zones.plctract10[idx], zones.plctrpop10[idx]
    return None

def processTweets(pid, records):
    import re
    import pyproj
    import shapely.geometry as geom
    
    pattern = re.compile("\w+")
    proj = pyproj.Proj(init="epsg:5070", preserve_units=True)
    index, zones = createIndex("500cities_tracts.geojson")
    drug_set = set(open('drug_illegal.txt', 'r').read().split("\n")) | set(open('drug_sched2.txt', 'r').read().split("\n"))
    drug_wor = {e for e in drug_set if " " not in e}
    drug_pha = {e for e in drug_set if " " in e}
    
    counts = {}
    for record in records:
        flag = 0
        row = record.strip().split("|")
        if len(set(row[-1].split(" ")) & drug_wor) > 0:  # First check words
            flag = 1
        else:  # if no words then check phrases
            words = pattern.findall(row[-2].lower())
            length = len(words)
            if length > 1:
                phrases = set()
                for i in range(2, min(9, length + 1)):  # Longest length of possible phrases is 8
                    for j in range(len(words) - i + 1):
                        phrases.add(" ".join(words[j:j + i]))
                if len(phrases & drug_pha) > 0:
                    flag = 1
        if flag == 1:
            p = geom.Point(proj(float(row[2]), float(row[1])))
            try:
                zone_id, zone_pop = findZone(p, index, zones)
            except:
                continue
            if zone_id and zone_pop > 0:
                counts[zone_id] = counts.get(zone_id, 0.0) + 1.0 / zone_pop
    return counts.items()

    
if __name__ == "__main__":
    sc = SparkContext()

    counts = sc.textFile(sys.argv[1]).mapPartitionsWithIndex(processTweets).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[0]).collect()
    
    f = open(sys.argv[2], "w")
    for count in counts:
        line = count[0] + "," + str(count[1])
        f.write(line + "\n")
    f.close()