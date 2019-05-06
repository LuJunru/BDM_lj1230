from pyspark import SparkContext
import sys


def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx, geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)


def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None


def processTrips(pid, records):
    import csv
    import pyproj
    import geopandas as gpd
    import shapely.geometry as geom

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex("neighborhoods.geojson")
    area = gpd.read_file("neighborhoods.geojson")[["neighborhood", "borough"]].to_dict()

    if pid == 0:
        next(records)
    reader = csv.reader(records)

    counts = {}
    for row in reader:
        try:
            if len(row) == 18 and row[9] and row[10] and row[5] and row[6]:
                p1 = geom.Point(proj(float(row[9]), float(row[10])))  # dropoff, end in borough
                p2 = geom.Point(proj(float(row[5]), float(row[6])))  # pickup, end in neighbors
                zone1 = findZone(p1, index, zones)
                zone2 = findZone(p2, index, zones)
                if zone1 and zone2:
                    zone1 = area["borough"][zone1].lower()
                    zone2 = area["neighborhood"][zone2].lower()
                    if zone1 in counts:
                        counts[zone1][zone2] = counts[zone1].get(zone2, 0) + 1
                    else:
                        counts[zone1] = {zone2: 1}
        except:
            continue
    return counts.items()

    
if __name__ == "__main__":
    sc = SparkContext()

    yellow = sys.argv[1]
    count = sc.textFile(yellow).mapPartitionsWithIndex(processTrips).collect()
    
    res = {}
    for (key, values) in count:
        if key not in res:
            res[key] = values
        else:
            res[key].update(values)
    
    for key in res:
        for top in sorted(res[key].items(), key=lambda x: -x[1])[:3]:
            print(key, top[0], top[1])