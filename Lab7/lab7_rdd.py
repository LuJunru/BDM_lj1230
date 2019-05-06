from pyspark import SparkContent


def filterBike(records):
    for record in records:
        fields = record.split(",")
        if fields[6] == "Greenwich Ave & 8 Ave" and fields[3].startswith("2015-02-01"):
            yield (fields[3][:19], 1)


def filterTaxi(pid, lines):
    if pid == 0:
        next(lines)
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    station = proj(-74.00263761, 40.73901691)
    sq_radius = 1320 ** 2
    for line in lines:
        fields = line.split(",")
        try:
            dropoff = proj(fields[5], fields[4])
        except:
            continue
        sq_dist = (dropoff[0] - station[0]) ** 2 + (dropoff[1] - station[1]) ** 2
        if fields[1].startswith("2015-02-01") and sq_dist <= sq_radius:
            yield (fields[1][:19], 0)


def connextTrips(_, records):
    import datetime
    lastTaxiTime = None
    count = 0
    for dt, mode in records:
        # 用一个指针来标记最近的taxi时间
        t = datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        if mode == 1:
            if lastTaxiTime != None:
                diff = (t - lastTaxiTime).total_seconds()
                if 0 <= diff <= 600:
                    count += 1
        else:
            lastTaxiTime = t
    yield count


if __name__ == "__main__":
    sc = SparkContent()
    taxi = sc.textFile("yellow.csv.gz")
    bike = sc.textFile("citibike.csv")

    matchedBike = bike.mapPartitions(filterBike)
    matchedTaxi = taxi.mapPartitionsWithIndex(filterTaxi)
    alltrip = (matchedBike + matchedTaxi).sortByKey().cache()
    counts = alltrip.mapPartitionsWithIndex(connextTrips).reduce(lambda x, y: x + y)
    print(counts)
