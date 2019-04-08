"""
===============================================================
Read gas_pipes, buildings, terraces from PostGIS and get lists
===============================================================
"""

import networkx as nx
import psycopg2
from shapely.wkt import loads

dbname = "00_NCL_SEWER_TEMP"


class Pipe:
    def __init__(self, id, geom):
        self.id = id
        self.geom = geom


class Road:
    def __init__(self, id, geom):
        self.id = id  # id of this road object
        self.geom = geom  # road geometry: shapely linestring object


class Building:
    def __init__(self, id, geom, toid):
        self.id = id  # id of this building object
        self.geom = geom  # building geometry: shapely polygon object
        self.centroid = geom.centroid
        self.connectType = "indirect"
        self.toid = toid


class Substation:
    def __init__(self, id, geom):
        self.id = id  # id of this substation object
        self.geom = geom  # substation geometry: shapely point object
        self.neighborIdList = []
        self.buildingIdList = []
        self.directBids = []  # this stores the building who direct connects sub  # NOQA


class Terrace:
    def __init__(self, id, geom):
        self.id = id
        self.geom = geom
        self.centroid = geom.centroid
        self.buildingIdList = []
        self.projectType = 'normal'


class Union:
    def __init__(self, id, geom):
        self.id = id
        self.geom = geom
        self.centroid = geom.centroid
        self.buildingIdList = []

"""
===============================================================================
           Use the data above to construct a networkx instance
===============================================================================
"""


def readNet():
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT ST_AsText(geom) from roads")
    results = cur.fetchall()
    cur.close()
    conn.close()

    roadNet = nx.Graph()

    for line in results:
        coordsData = line[0][16: -1]
        wkt = 'LINESTRING ' + coordsData
        lineString = loads(wkt)
        coords = list(lineString.coords)
        startNode = (round(coords[0][0], 3), round(coords[0][1], 3))
        endNode = (round(coords[-1][0], 3), round(coords[-1][1], 3))
        attributes = {}
        attributes['Coords'] = coords
        attributes['Length'] = lineString.length
        roadNet.add_edge(startNode, endNode, attributes)
        roadNet.node[startNode]['Coords'] = coords[0]
        roadNet.node[endNode]['Coords'] = coords[-1]

    return roadNet


def readRoad():
    roadList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT rid, ST_AsText(geom) from roads order by rid asc")
    results = cur.fetchall()
    cur.close()
    conn.close()

    for line in results:
        coordsData = line[1][16: -1]
        wkt = 'LINESTRING ' + coordsData
        lineString = loads(wkt)
        roadList.append(Road(line[0], lineString))

    return roadList


def readPipe():
    pipeList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT pid, ST_AsText(geom) from sewer_pipes order by pid asc")  # NOQA
    results = cur.fetchall()
    cur.close()
    conn.close()

    for line in results:
        coordsData = line[1][16: -1]
        wkt = 'LINESTRING ' + coordsData
        lineString = loads(wkt)
        pipeList.append(Road(line[0], lineString))

    return pipeList


def readBuilding():
    buildingList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT bid, ST_AsText(geom), toid from buildings order by bid asc")  # NOQA
    results = cur.fetchall()
    cur.close()
    conn.close()
    for line in results:
        coordsData = line[1][13: -1]
        toid = line[2]
        wkt = 'POLYGON ' + coordsData
        polygon = loads(wkt)
        buildingList.append(Building(line[0], polygon, toid))

    return buildingList


def readTerrace():
    terraceList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT tid, ST_AsText(geom) from terraces order by tid asc")
    results = cur.fetchall()
    cur.close()
    conn.close()
    for line in results:
        wkt = line[1]
        polygon = loads(wkt)
        terraceList.append(Terrace(line[0], polygon))

    return terraceList


def readUnion():
    unionList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT uid, ST_AsText(geom) from unions order by uid asc")
    results = cur.fetchall()
    cur.close()
    conn.close()
    for line in results:
        wkt = line[1]
        multiPolygon = loads(wkt)
        unionList.append(Union(line[0], multiPolygon))

    return unionList


def readSubstation():
    substationList = []
    conn = psycopg2.connect("dbname = %s password = 19891202 user = postgres" % dbname)  # NOQA
    cur = conn.cursor()
    cur.execute("SELECT sid, ST_AsText(geom) from substations order by sid asc")  # NOQA
    results = cur.fetchall()
    cur.close()
    conn.close()

    for line in results:
        wkt = line[1]
        point = loads(wkt)
        substationList.append(Substation(line[0], point))

    return substationList
