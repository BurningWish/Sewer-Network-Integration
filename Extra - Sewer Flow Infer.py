import shp2nx
import pickle
import copy
import fiona

G = shp2nx.read_shp('Input/NCL_Sewer_Pipes_Project.shp',
                          'Input/NCL_Sewer_Nodes_Project.shp')

ff = open("sink_nodes", "rb")
sink_nodes = pickle.load(ff)

# Delete edges from G
GCopy = copy.deepcopy(G)

current_sinks = sink_nodes

sum = 0
while (GCopy.number_of_edges() > 0):

    sum += 1
    print("iteration", sum)
    print("GCopy edges", GCopy.number_of_edges())
    print("number of current sinks", len(current_sinks))
    
    sinks_to_nodes = {}
    sinks_to_edges = {}
    
    for sink in current_sinks:
        # find immerdiate nodes connecting each sink
        sinks_to_nodes[sink] = GCopy.neighbors(sink)
        
        # find these edges conencting each sink
        sinks_to_edges[sink] = GCopy.edges(sink)

    # Now assign direction
    for sink in current_sinks:
        edges = sinks_to_edges[sink]
        for edge in edges:
            n0 = edge[0]
            n1 = edge[1]
            
            # check if this edge is connecting two current sinks
            if n0 in current_sinks and n1 in current_sinks:
                # use height
                height0 = G.node[n0]['height']
                height1 = G.node[n1]['height']
                if height0 > height1:
                    G.edge[n0][n1]['inferUsNodeId'] = G.node[n0]['nodeId']
                else:
                    G.edge[n0][n1]['inferUsNodeId'] = G.node[n1]['nodeId']
                    
            # otherwise the direction is always to the current sink
            else:
                if n0 == sink:
                    G.edge[n0][n1]['inferUsNodeId'] = G.node[n1]['nodeId']
                else:
                    G.edge[n0][n1]['inferUsNodeId'] = G.node[n0]['nodeId']

    # Remove edges that has been assigned flow from GCopy
    for sink in current_sinks:
        edges = sinks_to_edges[sink]
        for edge in edges:
            n0 = edge[0]
            n1 = edge[1]
            if GCopy.has_edge(n0, n1):
                GCopy.remove_edge(n0, n1)

    # Now see what nodes can become current sink in the next step
    temp_current_sinks = []
    for sink in current_sinks:
        nodes = sinks_to_nodes[sink]
        for n in nodes:
            # if that node still connects an edge in GCopy:
            if len(GCopy.edges(n)) > 0:
                if n not in temp_current_sinks:
                    temp_current_sinks.append(n)

    # do the last piece of work
    current_sinks = temp_current_sinks

"""
=====================================================
Write Data Back To Shapefile
=====================================================
"""
sourceDriver = 'ESRI Shapefile'
sourceCrs = {'y_0': -100000, 'units': 'm', 'lat_0': 49,
             'lon_0': -2, 'proj': 'tmerc', 'k': 0.9996012717,
             'no_defs': True, 'x_0': 400000, 'datum': 'OSGB36'}

result_folder = "Output//"

# write the network edges
sourceSchema = {'properties': {'Length': 'float:19.11', 
                               'UsNodeId': 'str', 
                               'DsNodeId': 'str', 
                               'InferUsNodeId': 'str'},  # NOQA
                'geometry': 'LineString'}

fileName = result_folder + 'Error_Edges' + '.shp'
with fiona.open(fileName,
                'w',
                driver=sourceDriver,
                crs=sourceCrs,
                schema=sourceSchema) as source:
    for edge in G.edges():
        startNode = edge[0]
        endNode = edge[1]
        record = {}
        thisEdge = G.edge[startNode][endNode]
        record['geometry'] = {'coordinates': thisEdge['Coords'], 'type': 'LineString'}  # NOQA
        record['properties'] = {'Length': thisEdge['Length'],
                                'UsNodeId': thisEdge['usNodeId'],
                                'DsNodeId': thisEdge['dsNodeId'],
                                'InferUsNodeId': thisEdge['inferUsNodeId']}  # NOQA
        source.write(record)
