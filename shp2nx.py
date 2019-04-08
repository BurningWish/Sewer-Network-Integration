import networkx as nx
import fiona
from shapely.geometry import LineString


def read_shp(edge_path, node_path):
    network = nx.Graph()

    with fiona.open(edge_path, 'r') as c:
        for record in c:
            old_coords = record['geometry']['coordinates']
            new_coords = []
            for coord in old_coords:
                x = round(coord[0], 3)
                y = round(coord[1], 3)
                new_coords.append((x, y))
            new_line = LineString(new_coords)
            startNode = new_coords[0]
            endNode = new_coords[-1]
            network.add_edge(startNode, endNode)
            network.edge[startNode][endNode]['wkt'] = new_line.wkt
            network.edge[startNode][endNode]['usNodeId'] = record['properties']['us_node_id']  # NOQA
            network.edge[startNode][endNode]['dsNodeId'] = record['properties']['ds_node_id']  # NOQA
            network.edge[startNode][endNode]['edgeType'] = record['properties']['link_type']  # NOQA
            network.edge[startNode][endNode]['gradient'] = record['properties']['gradient']  # NOQA
            network.edge[startNode][endNode]['capacity'] = record['properties']['capacity']  # NOQA
            network.edge[startNode][endNode]['Length'] = new_line.length  # NOQA
            network.edge[startNode][endNode]['Coords'] = new_coords

    with fiona.open(node_path, 'r') as c:
        for record in c:
            point = record['geometry']['coordinates']
            node = (round(point[0], 3), round(point[1], 3))
            if not network.has_node(node):
                network.add_node(node)
            network.node[node]['nodeId'] = record['properties']['node_id']
            network.node[node]['nodeType'] = record['properties']['node_type']
            network.node[node]['Coords'] = [node[0], node[1]]

    return network
