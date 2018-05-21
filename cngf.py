"""
This is an implementation of the CNGF algorithm proposed in the following
article:

Liyan Dong, Yongli Li, Han Yin, Huang Le, and Mao Rui,
"The Algorithm of Link Prediction on Social Network,"
Mathematical Problems in Engineering, vol. 2013,
Article ID 125123, 7 pages, 2013.
            https://doi.org/10.1155/2013/125123.
"""

from __future__ import division

__author__ = 'Nayan Jain', 'Shalini Aggarwal'

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col
from pyspark.sql import Row
from graphframes import *
import re
from math import log
import operator


def get_neighbours(vertex):
    """
    Given a node of the graph, this function finds all it's neighbours. Since
    it is designed for undirected graphs, the neighbours of the node are found
    in both directions.

    :param vertex: Any node of the global scope graph.
    :return: The list of neighbours of the graph.
    """
    neighbours1 = graph.edges.filter("src = '{}'".format(vertex)).select(
        "dst").distinct()
    neighbours2 = graph.edges.filter("dst = '{}'".format(vertex)).select(
        "src").distinct()
    neighbours = neighbours1.union(neighbours2)
    return neighbours.rdd.map(lambda row: row.dst).collect()


def get_subgraph(list_vertices):
    """
    Given a set of vertices, this function calculates a subgraph from the
    original graph.

    :param list_vertices: A set of vertices for which subgraph needs to be
    created.

    :return: The subgraph
    """

    # Find all the edges between all the nodes given in the list
    edge_motif = graph.find("(a)-[e]->(b)").filter(col("a.id").isin(
        list_vertices)).filter(col("b.id").isin(list_vertices))
    edge_select = edge_motif.select("e.src", "e.dst")

    # Create the subgraph from the new edges and return it
    return GraphFrame(graph.vertices, edge_select)


def get_guidance(subgraph_degree, original_degree):
    """
    Calculates the guidance of a node given it's original degree and subgraph
    degree.

    :param subgraph_degree: The degree of the node in the subgraph.
    :param original_degree: The degree of the node in the original graph.

    :return: The guidance of the node.
    """
    log_original_degree = log(original_degree)
    return subgraph_degree/log_original_degree


def calculate_similarity(subgraph_degrees):
    """
    Given a list of subgraph degrees, this function calls the guidance
    function and calculates the similarity of a particular node with all it's
    non-connected nodes.

    :param subgraph_degrees: A list of lists containing the non connected node
    and degrees of common neighbours from the subgraph.

    :return: A dictionary of similarity of each non-connected node
    """
    similarity_dict = []
    for nc_node in subgraph_degrees:
        similarity = 0
        for common_node in nc_node[1]:
            # Getting the degree of the common neighbour node from the original
            # graph
            original_degree = graph.degrees.filter("id = '{}'".format(
                common_node.id)).select("degree").collect()

            # Getting the degree of the common neighbour node from the subgraph
            sub_degree = common_node.degree

            # Calling the function to calculate guidance for the common
            # neighbour node
            guidance = get_guidance(sub_degree, original_degree[0].degree)

            # Adding the guidance to the similarity of the non-connected node
            similarity += guidance

        similarity_dict.append((nc_node[0], similarity))
    return similarity_dict


def node_processing():
    """
    Takes the graph object from global scope and processes each node to find
    all non-connected nodes and then find the similarity using the cngf
    algorithm.

    :return: The similarity of each node with all it's non connected nodes.
    """

    # Get the list of all nodes of the graph
    graph_similarity = []
    vertices_list = [i.id for i in graph.vertices.collect()]

    for root_node in vertices_list:
        print "Vertex " + str(root_node)

        # Get the neighbours of the node
        root_neighbours = set(get_neighbours(root_node))

        # Get the set of non-connected nodes by removing the node and the
        # neighbours of the node from the list of nodes.
        not_connected_nodes = set(vertices_list).difference(
            set(root_neighbours)).difference({root_node})

        subgraph_degrees = []

        for nc_node in not_connected_nodes:

            # Get neighbour of the non-connected node
            node_neighbours = set(get_neighbours(nc_node))

            # Get the common neighbours by taking the intersion of neighbours
            # of both the nodes.
            common_neighbours = root_neighbours.intersection(node_neighbours)

            if common_neighbours:
                # Create a set of all the vertices for which the subgraph needs
                # to be created, i.e., the common neighbours, the root node and
                # the non-connected node.
                subgraph_vertices = common_neighbours.union({nc_node},
                                                            {root_node})

                # Call the function to create the subgraph
                subgraph = get_subgraph(subgraph_vertices)

                # Find the degrees of the common neighbours from the subgraph
                common_neighbours_degrees = subgraph.degrees.filter(
                    col("id").isin(common_neighbours)).collect()
                subgraph_degrees.append((nc_node, common_neighbours_degrees))

        # Call the function to calculate the similarity of each non-connected
        # node with the current node.
        similarity = sorted(calculate_similarity(subgraph_degrees),
                            key=operator.itemgetter(1), reverse=True)
        graph_similarity.append((root_node, similarity))
        print similarity
    return graph_similarity


def create_graph(file_path, separator):
    """
    Reads the special character separated file to create data frame for edges
    and vertices. These data frames are then in turn used to create a graph for
    the given file.

    :param file_path: The path where the file is located in the system.
    :param separator: The column separator used in the file.

    :return: The graph for the given file.
    """

    # Get all the rows from the file
    edges = spark.read.text(file_path).rdd.map(lambda r: r[0])
    split_edges = edges.map(lambda line: re.split(separator, line))
    edges_rdd = split_edges. \
        map(lambda p: Row(src=str(p[0].strip()), dst=str(p[1].strip())))

    # Create data frame of edges
    e_df = spark.createDataFrame(edges_rdd)
    e_df.cache()

    src = split_edges.map(lambda p: (str(p[0].strip()),))
    dst = split_edges.map(lambda p: (str(p[1].strip()),))
    vertex_rdd = src.union(dst)

    # Create data frame of vertices
    v_df = spark.createDataFrame(vertex_rdd, schema=["id"]).dropDuplicates(
        ["id"])
    v_df.cache()

    # Create the graph for the above calculated edges and vertices
    print "Graph created"
    return GraphFrame(v_df, e_df)


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(
        description="This is a basic implementation of the cngf algorithm "
                    "proposed in the following paper: "
                    "https://www.hindawi.com/journals/mpe/2013/125123/ \n\n"
                    "It is built in spark using the graphframes library and "
                    "pyspark. \n\n\n"
                    "Run it using spark-submit in the following way: \n\n"
                    "$SPARK_HOME/bin/spark-submit "
                    "--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 "
                    "main.py file_path separator",
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument("path", help="Path of the file containing the data.")
    parser.add_argument("separator", help="The separator for the file")
    args = parser.parse_args()

    # Build spark session locally
    spark = SparkSession.builder.master("local[16]").appName(
        "Link Prediction").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Call the function to create a graph for the given file
    graph = create_graph(args.path, args.separator)

    # Call the function to calculate similarity between all non-connected
    # nodes in the graph
    node_processing()

    # Stop spark session
    spark.stop()
