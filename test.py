__author__ = 'Shalini Aggarwal', 'Nayan Jain'

import unittest
from pyspark.sql import SparkSession
import cngf
from test_data import *


class PySparkTest(unittest.TestCase):

    def create_testing_pyspark_session(self):
        """Creates a global pyspark session"""
        return (SparkSession.builder.master('local[16]').appName(
            'my - local - testing - pyspark - context').getOrCreate())

    def test01_setUpClass(self):
        """Sets logging level and creates pyspark session"""
        cngf.spark = self.create_testing_pyspark_session()
        cngf.spark.sparkContext.setLogLevel("OFF")

    def test02_createGraph(self):
        """Creates a dataframe from test.txt file which has the demo data"""
        cngf.graph = cngf.create_graph('test.txt', ' ')
        self.assertEqual(str(cngf.graph.vertices.collect()), graph_vertices,
                         msg="Invalid vertices of main graph")
        self.assertEqual(str(cngf.graph.edges.collect()), graph_edges,
                         msg="Invalid edges of main graph")

    def test03_process_nodes(self):
        """Verifies that the similarity indices for each node is correct"""
        graph_similarity = cngf.node_processing()
        self.assertEqual(graph_similarity, similarity_dict, msg="Similarity not"
                                                                " equal")

    def test04_neighbours(self):
        """Finds and verifies get_neighbours function for the node A"""
        neighbours = cngf.get_neighbours('A')
        self.assertEqual(neighbours, [u'E', u'B'], msg="Unexpected neighbours")

    def test05_tearDownClass(self):
        """Stops the pyspark session"""
        cngf.spark.stop()


if __name__ == '__main__':
    unittest.main()
