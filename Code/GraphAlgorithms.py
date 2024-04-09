# This file contains all the queries for applying algorithms for the part of Graph algorithms. The algortihms are: PageRank and Node Similarity.

from neo4j import GraphDatabase

URI = "neo4j://localhost"
AUTH = ("neo4j", "12345678")

driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()

# For the PageRank (https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/):

def pagerank(session):
    with driver.session() as session:
        session.run("""
        CALL gds.graph.project(
        'myGraph1',
        'paper',
        'CITES'
        );
        """)
        session.run("""
        CALL gds.pageRank.stream(
            'myGraph1'
        )
        YIELD
            nodeId,
            score
        RETURN 
            gds.util.asNode(nodeId).title AS title, 
            score
        ORDER BY 
            score DESC,
            title;
        """)

# For the Node Similarity (https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/):

def nodesimilarity(session):
    with driver.session() as session:
        session.run("""
        CALL gds.graph.project(
          'myGraph2',
          ['paper', 'keywords'],
          'HAS'
        );
        """)
        session.run("""
        CALL gds.nodeSimilarity.write.estimate('myGraph2', {
          writeRelationshipType: 'SIMILAR',
          writeProperty: 'score'
        });
        """)
        session.run("""
        CALL gds.nodeSimilarity.stream(
            'myGraph2'
        )
        YIELD
            node1,
            node2,
            similarity
        RETURN 
            gds.util.asNode(node1).title AS Paper1, 
            gds.util.asNode(node2).title AS Paper2,
            similarity
        ORDER BY 
            similarity DESC, 
            Paper1, 
            Paper2;
        """)

# Run the functions
        
session = create_session()
        
session.execute_write(pagerank)
session.execute_write(nodesimilarity)

session.close()