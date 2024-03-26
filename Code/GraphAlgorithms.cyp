-- This file contains all the queries for applying algorithms for the part of Graph algorithms. The algortihms are: PageRank, Node Similarity and 
-- Dijkstra Single-Source Shortest Path.

-- For the PageRank (https://neo4j.com/docs/graph-data-science/current/algorithms/page-rank/):

CALL gds.graph.project(
  'myGraph1',
  'paper',
  'CITES'
);

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

-- For the Node Similarity (https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/):

CALL gds.graph.project(
  'myGraph2',
  ['paper', 'keywords'],
  'HAS'
);

CALL gds.nodeSimilarity.write.estimate('myGraph2', {
  writeRelationshipType: 'SIMILAR',
  writeProperty: 'score'
});

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
