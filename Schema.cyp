-- Loading csv's
-- First, we load all the information for the nodes

LOAD CSV WITH HEADERS FROM 'file:///authors_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (a:author {id: row.ID, name: row.name, email: row.email, department: row.department})

LOAD CSV WITH HEADERS FROM 'file:///conference_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (c:conference {id: row.ID, name: row.name, year: row.year, edition: row.edition})

LOAD CSV WITH HEADERS FROM 'file:///journal_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (j:journal {id: row.ID, name: row.name})

LOAD CSV WITH HEADERS FROM 'file:///keywords_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (k:keywords {id: row.ID, name: row.name, domain: row.domain})

LOAD CSV WITH HEADERS FROM 'file:///paper_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (p:paper {id: row.ID, title: row.title, abstract: row.abstract, pages: row.pages, doi: row.doi, link: row.link})

LOAD CSV WITH HEADERS FROM 'file:///proceeding_semantics.csv' AS row
FIELDTERMINATOR ','
CREATE (p:proceeding {id: row.ID, name: row.name, city: row.city})

-- Once the nodes are there, we can built the relationship between the nodes

LOAD CSV WITH HEADERS FROM 'file:///paper_cites_paper.csv' AS row
FIELDTERMINATOR ','
MATCH (p1:paper {id: row.START_ID})
MATCH (p2:paper {id: row.END_ID})
CREATE (p1)-[:CITES]->(p2);

-- NB: Everything until here is correctly uploaded.

LOAD CSV WITH HEADERS FROM 'file:///paper_published_in_journal.csv' AS row
FIELDTERMINATOR ','
MATCH (p:paper {id: row.START_ID})
MATCH (j:journal {id: row.END_ID})
CREATE (p)-[:PUBLISHED_IN {volume: row.volume, year: row.year}]->(j);

LOAD CSV WITH HEADERS FROM 'file:///paper_has_keywords.csv' AS row
FIELDTERMINATOR ','
MATCH (p:paper {id: row.START_ID})
MATCH (k:keywords {id: row.END_ID})
CREATE (p)-[:HAS]->(k);

LOAD CSV WITH HEADERS FROM 'file:///paper_presented_in_conference.csv' AS row
FIELDTERMINATOR ','
MATCH (p:paper {id: row.START_ID})
MATCH (c:conference {id: row.END_ID})
CREATE (p)-[:PRESENTED_IN]->(c);

LOAD CSV WITH HEADERS FROM 'file:///author_writes_papers.csv' AS row
FIELDTERMINATOR ','
MATCH (a:author {id: row.START_ID})
MATCH (p:paper {id: row.END_ID})
CREATE (a)-[:WRITES {corresponds: row.corresponding_author}]->(p);

LOAD CSV WITH HEADERS FROM 'file:///conference_part_of_proceeding.csv' AS row
FIELDTERMINATOR ','
MATCH (c:conference {id: row.START_ID})
MATCH (p:proceeding {id: row.END_ID})
CREATE (c)-[:PART_OF]->(p);

LOAD CSV WITH HEADERS FROM 'file:///author_review_papers.csv' AS row
FIELDTERMINATOR ','
MATCH (a:author {id: row.START_ID})
MATCH (p:paper {id: row.END_ID})
CREATE (a)-[:REVIEWS {comment: row.comment, acceptance: row.acceptanceProbability}]->(p);


