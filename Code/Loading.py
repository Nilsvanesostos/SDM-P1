from neo4j import GraphDatabase

URI = "neo4j://localhost"
AUTH = ("neo4j", "12345678")

driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()

# First, we will load all the nodes

def loading_authors_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///authors_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (a:Author {id: row.ID, name: row.name, email: row.email});
        """)
        
def loading_conference_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///conference_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (c:Conference {id: row.ID, name: row.name, year: toInteger(row.year), edition: toInteger(row.edition)});
        """)    
def loading_journal_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///journal_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (j:Journal {id: row.ID, name: row.name});
        """)  

def loading_keywords_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///keywords_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (k:Keywords {id: row.ID, name: row.name, domain: row.domain});
        """)  

def loading_paper_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///paper_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (p:Paper {id: row.ID, title: row.title, abstract: row.abstract, pages: row.pages, doi: row.doi, link: row.link});
        """)  

def loading_proceeding_semantics(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///proceeding_semantics.csv' AS row
        FIELDTERMINATOR ','
        CREATE (p:Proceeding {id: row.ID, name: row.name, city: row.city});
        """)    

# Secondly, we load the edges

def loading_paper_cites_paper(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///paper_cites_paper.csv' AS row
        FIELDTERMINATOR ','
        MATCH (p1:Paper {id: row.START_ID})
        MATCH (p2:Paper {id: row.END_ID})
        CREATE (p1)-[:CITES]->(p2);
        """)  

def loading_paper_published_in_journal(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///paper_published_in_journal.csv' AS row
        FIELDTERMINATOR ','
        MATCH (p:Paper {id: row.START_ID})
        MATCH (j:Journal {id: row.END_ID})
        CREATE (p)-[:PUBLISHED_IN {volume: toInteger(row.volume), year: toInteger(row.year)}]->(j);
        """)  
    
def loading_paper_has_keywords(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///paper_has_keywords.csv' AS row
        FIELDTERMINATOR ','
        MATCH (p:Paper {id: row.START_ID})
        MATCH (k:Keywords {id: row.END_ID})
        CREATE (p)-[:HAS]->(k);
        """)  

def loading_paper_presented_in_conference(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///paper_presented_in_conference.csv' AS row
        FIELDTERMINATOR ','
        MATCH (p:Paper {id: row.START_ID})
        MATCH (c:Conference {id: row.END_ID})
        CREATE (p)-[:PRESENTED_IN]->(c);
        """)  

def loading_author_writes_papers(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///author_writes_papers.csv' AS row
        FIELDTERMINATOR ','
        MATCH (a:Author {id: row.START_ID})
        MATCH (p:Paper {id: row.END_ID})
        CREATE (a)-[:WRITES {corresponds: row.corresponding_author}]->(p);
        """)  

def loading_conference_part_of_proceeding(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///conference_part_of_proceeding.csv' AS row
        FIELDTERMINATOR ','
        MATCH (c:Conference {id: row.START_ID})
        MATCH (p:Proceeding {id: row.END_ID})
        CREATE (c)-[:PART_OF]->(p);
        """)  

def loading_author_review_papers(session):
    with driver.session() as session:
        session.run(""""
        LOAD CSV WITH HEADERS FROM 'file:///author_review_papers.csv' AS row
        FIELDTERMINATOR ','
        MATCH (a:Author {id: row.START_ID})
        MATCH (p:Paper {id: row.END_ID})
        CREATE (a)-[:REVIEWS]->(p);
        """)
        
# Execute all the functions
        
session = create_session()
        
session.execute_write(loading_authors_semantics)
session.execute_write(loading_conference_semantics)
session.execute_write(loading_journal_semantics)
session.execute_write(loading_keywords_semantics)
session.execute_write(loading_paper_semantics)
session.execute_write(loading_proceeding_semantics)
session.execute_write(loading_paper_cites_paper)
session.execute_write(loading_paper_published_in_journal)
session.execute_write(loading_paper_has_keywords)
session.execute_write(loading_paper_presented_in_conference)
session.execute_write(loading_author_writes_papers)
session.execute_write(loading_conference_part_of_proceeding)
session.execute_write(loading_author_review_papers)

session.close()