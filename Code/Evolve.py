from neo4j import GraphDatabase

URI = "neo4j://localhost"
AUTH = ("neo4j", "12345678")

driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()


def load_node_institution_semantic(session):
    session.run("""LOAD CSV WITH HEADERS FROM 'file:///authors_semantics.csv' AS line
        MERGE(o:Organization {
            name: row.institution
        });""")

    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///authors_semantics.csv' AS row
            CREATE (a:Author {name:row.name}) 
            WITH a, row
            MERGE (o:Organization {name: row.institution})
            WITH a, o, row
            CREATE (a) - [r:AFFILIATED_TO] -> (i)
            SET r.department = row.department;"""
    )

def load_relation_author_reviews_paper(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///author_review_papers.csv' AS row
            MATCH (author:Author {ID: toString(toInteger(row.START_ID))}) - [r:reviews] -> (paper:Paper {ID: row.END_ID})
            SET r.comment = row.comment, r.acceptanceProbability = toFloat(row.acceptanceProbability);
        """
    )

def query_accept_paper_publication(session):
    session.run(
        """
        MATCH (a:Author) - [r:reviews] -> (p:Paper)
        WITH a, r, p,
        CASE
            WHEN SUM(r.acceptanceProbability) > 0.5 
            THEN True ELSE False 
        END AS decision
        SET r.decision = decision;
        """
    )

session = create_session()

session.execute_write(load_node_institution_semantic)
session.execute_write(load_relation_author_reviews_paper)
session.execute_write(query_accept_paper_publication)


session.close()

