# Libraries needed for requesting and working with the papers from Semantic Scholar 
import requests
import json

# Libraries needed for parsing and transforming the data
# from faker import Faker
# from faker.providers import address, internet, lorem, sbn
# from faker_education import SchoolProvider
from random import choice, randint, uniform
import pandas as pd
import numpy as np
import uuid

fake = Faker()
fake.add_provider(SchoolProvider)
fake.add_provider(internet)
fake.add_provider(sbn)
fake.add_provider(address)
fake.add_provider(lorem)

# List with universities 

university_names = [
    "Harvard University",
    "Massachusetts Institute of Technology (MIT)",
    "Stanford University",
    "California Institute of Technology (Caltech)",
    "University of Oxford",
    "University of Cambridge",
    "ETH Zurich - Swiss Federal Institute of Technology",
    "University of California, Berkeley (UC Berkeley)",
    "Princeton University",
    "Yale University",
    "Columbia University",
    "University of Chicago",
    "University of Tokyo",
    "Tsinghua University",
    "Peking University",
    "University of Toronto",
    "University of Michigan",
    "National University of Singapore (NUS)",
    "University of Pennsylvania",
    "Johns Hopkins University",
    "Cornell University",
    "Northwestern University",
    "Duke University",
    "University of California, Los Angeles (UCLA)",
    "University of London",
    "University of Edinburgh",
    "University of Manchester",
    "University of Sydney",
    "University of Melbourne",
    "University of British Columbia",
    "University of Washington",
    "University of Texas at Austin",
    "University of Wisconsin-Madison",
    "University of Illinois at Urbana-Champaign",
    "University of Amsterdam",
    "University of Copenhagen",
    "University of Helsinki",
    "University of Barcelona",
    "University of Rome La Sapienza"
]

fields_of_study = [
    "Machine Learning",
    "Data Mining",
    "Artificial Intelligence",
    "Statistics",
    "Natural Language Processing",
    "Big Data Analytics",
    "Data Visualization",
    "Computational Biology",
    "Business Intelligence",
    "Predictive Analytics"
]

# All the papers are requested from Semantic Scholar data base, see https://api.semanticscholar.org/api-docs/graph

API_KEY = "giaMz2jO1ma8zyfru6Tz6aT7CcQ8cVHo490oICoP"

def search_paper_id(query):

    url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "limit": 100
    }
    headers = {
        "x-api-key": API_KEY
    }
    
    response = requests.get(url, params=params, headers=headers)
    ids_json = response.json()
    lista = ids_json["data"]


    paper_ids = []

    for element in lista:
        paper_ids.append(element['paperId'])
    return paper_ids

def sorted_papers(paper_ids):

    response = requests.post(
    'https://api.semanticscholar.org/graph/v1/paper/batch',
    params={'fields': 'title,authors,abstract,fieldsOfStudy,journal,publicationVenue,year'},
    json={"ids": paper_ids})

    papers_json = response.json()

    dataset = {'paper_id': [], 'title': [], 'abstract': [], 'field': [], 'domain': [], 'journal_name': [], \
                   'author_id': [], 'author_name': [], 'corresponding_author': [], 'conference_id': [] , \
                    'conference_name': [], 'conference_year': [], 'proceeding_id': [], 'proceeding_name': []}
    df = pd.DataFrame(dataset)

    for paper in papers_json:
        # print(json.dumps(paper, indent=2))
        list_of_authors = paper["authors"]
        field_and_domain = paper["fieldsOfStudy"]
        if field is None or len(field) == 0:
            randomField = choice(fields_of_study)
            field = randomField
            domain = randomField
        else:
            field = field_and_domain[0]
            domain = field_and_domain[-1]
        journal = paper["journal"]
        venue = paper["publicationVenue"]
        paper_id = paper["paperId"]
        title = paper["title"]
        author_id = []
        author_name = []
        abstract = paper["abstract"]
        conference_id = str(uuid.uuid4())
        proceeding_id = str(uuid.uuid4())
        ### I still have to generate the data for proceeding
        if venue["type"] == "conference":
            conference_name = venue["name"]
            conference_year = paper["year"]
        else: 
            conference_name = "Unknown"
            conference_year = 2000

        # Assume there are only 3 authors
        for index, author in enumerate(list_of_authors[:3]): # We have to make changes here
            # Assume first author is corresponding author
            if index == 0:
                corresponding_author = True
            else:
                corresponding_author = False
            author_id = author["authorId"]
            author_name = author["name"]
        
            # We still have to add the append to the database
        
            df2 = {'paper_id': paper_id, 'title': title, 'abstract': abstract, 'field': field, 'domain': domain, 'journal_name': journal, \
                   'author_id': author_id, 'author_name': author_name, 'corresponding_author': corresponding_author, 'conference_id': conference_id , \
                    'conference_name': conference_name, 'conference_year': conference_year, 'proceeding_id': proceeding_id, 'proceeding_name': conference_name}
            df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
            
    return df

# Functions for parsing data into nodes

def author_node(data):
    dict = {"ID": [], "name": [], "email": [], "department": [], "institution": []}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe
    for _, row in data.iterrows():
        email=row["author_name"].lower().replace(" ", "") + "@gmail.com"
        department = fake.school_type()
        institution = university_names[randint(0, len(university_names)-1)]
        df2 = {"ID": row['author_id'], "name": row['author_name'], "email": email, "department": department, "institution": institution}
        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)


    # Dump to csv
    df2.to_csv('/data/authors_semantic.csv')

def paper_node(data):
    dict = {"ID": [], "title": [], "abstract": [], "pages": [], "doi": [], "link": []}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe 
    for _,row in data.iterrows():
        pages = f'{randint(15,100)}-{randint(101,150)}'
        doi = fake.isbn()
        link = f"https://doi.org/{fake_doi}"
        df2 = {"ID": row['paper_id'], "title": row['title'], "abstract": row['abstract'], "pages": pages, "doi": doi, "link": link}
        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/paper_semantic.csv')

def keywords_node(data):
    dict = {"ID": [], "name": [], "domain":[]}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe 
    for _,row in data.iterrows():
        id = str(uuid.uuid4())

        df2 = {"ID": id, "name": row['field'], "domain": row['domain']}
        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/keywords_semantic.csv')

def journal_node(data):
    dict = {"ID": [], "name": []}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe 
    for _,row in data.iterrows(): 
        id = str(uuid.uuid4())

        df2 = {"ID": id, "name": row['journal_name']}
        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/journal_semantic.csv')

def conference_node(data):
    dict =  {"ID": [], "name": [], "year": [], "edition": []}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe 
    for _,row in data.iterrows():
        if row["conference"] not in df:
            # Choose a random number for the edition of the conference
            edition = randint(0,100)
            df2 = {"ID": row["conference_id"], "name": row["conference_name"], "year": row["conference_year"], "edition": edition} 
            df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/conference_semantic.csv')

def proceeding():
    dict_node = {"ID": [], "name": [], "city": []}
    dict_rel = {"START_ID": [], "END_ID": []}
    df_node = pd.DataFrame(dict_node)
    df_rel = pd.DataFrame(dict_rel)

    conf = pd.read_csv('/data/conference_semantics.csv', header=0)


    # Parse data into a dataframe for the proceeding node and the conference_part_of_proceeding
    for _,row in conf.iterrows():
        df2_node = {"ID": row["proceeding_id"], "name": row["conference"], "city": fake.city}
        df2_rel = {"START_ID": row["conference_id"], "END_ID": row["proceeding_id"]}
        df_node = pd.concat([df_node, pd.DataFrame([df2_node])], ignore_index=True)
        df_rel = pd.concat([df_rel, pd.DataFrame([df2_rel])], ignore_index=True)
    
    # Dump to csv
    df2_node.to_csv('/data/proceeding_semantic.csv')
    df2_rel.to_csv('/data/conference_part_of_proceeding.csv')

# Functions for parsing data into the relationships
    
def has_keyword_rel(data):
    dict = {"START_ID": [], "END_ID": []}
    df = pd.DataFrame(dict)
    key = pd.read_csv('/data/keywords_semantics.csv',header=0)

    # Parse data into a dataframe
    for _,row in data.iterrow():
        df2 = {"START_ID": row['paper_id'], "END_ID": key.loc[key['domain'] == row["domain"], 'ID']}

        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/paper_has_keywords.csv')

def cites_rel(data):
    dict = {"START_ID": [], "END_ID": []}
    df = pd.DataFrame(dict)

    #Parse data into a dataframe
    for _,row in data.iterrow():
        # Assume the paper is cited by [0-25] papers
        for i in range(randint[0,25]):
            cited_id = choice(list(data["paper_id"]))
            # A paper cannot cite itself
            if cited_id != row['paper_id']:
                df2 = {"START_ID": row['paper_id'], "END_ID": cited_id}

                df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)

    # Dump to csv
    df2.to_csv('/data/paper_cites_paper.csv')

def published_in_rel(data):
    dict = {"START_ID": [], "END_ID": [], "volume": [], "year": []}
    df = pd.DataFrame(dict)
    jor = pd.read_csv('/data/journal_semantics.csv', header=0)

    # Parse data into a dataframe
    for _,row in data.iterrows():
        volume = randint(0,100)
        year = randint(2000,2023)
        df2 = {"START_ID": row['paper_id'], "END_ID": jor.loc[jor['name'] == row["journal_name"], 'ID'], "volume": [volume], "year": [year]}

        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv 
    df2.to_csv('/data/paper_published_in_journal.csv')

def writes_rel(data):
    dict = {"START_ID": [], "END_ID": [], "corresponding_author": []}
    df = pd.DataFrame(dict)

    # Parse data into a dataframe 
    for _,row in data.iterrow():
        df2 = {"START_ID": row["author_id"], "END_ID": row["paper_id"], "corresponding_author": row["corresponding_author"]}

        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)
    
    # Dump to csv
    df2.to_csv('/data/author_writes_paper.csv')

def reviews_rel(data):
    dict = {"START_ID": [], "END_ID": [], "comment": [], "acceptanceProbability": []}
    df = pd.DataFrame(dict)
    rev = pd.read_csv('/data/author_writes_papers.csv', header=0)

    # Parse data into a dataframe
    for _,row in data.iterrow():
        # Pick 3 reviewers per paper 
        for i in range(3):
            reviewer = choice(list(rev["START_ID"]))
            # A review cannot have written the paper
            if reviewer != row["author_id"]:
                comment = fake.paragraph(nb_sentences=2)
                probability = uniform(0,1) 
                df2 = {"START_ID": reviewer, "END_ID": row['paper_id'], "comment": comment, "acceptanceProbability": probability}

                df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)

    # Dump to csv
    df2.to_csv('/data/paper_reviews_paper.csv')


def presented_in_rel(data):
    dict = {"START_ID" : [], "END_ID": []}
    df = pd.DataFrame(dict)

    conf = pd.read_csv('/data/conference_semantics.csv', header=0)

    # Parse data into a dataframe
    for _,row in data.iterrow():
        conf_id = conf.loc[conf['name'] == row["conference"], 'ID']
        df2 = {"START_ID": row["paper_id"], "END_ID": conf_id}
        df = pd.concat([df, pd.DataFrame([df2])], ignore_index=True)

    # Dump to csv
    df2.to_csv('/data/paper_presented_in_conference.csv')


# Example usage
query = "data science"

print("Getting the papers from the API...\n ")
paper_ids = search_paper_id(query)

print("Sorting the papers for the preprocess...\n")
papers_sorted = sorted_papers(paper_ids)

#Execute all the functions
print("Preprocessing the papers...\n")
author_node(papers_sorted)
paper_node(papers_sorted)
journal_node(papers_sorted)
keywords_node(papers_sorted)
conference_node(papers_sorted)
proceeding()
writes_rel(papers_sorted)
cites_rel(papers_sorted)
reviews_rel(papers_sorted)
has_keyword_rel(papers_sorted)
published_in_rel(papers_sorted)
presented_in_rel(papers_sorted)

print("Done.\n")