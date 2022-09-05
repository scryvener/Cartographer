# -*- coding: utf-8 -*-
"""
Created on Sat Feb 20 01:24:28 2021

@author: Kenneth
"""

#Create indexes for each, massive increase in speed 
#articles
#Create Index PubmedID for (p:Publication) on (p.PMID)

#create index for doi
#Create Index DOI for (p:Publication) on (p.DOI)


#authors
#Create Index AuthorName for (p:Author) on (p.Name)

#journals
#Create Index ISSN for (p:Journal) on (p.ISSN)

#descriptors index
#



#should change to authors rather then physicians, since not all who publich on pubmed are neccesarily physicians 


#Create articles
#:auto using periodic commit 5000 load csv with headers from 'file:///articles.csv' as line Create (:Publication{PMID:(line.PMID),Title:(line.Title),PublishDate:(line.PublishDate),Abstract:(line.Abstract)})

#create authors, 
#:auto using periodic commit 5000 load csv with headers from 'file:///authors.csv' as line Create (:Author{Name:(line.Author),Affiliation:(line.Affiliation)})

#create journals, guide for the rest 
#:auto using periodic commit 5000 load csv with headers from 'file:///journals.csv' as line Create (:Journal{ISSN:(line.JournalID), Name:(line.Journal)})

#create pubtypes
#:auto using periodic commit 5000 load csv with headers from 'file:///pubtypes.csv' as line Create (:PublicationType{Name:(line.PublicationType)})

#Create descriptors
#:auto using periodic commit 5000 load csv with headers from 'file:///descriptors_qualifiers.csv' as line Create (:Descriptor{idx:(line.Index),Name:(line.Descriptor),Qualifier:(line.Qualifier)})


#set additional properties
#:auto using periodic commit 5000 load csv with headers from 'file:///auth_pr.csv' as line match(author:Author {Name:line.Name}) set author.avgPageRank=line.Average_PageRank

#:auto using periodic commit 5000 load csv with headers from 'file:///auth_pr.csv' as line match(author:Author {Name:line.Name}) set author.maxPageRank=line.Max_PageRank

#:auto using periodic commit 5000 load csv with headers from 'file:///publication_doi.csv' as line match(publication:Publication {PMID:line.PMID}) set publication.DOI=line.DOI



#
#    
#author relationship  
#:auto using periodic commit 5000 load csv with headers from 'file:///author_article.csv' as line match(author:Author {Name:line.Author}) match (article:Publication{PMID:line.PMID}) create (author)-[:Authored]->(article);    

#citation   
#:auto using periodic commit 5000 load csv with headers from 'file:///citation_article.csv' as line match(origin:Publication {PMID:line.PMID_Origin}) match (citing:Publication{PMID:line.PMID_CitedIn}) create (citing)-[:Cited]->(origin);

#combined seems to have issues
#:auto using periodic commit 5000 load csv with headers from 'file:///Combined_Cite_PMID.csv' as line match(origin:Publication {PMID:line.PMID_Origin}) match (citing:Publication{PMID:line.PMID_CitedIn}) create (citing)-[:Cited]->(origin);

#:auto using periodic commit 5000 load csv with headers from 'file:///New_Cite_PMID.csv' as line match(origin:Publication {PMID:line.Reference}) match (citing:Publication{PMID:line.Base}) create (citing)-[:Cited]->(origin);


 
#journal publish relationship
#:auto using periodic commit 5000 load csv with headers from 'file:///journal_article.csv' as line match(journal:Journal {ISSN:line.JournalID}) match (article:Publication{PMID:line.PMID}) create (article)-[:PublishedIn]->(journal);

#publicationtype relationship
# :auto using periodic commit 5000 load csv with headers from 'file:///pubtype_article.csv' as line match(pubtype:PublicationType {Name:line.PublicationType}) match (article:Publication{PMID:line.PMID}) create (article)-[:isType]->(pubtype);

#Descriptor Relationship
#:auto using periodic commit 5000 load csv with headers from 'file:///descriptors_article.csv' as line match(descriptor:Descriptor {idx:line.index}) match (article:Publication{PMID:line.PMID}) create (descriptor)-[:Describes]->(article);

#Create graph for pagerank
call gds.graph.create('myGraph',['Publication'],{Cited:{type:'Cited'}})

#write properties
CALL gds.pageRank.write('myGraph',{writeProperty:"PageRank"})
YIELD nodePropertiesWritten, ranIterations

#CALL gds.pageRank.stream('myGraph')
#YIELD nodeId, score
#RETURN gds.util.asNode(nodeId).PMID AS name, score
#ORDER BY score DESCENDING, name ASC

#calculate average pagerank of the author. need to repeat in the author list 
#match (a:Publication)-[r:Authored]-(b:Author)-[r2:Authored]-(c:Publication) where a.PMID="28646935" and b.Name='Neal D Shore' return c.PageRank,c.Title,c.PMID

#current pagerank calculation is of the entire graph, would ideally like to calculate the pagerank individually if searching for specific subsets 
#the correlary to that is that we need way more data, to avoid the traps/orphans. 




#lets also look for similarity algos 

call gds.graph.create('myGraph',['Publication','Descriptor','Author'],{Cited:{type:'Cited'},Describes:{type:"Describes"},Authored:{type:"Author"}})

CALL gds.nodeSimilarity.stream('myGraph',{topK:10,similarityCutoff:.75,nodeLabels:['Publication']})
YIELD node1, node2, similarity
RETURN gds.util.asNode(node1).Title AS Person1, gds.util.asNode(node2).Title AS Person2, similarity
ORDER BY similarity DESCENDING, Person1, Person2
#%%
#general maitenance

#delte duplicate relationships
match ()-[r:Cited]->() 
match (s:Publication)-[r:Cited]->(e:Publication)
with s,e,type(r) as typ, tail(collect(r)) as coll 
foreach(x in coll | delete x)



