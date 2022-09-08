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

#%%
#analysis commands

#search for cinical trial exp
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:isType]-(c:PublicationType) where a.Name="Claus G Roehrborn" and (c.Name="Multicenter Study" or c.Name="Clinical Trial" or c.Name="Randomized Controlled Trial") return b'
#look for b, then measure result list len instead

#multicenter only
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:isType]-(c:PublicationType) where a.Name="Claus G Roehrborn" and (c.Name="Multicenter Study") return b'

#randomized controlled
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:isType]-(c:PublicationType) where a.Name="Claus G Roehrborn" and (c.Name="Randomized Controlled Trial") return b'

#search for review articles
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:isType]-(c:PublicationType) where a.Name="Claus G Roehrborn" and (c.Name="Review") return b'

#list all descriptors
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Describes]-(c:Descriptor) where a.Name="Henry Hyunshik Woo" return b as Publication ,r2,c as Descriptor'

#journal history
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:PublishedIn]-(c:Journal) where a.Name="Henry Hyunshik Woo" return b as Publication,r2,c as Journal'


#co-authors
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Authored]-(c:Author) where a.Name="Henry Hyunshik Woo" return b as Publication,r2,c as Author'

#pull publication count
match (a:Author)-[r1:Authored]-(b:Publication) where a.Name="Henry Hyunshik Woo" return count(b)


#pull page rank
match(a:Author)-[r2:Authored]-(c:Publication) where a.Name='"+authorName+'"return avg(c.PageRank) as Average, max(c.PageRank) as MaxVal


#pull most cited authors
target='Henry Hyunshik Woo'
cypher='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Cited]-(c:Publication)-[r3:Authored]-(d:Author) where a.Name="'+target+'" return distinct c,r3 as rel,d as Author'  

#pull most cited authors for each topic
# use id's from first query in second
match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Cited]-(c:Publication)-[r3:Describes]-(d:Descriptor) where a.Name="Henry Hyunshik Woo" and d.Name="Laser Therapy" and d.Qualifier="methods" return distinct c.PMID as PMID
match (a:Author)-[r:Authored]-(b:Publication) where b.PMID in ["23179309","30516393","29227749","28544292","27889960","23837622","22575913","22507245","25311768","16413337","20605316","16359214","17622237","21105987","22153927","27053186","30113481","27203515","28229211","26066832","28951116","29102672","29621785","31617419","31498146","24242891","23347100","23615757","18681811","23992242","21497852","29806840","27677474","28293871","26210343","28793314","31015537","28942591","30220043","28405763","29046983","25369778","28303843","27502738","30039386","28480345","29275506","29730838","29858700","29558093","26983892","25300823","29368232","27766387","28698991","31076849","31164686","31838943","33181656","31942465","30403609","25542629","25605646","26291564","26077354","26254171","27500451","27476130","27981854","28735017","28830227","28401579","29373285","29329895","29186090","29205507","29538164","29430969","30222994","29633359","29907496","29754809","30116964","31062613","30950888","31810402","31588793","31389161","31790785","32633020","16713070","21923418","17869409","21658839","17566639","23879477","17499427","20800340","18308094","23746047","21944122","25833318","30489151","24859776","18837655","16704894","19022557","15801365","15006063","15205738","20806388","16903819","30587221","22033173","19361906","26216644","26299915","24097277","19698021","23352299","19912196","28586247","25647174","27585786","29090340","30950890","17382757","29332492","21459416","18613777","22943082","25307850","25343625","24972732","24929643","28044238","27048160","23913094","22940718","26732107","24913423","24930949","22732644","25040293","25164484","24936718","25290572","25556024","26323662","26307430","26970202","28576667","28576422","29570929","16481099","16126327","21168876","21550638","19958155","21711132","8886063","15091045","16686717","20964486","21045703","21183365","22018158","21779921","22460737","23223651","24754237","25378050","27841666","25905430","26142599","31256112","22050499","31880953","32020749","21481134","16094022","16359215","25101536","23828495","25007827","25652616","18785895","24570773","26970929","27905699","28443721","27320474","26732101","18025849","19394500","10954311","32210252","10799170","15592063","24521152","26712715","28156134","25068651","25745792","26832449","30039715","18597826","17689002","16135396"] return a.Name as Name




