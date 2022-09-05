# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 23:34:23 2019

@author: Kenneth
"""
  
    
#%%
#Begin Script for real
import requests
from xml.etree import ElementTree
import pandas as pd
from neo4j import GraphDatabase
import time
import math
import pickle

#%%

#two main issues: rare:names are mispelled leading to repeats-rare enough that it can be fixed manually

# more common-different formatting leading to variations in the name-first check formatting, if it matches, then continue
#second, look for repeats using shortest entry and contains, choose the one that matches closest to the desired format. 
#if no repeats, use as is. <-should reduce the amount enough that can then be manually removed. 
 

#%%

def PubmedAuthorPull(author,associatedArticleID):#Author format is Last NAme, space , then full and middle initials

    if associatedArticleID==None:#used for the very first pull

        path='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term='+str(author)+'[author]&retmax=10000'
    else:
        
        
        if len(author.split(' '))==2:
            first=author.split(' ')[0]
            last=author.split(' ')[1]
            
            path='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=10000'\
            '&term='+first+'%20'+last+'%5BAuthor%5D&cauthor=true&cauthor_uid='+str(associatedArticleID)
            
        elif len(author.split(' '))==3:
            first=author.split(' ')[0]
            middle=author.split(' ')[1]
            last=author.split(' ')[2]
            
            path='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&retmax=10000'\
            '&term='+first+'%20'+middle+'%20'+last+'%5BAuthor%5D&cauthor=true&cauthor_uid='+str(associatedArticleID)
        
#        firstMiddle=author.split(' ')[1]
#        last=author.split(' ')[0]
        
#        print(last,firstMiddle)

    s=requests.Session()
    
    response=s.get(path)

    tree=ElementTree.fromstring(response.content)
    articleList=[]
    articleTree=tree[3].findall('.//Id')

    for each in articleTree:
        articleList.append(each.text)
        
    return articleList

#articles=PubmedAuthorPull('Steven N Gange',28646935)#Manually seed input for the first one
    

#%%
    
term='benign prostatic hyperplasia'#must use as specific of a term as possible, else we get a lot of extraneous articles 
def PubmedTermPull(term):#Author format is Last NAme, space , then full and middle initials

    path='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term='+str(term)+'&retmax=10000'

    s=requests.Session()
    
    response=s.get(path)

    tree=ElementTree.fromstring(response.content)
    
    articleList=[]
    articleTree=tree[3].findall('.//Id')

    for each in articleTree:
        articleList.append(each.text)
    
    
    retcount=tree.find('.//Count').text
    
    remainder=int(retcount)-10000
    
    if remainder!=0:
        batchremain=math.ceil(remainder/10000)
        
        for i in range(batchremain):
            path='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term='+str(term)+'&retmax=10000&retstart='+str((i+1)*10000)
            
            s=requests.Session()
    
            response=s.get(path)
        
            tree=ElementTree.fromstring(response.content)
            
            articleTree=tree[3].findall('.//Id')

            for each in articleTree:
                articleList.append(each.text)
      
    return articleList 

articles=PubmedTermPull(term)
#%%
def Batching(batch_size,totalList):# Supporting function, breaks list into batches. returns list of lists to use.
    
    nbatches=round(len(totalList)/batch_size)
    
    batches=[]
    for i in range(nbatches):
        if i!=list(range(nbatches))[-1]:
            batches.append(totalList[i*batch_size:(i+1)*batch_size])
        else:
            batches.append(totalList[i*batch_size:])
            
    return batches

def extractLinks(citetree):
    
    linksets=citetree.findall('.//LinkSetDb')
    
    citelinkdb=[]
    for each in linksets:
        name=each.find('.//LinkName').text
        
#            print(name)
        if name=='pubmed_pubmed_citedin':
            citelinkdb=each
            break
        else:                
            continue
    
    
    if len(citelinkdb)==0:
        citedin=[]
    else:

        citelist=citelinkdb.findall('.//Link')
        
        citedin=[]
        
        for each in citelist:
            citedin.append(each.find('.//Id').text)
            
    return citedin

def extractforCite(articleDetail): 

    articlelist=[]
    for article in articleDetail:
        articlelist.extend(article['Citations'])
        articlelist.extend(article['References'])
    
    temp_df=pd.DataFrame(articlelist)
    
    temp_df=temp_df.drop_duplicates()
    
    articles_test=temp_df[0].to_list()
    
#    print(len(articles_test))
#    cleanlist=[]
#    clean_result
#    for each in articles_test:
#        if each not in existing:
#            cleanlist.append(each)
            
    return articles_test

def PubmedArticlePull(articleList,batch_size):#given a list of IDs, will return dict of ID and associated information to be added to the db

    batches=Batching(batch_size,articleList)
    articleDetailList=[]
    citelist=[]
    
    

    for count,batch in enumerate(batches):
        ignorelist=[]
        print('Working on Batch '+str(count)+'//'+str(len(batches)))
        
        basePath='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml'
        
#        for each in batch:
#            
#            basePath=basePath+each+','
            
        s=requests.Session()
        
        response=s.post(basePath,data={'id':batch})
        print(response)
        
        tree=ElementTree.fromstring(response.content)
        
        
        for i in range(len(tree)):
        
            #check and make sure this is an article
            
            if len(tree[i].findall('.//Article'))==0:
                ignorelist.append(batch[i])
                continue
            
            #Pull PMID
            articleID=tree[i].find('.//PMID').text
#            print(articleID)
            
            #Pull Article Title, Publish Date and Journal Details
            article=tree[i].findall('.//Article')[0]
            
            title=article.find('.//ArticleTitle').text
            
            journal=article.findall('.//Journal')[0]
            
            journalTitle=journal[2].text#.find('.//Title"')
            journalISSN=journal[0].text
            
            
            
            #deal with the date
            datedata=journal.findall('.//PubDate')
            
            datetaglist=[]
            for each in datedata[0]:
                datetaglist.append(each.tag)
                
            if 'Year' in datetaglist:
                
                publicationYear=''
                publicationMonth=''
                publicationDay=''
                
                
                for element in datedata[0]:
                    if element.tag=='Year':
                        publicationYear=element.text
                    
                    if element.tag=='Month':
                        publicationMonth=element.text
                        
                    if element.tag=='Day':
                        publicationDay=element.text
                        
                publishDate=publicationDay+' '+publicationMonth+' '+publicationYear
            else:
                #try alternative
                datedata=journal.findall('.//MedlineDate')
                
                publishDate=datedata[0].text
        
            #Pull All Authors
            authorlist=[]
            authorTree=tree[i].findall('.//Author')
            for author in authorTree:
                
                taglist=[]
                
                for child in list(author):
                    taglist.append(child.tag)
                
                if 'ForeName' not in taglist or 'LastName' not in taglist:
                    fname=''
                    lname=''
                else:
                    fname=author.find('.//ForeName').text
                    lname=author.find('.//LastName').text
                    
                    if fname=='' or lname=='' or fname==None or lname==None:
                        continue
                    else:
                        authorName=fname+' '+lname
                    
                    if 'Initials' in taglist:
                        initials=author.find('.//Initials').text
                    else:
                        initials=''

                    
                if 'AffiliationInfo' not in taglist:
                    authorAffil=''
                else:
                    authorAffil=author.find('.//Affiliation').text
                    
                authorlist.append({"ForeName":fname,"LastName":lname,"Name":authorName,"Affiliation":authorAffil,'Initials':initials})
        

            #pull abstract and compile into a single string block
                
            abstracttext=tree[i].findall('.//Abstract')   
            abstract_compiled=''
            
            if len(abstracttext)!=0:
                
                textlist=abstracttext[0].findall('.//AbstractText')
            
                for each in textlist:
                    
                    
                    text=''.join(each.itertext())
                    
                    
                    abstract_compiled=abstract_compiled+' '+text
                    abstract_compiled=abstract_compiled.lstrip()
                #end abstract
            
                abstract=abstract_compiled   
        
            #Pull Publication Types
            
            pubtypes=article.find('.//PublicationTypeList')
            
            pubtypeslist=[]
            
            for each in pubtypes:
                pubtypeslist.append(each.text)
                
            #Pull Descriptors and keywords
            #Descriptors should get their own label category in the graph database, rather then just be a label, similar to journals, callsed "Topics"?
            
            descriptionlist=[]
            descriptorslist=tree[i].findall('.//MeshHeading')
            
            for descriptor in descriptorslist:
                
                children=list(descriptor)
                
                qualifierlist=[]
                
                for child in children:
                    if child.tag=='DescriptorName':
                        name=child.text
                    if child.tag=='QualifierName':
                        qualifierlist.append(child.text)
                        
                descriptionlist.append({'Descriptor':name,'Qualifiers':qualifierlist})
                
            #pull keywords   
            keyword=tree[i].findall('.//Keyword')
            
            keylist=[]
            for each in keyword:
                keylist.append(each.text)
                
            #end keywords    
            
            
            
            #Pull list of articles this one cites
            reflist=tree[i].findall('.//Reference')
            ref_ID=[]
            for each in reflist:
                
                ref=each.findall('.//ArticleId[@IdType="pubmed"]')
                
                if len(ref)!=0:
                    
                    ref_ID.append(ref[0].text)
                
            item={"PMID":articleID,"Title":title,"Authors":authorlist,"Journal":journalTitle,"JournalID":journalISSN,"Publication_Date":publishDate,"Abstract":abstract,
                  "PublicationTypes":pubtypeslist,"Descriptors":descriptionlist,"KeywordList":keylist,"References":ref_ID}
            
            articleDetailList.append(item)
        
       
        cleanbatch=batch.copy()

        for each in ignorelist:

            cleanbatch.remove(each)
        

        basePath='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pubmed&cmd=neighbor_score'
        
        for each in cleanbatch:
            
            basePath=basePath+'&id='+each
        
        s2=requests.Session()
            
        response2=s2.get(basePath)
        
        citetree=ElementTree.fromstring(response2.content)    
        
        citedin=list(map(extractLinks,citetree.findall('.//LinkSet')))
        
        citelist.extend(citedin)
    
    for count,i in enumerate(articleDetailList):
        
        i['Citations']=citelist[count]
            
    return articleDetailList




#%%
detaillist=[]
a_detail=PubmedArticlePull(articles,200)

detaillist.extend(a_detail)
         
nextarticles=extractforCite(a_detail,articles)

a_detail=PubmedArticlePull(nextarticles,200)

detaillist.extend(a_detail)

nextarticles2=extractforCite(a_detail,articles+nextarticles)

pickle.dump(detaillist,open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','wb'))

#articleDetail=PubmedArticlePull(articles)
#Title details added to db under article
#Journals added into db and link established with article
#Authorlist pruned of duplicates and those already in db, then fed to the pubmed pull Author again. 
#%%Testing for above

#testlist=[]
#for each in temp:
#    testlist.append(each['PMID'])
#
#test_detail=PubmedArticlePull(testlist,150)

origlist=[]
for each in detaillist:
    if each['PMID'] in articles:
        origlist.append(each['PMID'])
        
        
        
start=time.time()
'200000' in a2
time.time()-start

#%%

s=requests.Session()
basePath='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml'

response=s.post(basePath,data={'id':testlist})
print(response)


tree=ElementTree.fromstring(response.content)

for i in range(len(tree)):
    articleID=tree[i].find('.//PMID').text

#    abstractbase=tree[i].findall('.//Abstract')

    abstracttext=tree[i].findall('.//Abstract')   
    abstract_compiled=''
    
    if len(abstracttext)!=0:
        
        textlist=abstracttext[0].findall('.//AbstractText')
    
        for each in textlist:
            
            
            text=''.join(each.itertext())
            
            
            abstract_compiled=abstract_compiled+' '+text
            abstract_compiled=abstract_compiled.lstrip()
        #end abstract
    
        abstract=abstract_compiled   


#%%testing functions


uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

articlelist=[]
for article in articleDetail2:
    articlelist.extend(article['Citations'])
    
temp_df=pd.DataFrame(articlelist)

temp_df=temp_df.drop_duplicates()


articles_test=temp_df[0].to_list()

cypherauth='['
for count,each in enumerate(articles_test):
#    cypherauth=cypherauth+'\''+str(each)+'\','
    
    if count==len(articles_test)-1:
        cypherauth=cypherauth+'\''+str(each)+'\''
    else:
        cypherauth=cypherauth+'\''+str(each)+'\','

cypherauth=cypherauth+']'

cypher='MATCH(a:Publication) WHERE a.PMID IN '+cypherauth+' return a'

with driver.session() as session:

    result=session.run(cypher)
    temp=result.data()
    
    
    if len(temp)!=0:
    
        list_exist=[]
        for each in temp:
            list_exist.append(each['a']['PMID'])
            
        for each in list_exist:
            
            if each in articles_test:
                articles_test.remove(each)
            else:
                continue
    
    
    
#%%
#new function-receive article details and add. all checking and creation under the main function
    
    
#Main Nodes
    
#Author-individual form Author List
#Completed
    
#Article-abstract, PMID, publication date, title
    
#Journal-title,ISSN ID
    
#Publication Types-categorical node
    
#Descriptors-categorical node with children qualifier nodes
    #if children exist, relation defined from the qualifier, if not, from the parent 
    

    
from neo4j import GraphDatabase
import pandas as pd

#looksGood
def CreateAuthors(authorlist,articleID):
    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    with driver.session() as session:
        
        createlist=[]
        
        for author in authorlist:
            #Check if author already exists
            cypher='MATCH(n:Physician) WHERE n.Name="'+author['Name']+'" return n'
            personCheck=session.run(cypher)
            response=personCheck.values()
            
            if len(response)==0:
                #if not, add author into createlist for db insertion
                createlist.append(author)
            else:
                continue
                 
        basequery='Create'
    
        finisher='"})'
        connector=','
        
        for count,author in enumerate(createlist):
            
            starter='(a'+str(count)+':Physician{Name:"'
            
            if count==0 and len(createlist)!=1:
                cypher=basequery+starter+author['Name']+finisher+connector
            elif count==0 and len(createlist)==1:
                cypher=basequery+starter+author['Name']+finisher
            
            elif count==len(createlist)-1:
                cypher=cypher+starter+author['Name']+finisher
            else:
                cypher=cypher+starter+author['Name']+finisher+connector
                
        session.run(cypher)
        
        #Create Relations with article
        
        
#        cypherauth='['
#        for count,each in enumerate(authorlist):
#            
#            if count==len(authorlist)-1:
#                cypherauth=cypherauth+'\''+each['Name']+'\''
#            else:
#                cypherauth=cypherauth+'\''+each['Name']+'\','
#            
#        cypherauth=cypherauth+']'
        
#        cypher='MATCH(a:Physician)-[r]-(b:Publication) WHERE b.PMID="'+articleID+'" AND a.Name IN '+cypherauth+' return a,b,r'
#        print(cypher)
#        result=session.run(cypher)
#        temp=result.data()
#        
#        list_exist=[]
#        for each in temp:
#            list_exist.append(each['a']['Name'])
#            
#        for each in authorlist:
#            if each['Name'] in list_exist:
#                authorlist.remove(each)

        for author in authorlist:
            
#            if len(response)==0:
                
            cypher='Match (a:Publication),(b:Physician) WHERE b.Name="'+author['Name']+'" AND a.PMID="'+articleID+'" Create (b)-[r:Author]->(a) return a.Name'
            session.run(cypher)

        session.close()
                
    return None   

#good

def CreatePubTypes(pubtypes,articleID):
    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    with driver.session() as session:
        pubcreate=[]
        
        for pubtype in pubtypes:
            cypher='MATCH(n:PublicationType) WHERE n.Name="'+pubtype+'" return n'
            dupeCheck=session.run(cypher)
            response=dupeCheck.values()
            
            if len(response)==0:
                pubcreate.append(pubtype)
            else:
                continue
            
        if len(pubcreate)!=0:
            basequery='Create'
            
            finisher='"})'
            connector=','
            
            for count,ID in enumerate(pubcreate):
                
                starter='(a'+str(count)+':PublicationType{Name:"'
                
                if count==0 and len(pubcreate)!=1:
                    cypher_pubtype=basequery+starter+ID+finisher+connector
                elif count==0 and len(pubcreate)==1:
                    cypher_pubtype=basequery+starter+ID+finisher
                
                elif count==len(pubcreate)-1:
                    cypher_pubtype=cypher_pubtype+starter+ID+finisher
                else:
                    cypher_pubtype=cypher_pubtype+starter+ID+finisher+connector
                    
            session.run(cypher_pubtype)
        
        for each in pubtypes:
            
            cypher='MATCH (a:Publication)-[r]-(b:PublicationType) WHERE b.Name="'+each+'" AND a.PMID="'+articleID+'" return r'
            dupeCheck=session.run(cypher)
            response=dupeCheck.values()
            
            if len(response)==0:

                cypher='Match (a:Publication),(b:PublicationType) WHERE b.Name="'+each+'" AND a.PMID="'+articleID+'" Create (a)-[r:IsType]->(b) return a.PMID'
                session.run(cypher)

        session.close()
        
    return None

#good
def CreateJournals(journal,journalID,articleID):
    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    with driver.session() as session:
        
        pubcreate=False
        
        cypher='MATCH(n:Journal) WHERE n.JournalID="'+journalID+'" return n'
        dupeCheck=session.run(cypher)
        response=dupeCheck.values()
        
        if len(response)==0:
            pubcreate=True
            
        if pubcreate==True:
            
            cypher='Create (a:Journal{JournalID:"'+journalID+'",Name:"'+journal+'"})'
            
            session.run(cypher)
            
            cypher_rel='Match (a:Publication),(b:Journal) WHERE b.JournalID="'+journalID+'" AND a.PMID="'+articleID+'" Create (a)-[r:PublishedIn]->(b) return a.Name'
            
            session.run(cypher_rel)
        else:
        
            cypher='MATCH(a:Publication)-[r]-(b:Journal) WHERE b.JournalID="'+journalID+'" AND a.PMID="'+articleID+'" return r'
            dupeCheck=session.run(cypher)
            response=dupeCheck.values()
            
            if len(response)==0:
                cypher_rel='Match (a:Publication),(b:Journal) WHERE b.JournalID="'+journalID+'" AND a.PMID="'+articleID+'" Create (a)-[r:PublishedIn]->(b) return a.Name'
                
                session.run(cypher_rel)
    
        session.close()
    
#still need to do this
#def CreateDescriptors(descriptors,articleID):
    
def CreateArticles(articleIDlist,datelist,titlelist):
    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    with driver.session() as session:
        
        articlecreatelist=[]
        
        for count,articleID in enumerate(articleIDlist):
            cypher='MATCH(n:Publication) WHERE n.PMID="'+articleID+'" return n'
            dupeCheck=session.run(cypher)
            response=dupeCheck.values()

            if len(response)==0:
                articlecreatelist.append([articleID,datelist[count],titlelist[count]])

        
        if len(articlecreatelist)!=0:
        
            for count,data in enumerate(articlecreatelist):
                
                #general cypher text
                basequery='Create'
                finisher='})'
                connector=','
                starter='(a'+str(count)+':Publication{'
    
                #create cypher of title
                title=data[2]
                
                if title==None:
                    title=''
                
                if '"' in title:
                    title=title.replace('"','\\"')
                cypher_name='Title:"'+title+'"'
                
                
                cypher_id='PMID:"'+data[0]+'"'
                cypher_date='PublishDate:"'+data[1]+'"'

                cypher_data=cypher_id+connector+cypher_date+connector+cypher_name
                
                
                if count==0 and len(articlecreatelist)!=1:
                    cypher=basequery+starter+cypher_data+finisher+connector
                elif count==0 and len(articlecreatelist)==1:
                    cypher=basequery+starter+cypher_data+finisher
                
                elif count==len(articlecreatelist)-1:
                    cypher=cypher+starter+cypher_data+finisher
                else:
                    cypher=cypher+starter+cypher_data+finisher+connector
                
            session.run(cypher)
            
        return None


#extractArticles-pull all articles from citations in the current articleDetail list for use as the next seed list       
def extractArticles(articleDetail):

    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    articlelist=[]
    for article in articleDetail:
        articlelist.append(article['PMID'])
        
#    print(articlelist)
    temp_df=pd.DataFrame(articlelist)
    
    temp_df=temp_df.drop_duplicates()
    
#    articles_cite=temp_df[0].to_list().copy()
    articles_test=temp_df[0].to_list()
    
    
    cypherauth='['
    for count,each in enumerate(articles_test):
    #    cypherauth=cypherauth+'\''+str(each)+'\','
        
        if count==len(articles_test)-1:
            cypherauth=cypherauth+'\''+str(each)+'\''
        else:
            cypherauth=cypherauth+'\''+str(each)+'\','
    
    cypherauth=cypherauth+']'
    
    cypher='MATCH(a:Publication) WHERE a.PMID IN '+cypherauth+' return a'
    
#    print(cypher)
    
    with driver.session() as session:
    
        result=session.run(cypher)
        temp=result.data()
        
        
        if len(temp)!=0:
        
            list_exist=[]
            for each in temp:
                list_exist.append(each['a']['PMID'])
                
            for each in list_exist:
                
                if each in articles_test:
                    articles_test.remove(each)
                else:
                    continue
        
        session.close()
                
                
    article_detail_clean=[]
    for each in articleDetail:
        if each['PMID'] in articles_test:
            article_detail_clean.append(each)
                
    return article_detail_clean
    

                
#prep function for parallel article create
def extractData(articleDetail):

    idlist=[]
    datelist=[]
    titlelist=[]
#    typelist=[]
    
#    start=time.time()
    for each in articleDetail:
        idlist.append(each['PMID'])
        datelist.append(each['Publication_Date'])
        titlelist.append(each['Title'])
#        typelist.append(each['PublicationTypes'])
        
    return idlist,datelist,titlelist







#Create Citations
def CreateCitations(articleID,citationlist):
    
    
    #assume all articles exist due to how we set up the script
    
    uri='bolt://localhost:7687'
    driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
    
    with driver.session() as session:
        
        for citation in citationlist:
            
            
            cypher='MATCH(a:Publication)-[r]-(b:Publication) WHERE b.PMID="'+citation+'" AND a.PMID="'+articleID+'" return r'
            dupeCheck=session.run(cypher)
            response=dupeCheck.values()
            
            if len(response)==0:
        
                cypher_rel='Match (a:Publication),(b:Publication) WHERE b.PMID="'+citation+'" AND a.PMID="'+articleID+'" Create (a)<-[r:Cited]-(b) return a.Name'
                session.run(cypher_rel)
        

     


#%%



authorlist=['Claus G Roehrborn','Jack Barkin','Steven N Gange','Neal D Shore','Jonathan L Giddens','Damien M Bolton','Barrett E Cowan','Anthony L Cantwell','Kevin T McVary',
            'Alexis E Te','Shahram S Gholami','William G Moseley','Peter T Chin','William T Dowling','Sheldon J Freedman','Peter F Incze','K Scott Coffield','Sean Herron',
            'Prem Rashid','Daniel B Rukstalis']

termlist=['benign prostatic hyperplasia']

def PullData(articles,degrees):#should probably batch this  too? to prevent losing all progress on an error 
    
    detaillist=[]
    
    nextarticles=articles
    
    for i in range(degrees):
#        print(i)
        a_detail=PubmedArticlePull(nextarticles,200)
        print(len(a_detail))
        detaillist.extend(a_detail)
         
        nextarticles=extractforCite(a_detail)

    time.sleep(5)

    return detaillist

start=time.time()
result_list=[]



#seed_article=PubmedTermPull(termlist[0])

#batches=Batching(1000,seed_article)#break this up so we can save more often, wasn't an issue before, but we have 1900 batches 

for term in termlist:
    
#    print(author)
#    seed_article=PubmedTermPull(term)

    author_result=PullData(articles,1)#implement try/except incase of issues doesn't tank the entire thing
              
    result_list.extend(author_result)
    
    pickle.dump(result_list,open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','wb'))
    time.sleep(15)



batches=Batching(10000,articles2)

for batch in batches:
    author_result=PullData(batch,1)
              
    result_list.extend(author_result)
    
    pickle.dump(result_list,open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','wb'))


 
    
#proposed structure-pull both descriptors and keywrods, combine to unified list 
#descriptors all have direct link. 
#if a descriptor has a qualifier, we create a qualifier node inbetween the connection 
#(descriptor)-[Description]->(qualifier)->[Description]->article
#repeat for each descriptor 
#otherwise just descriptor-[Description]->article

#we can then search using a multip hop 
#match (a:Sample)<-[:Describes*1..3]-(b:Descriptor) where b.Name='Cancer' return a
    
#abstract-just pull everything and condense into paragraph. inconsistent categorization will kill us .
    
    
#Citereferences- pull from ReferenceList under PubmedData
    
#modify the extract algo, have it pull both

#%%
result_descriptors_article=[]

for count,result in enumerate(result_list):
    
    if count%10000==0 and count!=0:
        print('Working on '+str(count))
        
    keywords=result['KeywordList']
    descriptors=result['Descriptors']
    articleID=result['PMID']
    
    if len(keywords)!=0:
        for each in keywords:
            
            if each==None or each=='.':
                continue
            else:
                item={'PMID':articleID,'Descriptor':str.strip(each),'Qualifier':''}
                item_df=pd.DataFrame(item,index=[0])
                result_descriptors_article.append(item_df)
    
    if len(descriptors)!=0:
        for i in descriptors:
            if len(i['Qualifiers'])==0:
                
                item={'PMID':articleID,'Descriptor':str.strip(i['Descriptor']),'Qualifier':''}
                item_df=pd.DataFrame(item,index=[0])
                result_descriptors_article.append(item_df)
            else:
                for qualifier in i['Qualifiers']:
                    item={'PMID':articleID,'Descriptor':str.strip(i['Descriptor']),'Qualifier':str.strip(qualifier)}
                    item_df=pd.DataFrame(item,index=[0])
                    result_descriptors_article.append(item_df)

descriptors_article_df=pd.concat(result_descriptors_article).drop_duplicates()




descriptors=descriptors_article_df[['Descriptor','Qualifier']].drop_duplicates()
descriptors.reset_index(drop=True,inplace=True)
descriptors.reset_index(drop=False,inplace=True)

pickle.dump(descriptors,open(r'E:\Cartographer\TermSeeded\descriptors_qualifiers.pkl','wb'))
descriptors.to_csv(r'E:\Cartographer\TermSeeded\descriptors_qualifiers.csv',index=True,index_label='Index')


descriptors_article_df_final=descriptors_article_df.merge(descriptors,on=['Descriptor','Qualifier'],how='inner')


pickle.dump(descriptors_article_df_final,open(r'E:\Cartographer\TermSeeded\descriptors_article_data.pkl','wb'))
descriptors_article_df_final.to_csv(r'E:\Cartographer\TermSeeded\descriptors_article.csv',index=False,index_label=False)


#%%

#%

result_list=pickle.load(open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','rb'))

start=time.time()

result_citation_article=[]
result_author_article=[]
result_journal=[]
result_pubtypes=[]
result_author_article=[]
        

for count,each in enumerate(result_list):
    
    if count%10000==0 and count!=0:
        print('Working on '+str(count))
    
    abstract=each['Abstract']
    articleID=each['PMID']
    authorlist=each['Authors']
    citations=each['Citations']
    references=each['References']
    title=each['Title']
    publishdate=each['Publication_Date']
    pubtypes=each['PublicationTypes']
    
    journal=each['Journal']
    journalID=each['JournalID']
    
    item_journal={'PMID':articleID,'Journal':journal,'JournalID':journalID}
    
    df_item_journal=pd.DataFrame(item_journal,index=[count])
    
#    journal_df1=journal_df1.append(df_item_journal)
    result_journal.append(df_item_journal)
    
    if len(authorlist)!=0:
        
        authors=pd.DataFrame(authorlist)[['Name','Affiliation']]
    
        item={'PMID':articleID,'Author':authors['Name'],'Affiliation':authors['Affiliation'],'Title':title,'PublishDate':publishdate,'Abstract':abstract}
        item_df=pd.DataFrame(item)
        
#        author_article_df1=author_article_df1.append(item_df)
        result_author_article.append(item_df)
        
#    if len(citations)!=0:
#            
#        item=pd.DataFrame(columns=['PMID_Origin','PMID_CitedIn'])
#        
#        item['PMID_CitedIn']=citations
#        
#        item['PMID_Origin']=articleID
#        
##        citation_article_df1=citation_article_df1.append(item)
#        result_citation_article.append(item)
#        
#    if len(references)!=0:
#        item=pd.DataFrame(columns=['PMID_Origin','PMID_CitedIn'])
#        
#        item['PMID_CitedIn']=articleID
#        item['PMID_Origin']=references
#        
#        result_citation_article.append(item)
#
#    if len(pubtypes)==0:
#        pubtype='NA'
#    else:
#        types=pd.DataFrame(pubtypes,columns=['PublicationType'])
#        item={'PMID':articleID,'PublicationType':types['PublicationType']}
#        item_df=pd.DataFrame(item)
#        
#        result_pubtypes.append(item_df)
        
print(time.time()-start)


citation_article_df=pd.concat(result_citation_article)
author_article_df=pd.concat(result_author_article)
journal_df=pd.concat(result_journal)


pubtype_article_df=pd.concat(result_pubtypes).drop_duplicates()
pubtype_df=pubtype_article_df['PublicationType'].drop_duplicates()

pickle.dump(pubtype_article_df,open(r'E:\Cartographer\TermSeeded\pubtype_article_data.pkl','wb'))
pickle.dump(pubtype_df,open(r'E:\Cartographer\TermSeeded\pubtype_data.pkl','wb'))
   
#these are for relations
author_article_df=author_article_df.drop_duplicates()
citation_article_df=citation_article_df.drop_duplicates()
journal_df=journal_df.drop_duplicates()

pickle.dump(author_article_df,open(r'E:\Cartographer\TermSeeded\article_author_data.pkl','wb'))
pickle.dump(citation_article_df,open(r'E:\Cartographer\TermSeeded\article_citation_data.pkl','wb'))
pickle.dump(journal_df,open(r'E:\Cartographer\TermSeeded\journal_article_data.pkl','wb'))
#these are for creation

authors_df=author_article_df[['Author']].drop_duplicates()
articles_df=author_article_df[['PMID','Title','PublishDate','Abstract']].drop_duplicates()
journalsonly_df=journal_df[['Journal','JournalID']].drop_duplicates()

pickle.dump(authors_df,open(r'E:\Cartographer\TermSeeded\author_data.pkl','wb'))
pickle.dump(articles_df,open(r'E:\Cartographer\TermSeeded\article_data.pkl','wb'))
pickle.dump(journalsonly_df,open(r'E:\Cartographer\TermSeeded\journal_data.pkl','wb'))


#%%#xport to csv
#create db using the pubmedcartographer neo4j queryes 

articles_df.to_csv(r'E:\Cartographer\TermSeeded\articles.csv',index=False,index_label=False)
authors_df.to_csv(r'E:\Cartographer\TermSeeded\authors.csv',index=False,index_label=False)
journalsonly_df.to_csv(r'E:\Cartographer\TermSeeded\journals.csv',index=False,index_label=False)
pubtype_df.to_csv(r'E:\Cartographer\TermSeeded\pubtypes.csv',index=False,index_label=False)

author_article_df.to_csv(r'E:\Cartographer\TermSeeded\author_article.csv',index=False,index_label=False)
citation_article_df.to_csv(r'E:\Cartographer\TermSeeded\citation_article.csv',index=False,index_label=False)
journal_df.to_csv(r'E:\Cartographer\TermSeeded\journal_article.csv',index=False,index_label=False)
pubtype_article_df.to_csv(r'E:\Cartographer\TermSeeded\pubtype_article.csv',index=False,index_label=False)













#%%No Longer needed, refer to the neo4j page

#%create articles
#batch

articles_df=pickle.load(open(r'E:\Cartographer\TermSeeded\article_data.pkl','rb'))
batches=Batching(250,articles_df)

#insertion
uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

with driver.session() as session:
    
    for batch in batches:
        cypher=''
        #general cypher text
        basequery='Create'
        finisher='})'
        connector=','
        
        
        for count,df in enumerate(batch.iterrows()):
        
            data=df[1]
            starter='(a'+str(count)+':Publication{'
            
            #create cypher of title
            title=data['Title']
            
            if title==None:
                title=''
            
            if '"' in title:
                title=title.replace('"',r'\"')
                
            elif '\\' in title:
                title=title.replace('\\','\\\\')
            
            cypher_name='Title:"'+title+'"'
            
            
            cypher_id='PMID:"'+data['PMID']+'"'
            cypher_date='PublishDate:"'+data['PublishDate']+'"'
        
            cypher_data=cypher_id+connector+cypher_date+connector+cypher_name
            
            
            if count==0 and len(batch)!=1:
                cypher=basequery+starter+cypher_data+finisher+connector
            elif count==0 and len(batch)==1:
                cypher=basequery+starter+cypher_data+finisher
            
            elif count==len(batch)-1:
                cypher=cypher+starter+cypher_data+finisher
            else:
                cypher=cypher+starter+cypher_data+finisher+connector
            
        session.run(cypher)
#% create authors
        
authors_df=pickle.load(open(r'E:\Cartographer\TermSeeded\author_data.pkl','rb'))
batches=Batching(250,authors_df)

uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

with driver.session() as session:
    
    
   for c1,batch in enumerate(batches):
        cypher=''
        #general cypher text
        basequery='Create'
        finisher='"})'
        connector=','
        
        for count,data in enumerate(batch):
        
            starter='(a'+str(count)+':Physician{Name:"'
            
            if count==0 and len(batch)!=1:
                cypher=basequery+starter+data+finisher+connector
            elif count==0 and len(batch)==1:
                cypher=basequery+starter+data+finisher
            
            elif count==len(batch)-1:
                cypher=cypher+starter+data+finisher
            else:
                cypher=cypher+starter+data+finisher+connector
        
#        try:
        session.run(cypher)    
#        time.sleep(1)
#        except:
#            print(cypher)
        
        
        
#%Create journals
        
journals_df=pickle.load(open(r'E:\Cartographer\TermSeeded\journal_data.pkl','rb'))
batches=Batching(250,journals_df)

uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

with driver.session() as session:
    
    
   for batch in batches:
        cypher=''
        #general cypher text
        basequery='Create'
        finisher='})'
        connector=','
        
        
        for count,df in enumerate(batch.iterrows()):
        
            data=df[1]
            starter='(a'+str(count)+':Journal{'
            
            #create cypher of title
            title=data['Journal']
            
            if title==None:
                title=''
            
            if '"' in title:
                title=title.replace('"',r'\"')
                
            elif '\\' in title:
                title=title.replace('\\','\\\\')
            
            cypher_name='Name:"'+title+'"'
            
            
            cypher_id='ISSN:"'+data['JournalID']+'"'
#            cypher_date='PublishDate:"'+data['PublishDate']+'"'
        
            cypher_data=cypher_id+connector+cypher_name
            
            
            if count==0 and len(batch)!=1:
                cypher=basequery+starter+cypher_data+finisher+connector
            elif count==0 and len(batch)==1:
                cypher=basequery+starter+cypher_data+finisher
            
            elif count==len(batch)-1:
                cypher=cypher+starter+cypher_data+finisher
            else:
                cypher=cypher+starter+cypher_data+finisher+connector
            
        session.run(cypher)   
#%%

#%relationships
author_article_df=pickle.load(open(r'E:\Cartographer\TermSeeded\article_author_data.pkl','rb'))
citation_article_df=pickle.load(open(r'E:\Cartographer\TermSeeded\article_citation_data.pkl','rb'))
journal_article_df=pickle.load(open(r'E:\Cartographer\TermSeeded\journal_article_data.pkl','rb'))        

#%
uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

#multicreate is possible->
#match (a:Journal),(b:Physician), (c:Publication) where a.ISSN="1433-8726" and c.PMID="23212295" and b.Name="Milessa Decker" create ((a)-[r:Test]->(b)),((a)-[r2:Test]->(c)) 
#need to explore and see if it is faster. 

#cypherlist=[]
ignorelist=[]
timelist=[]
consec=0



batches=Batching(100,articles_df)

start=time.time()

for count,article in enumerate(articles_df.iterrows()):
    
    start=time.time() 
    
    print(str(count)+'/'+str(articles_df.shape[0]))
    
    data=article[1]
    
    articleID=data['PMID']
    
    df=author_article_df.query('PMID=="'+str(articleID)+'"')

    df_cite=citation_article_df.query('PMID_Origin=="'+str(articleID)+'"')
    
    citelist=df_cite['PMID_CitedIn']
    authorlist=df['Author']
    
    cypher_auth='['
    
    journalID=journal_article_df.query('PMID=="'+str(articleID)+'"')['JournalID'].iloc[0]    
    
    if len(authorlist)==0:
        ignorelist.append(result)
        pass
    else:
    
        for count,author in enumerate(authorlist):
    
            if count+1==len(authorlist):
                
                cypher_auth=cypher_auth+'"'+author+'"]'
            else:
                cypher_auth=cypher_auth+'"'+author+'",'
                
        cypher='Match (a:Publication),(b:Physician) WHERE b.Name in '+cypher_auth+' AND a.PMID="'+articleID+'" Create (b)-[r:Author]->(a) return a.Name'
        
        with driver.session() as session:
            session.run(cypher)
            
    if len(citelist)==0:
        pass
    else:
        cypher_cite='['
    

        for count,cite in enumerate(citelist):
            if count+1==len(citelist):
                cypher_cite=cypher_cite+'"'+str(cite)+'"]'
            else:
                cypher_cite=cypher_cite+'"'+str(cite)+'",'
                    
          
        cypher='Match (a:Publication),(b:Publication) WHERE b.PMID in '+cypher_cite+' AND a.PMID="'+articleID+'" Create (a)<-[r:Cited]-(b) return a.Name'
        
        with driver.session() as session:
            session.run(cypher)
#            

    #still need to deal with the blank journals. do by hand for now?
    if journalID[0]=='\n':
        pass
    else:
        cypher_journal='Match (a:Publication),(b:Journal) WHERE b.ISSN="'+journalID+'" AND a.PMID="'+articleID+'" Create (a)-[r:PublishedIn]->(b) return a.Name'
        with driver.session() as session:
            session.run(cypher_journal)
    
    timelist.append(time.time()-start)
            
session.close()
print(time.time()-start)


#%%

#author merging
#first, sort by last name

#then link with author affiliations, if different, then likely different people 

#can go further with a ml system, first split all affils by comma, then create a most common list, then a feature matrix 


result_list=pickle.load(open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','rb'))

#filter first by lastnames
auth_list=[]

for each in result_list[0:100]:
    
    authorlist=each['Authors']
    
    for author in authorlist:


        forenames=author['ForeName'].split(' ')
        
        if len(forenames)==2:
            fname=forenames[0]
            mname=forenames[1]
        else:
            fname=author['ForeName']
            mname=''
        
        
        item={'Forename':fname,'MiddleName':mname,'LastName':author['LastName'],'Affil':author['Affiliation']}        
        auth_list.append(item)
    
    
affil=pd.DataFrame(auth_list)['Affil'].drop_duplicates()
    
#auth_df_main=pd.DataFrame(auth_list).drop_duplicates().sort_values('Forename').reset_index(drop=True)

#lnames=auth_df_main['LastName'].drop_duplicates()

#%%   
comparelist=[]

start=time.time()
for count,lname in enumerate(lnames):
    
    if count%1000==0 and count!=0:
        print(count)
    
    auth_df=auth_df_main.query('LastName=="'+lname+'"')

    #do initials first
    initials=auth_df[auth_df['Forename'].str.len()==1]
    
    
    boollist=[]
    for row in initials.iterrows():
        
        each=row[1]
        
        if each['MiddleName']=='':
            check=auth_df['Forename'].str.startswith(each['Forename'])
            check=pd.concat([auth_df['Forename'].str.startswith(each['Forename']), auth_df['Forename'].str.len()!=1],axis=1).all(axis=1)
            interest=auth_df[check].append(each)
        else:
            check=pd.concat([auth_df['Forename'].str.startswith(each['Forename']), auth_df['MiddleName'].str.startswith(each['MiddleName'])],axis=1).all(axis=1)
            interest=auth_df[check]
            
    #    boollist.append(check)
            
        
    
        if interest.shape[0]!=1:#if its the only one, ignore
            
            comparelist.append(interest)
                
    
    #now do the name stuff
    auth_df_non_init=auth_df[auth_df['Forename'].str.len()!=1]    
    vcount=auth_df_non_init['Forename'].value_counts()
    vcount=vcount[vcount>1]
    
    names=vcount.reset_index()['index']
            
    #start=time.time()
    #namelist=[]
    
    for name in names:
        sub=auth_df.query('Forename=="'+name+'"')
        comparelist.append(sub)

print(time.time()-start)

pickle.dump(comparelist,open(r'E:\Cartographer\TermSeeded\author_check.pkl','wb'))

comparelist=pickle.load(open(r'E:\Cartographer\TermSeeded\author_check.pkl','rb'))

countlist=[]
for each in comparelist:
    if each.shape[0]==2:
        countlist.append(each)


#neo4j query
#this seems to work better when its not an abbreviation/initials. 
#match (a:Author),(b:Author), p=allShortestPaths((a)-[*..5]-(b)) where none(r in relationships(p) WHERE type(r)='Cited') and none(r in relationships(p) WHERE type(r)='isType') and none(r in relationships(p) WHERE type(r)='Describes') and a.Name='Benjamin J Shore' and b.Name='B Shore' return count(p)

#match (a:Author),(b:Author), p=allShortestPaths((a)-[*..5]-(b)) where none(r in relationships(p) WHERE type(r)='Cited') and none(r in relationships(p) WHERE type(r)='isType') and a.Name='D H Thamm' and b.Name='Douglas H Thamm' return count(p)


#journal only runs into issues if the publishing volume isn't very high, which reduces the change of overlap
#descrriptions are good too, but need some way to weight how significant it is. ie if its just 'human', it doesn't mean much 

#could compare how many paths vs how many possible routes (count (descriptor+journals) for each,divided by #articles )
#%%
uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))
#duallist=[]
potentiallist=[]

#do the easy ones first
#for each in comparelist:
#    if each.shape[0]==2:
#        duallist.append(each)

#for df in duallist:
#    name1=df['Forename'].iloc[0]+' '+df['MiddleName'].iloc[0]+' '+df['LastName'].iloc[0]
#    name2=df['Forename'].iloc[1]+' '+df['MiddleName'].iloc[1]+' '+df['LastName'].iloc[1]
#for the easy ones up top

for df in name_comparelist:
    name1=df[0]
    name2=df[1]

    
    #need to account for if they share co-authors?
    with driver.session() as session:
        cypher=r'match (a:Author),(b:Author), p=allShortestPaths((a)-[*..5]-(b)) where none(r in relationships(p) WHERE type(r)="Cited") and none(r in relationships(p) WHERE type(r)="isType") and a.Name="'+name1+'" and b.Name="'+name2+'" return nodes(p)'
        result=session.run(cypher)
        
        rel_links=result.data()
        session.close()


    desclist=[]
    link_count=len(rel_links)
    
    if link_count==0:
        #no match
        pass
    else:
        
        for each in rel_links:
            data=each[list(each.keys())[0]]
            for nodes in data:
                if 'idx' in list(nodes.keys()):
                    desclist.append(nodes)
                    
    
    clean_desc=[]
    #following keys too general, don't mean anything. would like to weighted, but not sure how to do that
    #other thing is more specific the descriptor, ie if it has a qualifier, should also weigh more heavily
    ignorelist=['86','84','80','837','552','285','491','826']
    existlist=[]
    #filter for the ones we want to deprio
    for each in desclist:
        
        if each['idx'] not in ignorelist and each['idx'] not in existlist:
            clean_desc.append(each)
            existlist.append(each['idx'])
        
    filter_link_count=len(clean_desc)
    
    potentiallist.append([filter_link_count,clean_desc,name1,name2])
#    response=session.run()
  #%%  
#the more complicated ones
    
multilist=[]
for each in comparelist:
    if each.shape[0]!=2:
        multilist.append(each)


def combineName(df_row):
    
    if df_row['MiddleName']=='':
        name=df_row['Forename']+' '+df_row['LastName']
    else:
        name=df_row['Forename']+' '+df_row['MiddleName']+' '+df_row['LastName']
        
    return name
    
    
name_comparelist=[]
for count,each in enumerate(multilist):
    print(count)
    each=each.sort_values('Forename')
    
    if len(each.iloc[0]['Forename'])==1:#check if there are initials, they get set at the top by the sort valuyes
        #if initials found, need to match with all
        
        initname=combineName(each.iloc[0])
        
        for row in each[1:].iterrows():
            data=row[1]
            
            nextname=combineName(data)
            
            name_comparelist.append([initname,nextname])
            
    #check for first name duplicates that also don't have differing middlenames
    
    each_vcount=each['Forename'].value_counts().reset_index()
    
    dupenames=each_vcount.query('Forename>1')
    
    for name in dupenames['index']:
        sub_names=each.query('Forename=="'+name+'"').sort_values('Forename')
        
        #check if there is a middleName
        
        blankmiddle=sub_names.query('MiddleName==""')
        
        if blankmiddle.shape[0]!=0:
            #if no middle name, then need to match with all
            initname=combineName(blankmiddle.iloc[0])
            
            allothers=sub_names.query('MiddleName!=""')
            
            for row in allothers.iterrows():
                data=row[1]
                
                nextname=combineName(data)
                
                name_comparelist.append([initname,nextname])
                


        for name in sub_names['MiddleName']:
            if len(name)==1:#if initial, create match with every other row that starts with that initial
                allothers=sub_names.query('MiddleName!="'+name+'"')
                
                allothers=allothers[allothers['MiddleName'].str.startswith(name)]
                
                initname=combineName(sub_names.query('MiddleName=="'+name+'"').iloc[0])
                
                for row in allothers.iterrows():
                    data=row[1]
                    
                    nextname=combineName(data)
                    
                    name_comparelist.append([initname,nextname])
                        
    
        


#%%merge nodes
mergelist=[]                
for each in potentiallist:
    if each[0]>0:
        mergelist.append(each)
                    
list1=[]
list2=[]
list3=[]          
for each in mergelist:
    list1.append(each[0])
    list2.append(each[2])
    list3.append(each[3])
        
noninit=[]
ambig_initial=[]        
for each in mergelist:

    
    if len(each[2].split(' ')[0])!=1:
        noninit.append([each[0],each[2],each[3]])
    else:
        ambig_initial.append([each[0],each[2],each[3]])


vcount=noninit_df[1].value_counts().reset_index()

cleaned=[]
for row in vcount.iterrows():
    each=row[1]

    if each[1]==1:
        
        sub_df=noninit_df[noninit_df[1]==each['index']]
        
        cleaned.append([sub_df.iloc[0][1],sub_df.iloc[0][2]])
    else:
        sub_df=noninit_df[noninit_df[1]==each['index']].sort_values(0,ascending=False)
        
        if sub_df.iloc[0][0]==sub_df.iloc[1][0]:
            continue
        else:
            cleaned.append([sub_df.iloc[0][1],sub_df.iloc[0][2]])
    
cleaned_df=pd.DataFrame(cleaned)


#%%
#will deal with the noninitals first, since there is still ambiguity with the initials 
for each in cleaned[5:]:
    
#    if each[0]>=1:
    name1=each[0]
    name2=each[1]
    
    #find the most detailed node
    if len(name1)>len(name2):
        firstName=name1
        secondName=name2
    elif len(name2)>len(name1):
        firstName=name2
        secondName=name1
    
    
    cypher='MATCH (a1:Author{Name:"'+firstName+'"}), (a2:Author {Name:"'+secondName+'"})\
    WITH head(collect([a1,a2])) as nodes\
    CALL apoc.refactor.mergeNodes(nodes,{properties:"discard", mergeRels:true})\
    YIELD node RETURN nodes limit 5'
    
    with driver.session() as session:
        session.run(cypher)
            
session.close()          
#should really refactor the author node to have fname, lastname, mname, instead of just one full name 




#%% calculate the average pagerank of everybody on a certain article 

uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))


#cypher='match (a:Publication)-[r:Authored]-(b:Author)-[r2:Authored]-(c:Publication) where a.PMID="27147135" return distinct b.Name'
cypher='match (a:Author) return a.Name as Name'

with driver.session() as session:
    result=session.run(cypher)
    authlist=result.data()

    prlist=[]
    start=time.time()
    for count, each in enumerate(authlist):
        
        if count%10000==0 and count!=0:
            print('Working on '+str(count)+'/'+str(len(authlist)))
            print((time.time()-start)/60)
            start=time.time()
        
        name=each['Name']
        
        cypher_auth='match (b:Author)-[r2:Authored]-(c:Publication) where b.Name="'+name+'" return avg(c.PageRank) as Average, max(c.PageRank) as MaxPR'
        
        result_auth=session.run(cypher_auth)
        prdata=result_auth.data()
        
        avg_pr=prdata[0]['Average']
        max_pr=prdata[0]['MaxPR']
        
        prlist.append({'Name':name,'Average_PageRank':avg_pr,'Max_PageRank':max_pr})
        
df_pr=pd.DataFrame(prlist)

pickle.dump(df_pr,open(r'E:\Cartographer\TermSeeded\Author_PageRank_5.30.2021.pkl','wb'))


auth_pr=pickle.load(open(r'E:\Cartographer\TermSeeded\Author_PageRank_5.30.2021.pkl','rb'))
auth_percentiles=np.percentile(auth_pr['Average_PageRank'],range(0,100,5))

auth_pr.to_csv(r'E:\Cartographer\TermSeeded\auth_pr.csv')

#Old
#average pagerank of everybody=0.2564830756104266
#median=0.1758150318461719, heavily skewed, should probably use median to compare. 
#min=0.15000000000000002
#max=15.997376543481595

#New, as of 5.30.2021, Updated with Crossref references 

cypher='match (a:Publication) return a.PMID as PMID,a.PageRank as PageRank'

with driver.session() as session:
    result=session.run(cypher)
    idlist=result.data()

    pub_prlist=[]
    start=time.time()
    
    for each in idlist:
        name=each['PMID']
        rank=each['PageRank']
        
        pub_prlist.append({'Name':name,'PageRank':rank})
        
df_pub_pr=pd.DataFrame(pub_prlist)

pub_percentiles=np.percentile(df_pub_pr['PageRank'],range(0,100,5))



#%%

df_pr=df_auth
cypher='match (a1:Descriptor)-[r1:Describes]-(a2:Publication)-[r2:Authored]-(a3:Author) where a1.Name contains "Prostatic Hyperplasia" and a1.Qualifier="therapy" return a3.Name as Name'

sub_auth_list=[]
with driver.session() as session:
    result=session.run(cypher)
    auth_result=result.data()
    
    for each in auth_result:
        name=each['Name']
        sub_auth_list.append(name)
    
sub_auth_df=[]
for each in sub_auth_list:
    sub_df=df_pr.query('Name=="'+each+'"')
    sub_auth_df.append([each,sub_df['Average_PageRank'].iloc[0]])
    
sub_auth_df=pd.DataFrame(sub_auth_df,columns=['Name','Avg_PR'])


pickle.dump(sub_auth_df,open(r'E:\Cartographer\TermSeeded\bph_Author_PageRank.pkl','wb'))

#average global pagerank of prostatic hyperplasia articles=.238945
#median for above=0.19763438558336838
#max=13.9285
#min=.15


#bph therapy publications only
#mean



#%%
#consider only their bph articles:

#for example Neal D Shore
#pr with only bph: 0.3685403494626427
#pr with everything:0.7015047017986555

start=time.time()

sub_df=pd.DataFrame(sub_auth_list,columns=['Name'])
sub_df=sub_df.drop_duplicates()

sub_auth_pr=[]
with driver.session() as session:
    for each in sub_auth_list:
        cypher='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Describes]-(c:Descriptor) where c.Name contains "prostatic hyperplasia" and a.Name="'+each+'" return avg(b.PageRank) as pr'
        
        result=session.run(cypher)
        
        rank=result.data()
        
        sub_auth_pr.append([each,rank[0]['pr']])
        
sub_auth_pr=pd.DataFrame(sub_auth_pr,columns=['Name','Avg_PR'])   

print(time.time()-start)

#mean: 0.195596
#median:0.16446697756182405



#notes: peoples scores are inflated by things like prostate cancer articles, so if you were searching off of this, and you wanted to know specifically how influential
#they were in a field, would need to filter by descriptors(bph) as well. 
#alternatively, would need to subset filter first, then run pagerank on people's articles. the current pagerank score is 'global'


#%%

#find avg article pagerank of journals
uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))


#cypher='match (a:Publication)-[r:Authored]-(b:Author)-[r2:Authored]-(c:Publication) where a.PMID="27147135" return distinct b.Name'
cypher='match (a:Journal) return a.ISSN as ISSN,a.Name as Name'

with driver.session() as session:
    result=session.run(cypher)
    authlist=result.data()

    jrlist=[]
    start=time.time()
    for count, each in enumerate(authlist):
        
        if count%10000==0 and count!=0:
            print('Working on '+str(count)+'/'+str(len(authlist)))
            print((time.time()-start)/60)
            start=time.time()
        
        ISSN=each['ISSN']
        name=each['Name']
        
        cypher_auth='match (b:Journal)-[r2:PublishedIn]-(c:Publication) where b.ISSN="'+ISSN+'" return avg(c.PageRank) as Average'
        
        result_auth=session.run(cypher_auth)
        prdata=result_auth.data()
        
        avg_pr=prdata[0]['Average']
        
        jrlist.append({'Name':name,'ISSN':ISSN,'Average_PageRank':avg_pr})
        
df_jr=pd.DataFrame(jrlist)

pickle.dump(df_jr,open(r'E:\Cartographer\TermSeeded\Journal_PageRank.pkl','wb'))

#%%

uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))



#pull descriptors and organize 

cypher_descrip='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Describes]-(c:Descriptor) where a.Name="Henry Hyunshik Woo" return b as Publication ,r2,c as Descriptor'
#cypher2='MATCH (a:Author)-[r1:Authored]-(b:Publication)-[r2:Authored]-(c:Author) where a.Name="Henry Hyunshik Woo" return b,r2,c'

with driver.session() as session:
    result=session.run(cypher_descrip)
    descriplist=result.data()
session.close()

total_descriplist=[]
for each in descriplist:
#    print(each['Author1']['Name'])
    
    if 'Qualifier' in list(each['Descriptor'].keys()):
    
        total_descriplist.append([each['Descriptor']['Name'],each['Descriptor']['Qualifier']])
    else:
        total_descriplist.append([each['Descriptor']['Name'],''])

df_descrip=pd.DataFrame(total_descriplist,columns=['Name','Qualifier'])

most_common_descrip_general=df_descrip['Name'].value_counts()

most_common_descrip_specific=df_descrip[['Name','Qualifier']].value_counts()

most_common_descrip_general=most_common_descrip_general.reset_index()
most_common_descrip_general=most_common_descrip_general.rename(columns={'index':'Name','Name':'Counts'})

most_common_descrip_specific=most_common_descrip_specific.reset_index()
most_common_descrip_specific=most_common_descrip_specific.rename(columns={0:'Counts'})


#pull co-authors
cypher_auth='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Authored]-(c:Author) where a.Name="Claus G Roehrborn" return b as Publication,r2,c as Author'

with driver.session() as session:
    result=session.run(cypher_auth)
    authlist=result.data()
session.close()

co_authlist=[]

for each in authlist:
    co_authlist.append(each['Author']['Name'])
    
df_auth=pd.DataFrame(co_authlist,columns=['Name'])

common_coauth=df_auth['Name'].value_counts()

#pull publication history in journals
cypher_journ='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:PublishedIn]-(c:Journal) where a.Name="Henry Hyunshik Woo" return b as Publication,r2,c as Journal'

with driver.session() as session:
    result=session.run(cypher_journ)
    journlist=result.data()
session.close()

journ_histlist=[]

for each in journlist:
    journ_histlist.append(each['Journal']['Name'])
    
df_journ=pd.DataFrame(journ_histlist,columns=['Name'])

common_journ=df_journ['Name'].value_counts()

common_journ=common_journ.reset_index()
common_journ=common_journ.rename(columns={'index':'Name','Name':'Counts'})


cypher_pubtypes='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:isType]-(c:PublicationType) where a.Name="Henry Hyunshik Woo" return b as Publication,r2,c as PubType'

with driver.session() as session:
    result=session.run(cypher_pubtypes)
    pubtypelist=result.data()
session.close()

pubtypelistoutput=[]

for each in pubtypelist:
    pubtypelistoutput.append(each['PubType']['Name'])

df_pubtypes=pd.DataFrame(pubtypelistoutput,columns=['Name'])

common_pubtypes=df_pubtypes['Name'].value_counts()    



cypher_pubrank='match (a:Author)-[r:Authored]-(b:Publication) where a.Name="Henry Hyunshik Woo" return b.Title as Name, b.PageRank as PR, b.PublishDate as PubDate'

with driver.session() as session:
    result=session.run(cypher_pubrank)
    publist=result.data()
session.close()

pubPRlistoutput=[]

for each in publist:
    
    pubPRlistoutput.append([each['Name'],each['PR'],str.strip(each['PubDate'])])
    
df_pubPR=pd.DataFrame(pubPRlistoutput,columns=['Name','PageRank','PublishDate'])
df_pubPR.sort_values('PageRank',ascending=False,inplace=True)


#%%
affil_lookup=pickle.load(open(r'E:\Cartographer\TermSeeded\author_affil_lookup.pkl','rb'))

lookup_list=['Peter John Gilling',
             'Kevin T McVary',
             'Sascha A Ahyai',
             'Jean de la Rosette',
             'Christian Gratzke',
             'Giacomo Novara',
             'Jean-Nicolas Comu',
             'Alexander Bachmann',
             'Stephan Madersbacher' 

             ]
outputlist=[]

for name in lookup_list:
    
    output=affil_lookup.query('Author=="'+name+'"')
    
    outputlist.append(output['Affiliation'].drop_duplicates())
    
#%%
    
#affil method based on pagerank of article
result_list=pickle.load(open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','rb'))

#filter first by lastnames
auth_list=[]

for count,each in enumerate(result_list):
    
    authorlist=each['Authors']
    
    if count%10000==0 and count!=0:
        print('Working on '+str(count)+'/'+str(len(result_list)))
#        print((ti1me.time()-start)/60)
#        start=time.time()

    
    for author in authorlist:


        forenames=author['ForeName'].split(' ')
        
        if len(forenames)==2:
            fname=forenames[0]
            mname=forenames[1]
        else:
            fname=author['ForeName']
            mname=''
        
        lname=author['LastName']
        fullName=author['ForeName']+' '+author['LastName']

        item={'Name':fullName,'Forename':fname,'MiddleName':mname,'LastName':lname,'Affil':author['Affiliation'],'PMID':each['PMID']}        
        auth_list.append(item)
    
    
affil=pd.DataFrame(auth_list)
affil.to_csv(r'E:\Cartographer\TermSeeded\affil_lookup.csv')


#%%

cypher='match (a:Publication) return a.PMID as PMID,a.PageRank as pr'

pr_result=[]
with driver.session() as session:
    result=session.run(cypher)
    query_result=result.data()
    
    for each in query_result:
        
        pr_result.append({'PMID':each['PMID'],'PageRank':each['pr']})
        

df_pr=pd.DataFrame(pr_result)

affil_merged=affil.merge(df_pr,how='inner',left_on='PMID',right_on='PMID')
affil_merged=affil_merged.drop_duplicates()

affil_auth=affil_merged[['Name','Forename','MiddleName','LastName']].drop_duplicates()

affil_auth.to_csv(r'E:\Cartographer\TermSeeded\authors_detailed.csv')
affil_merged.to_csv(r'E:\Cartographer\TermSeeded\author_publication_detailed.csv')

#pull all authors, then match by PMID the affiliations

cypher='match (a:Author) return a.Name as Name'

compiled=[]
with driver.session() as session:
    result=session.run(cypher)
    authlist=result.data()

    prlist=[]
    start=time.time()
    for count, each in enumerate(authlist):
        
        if count%10000==0 and count!=0:
            print('Working on '+str(count)+'/'+str(len(authlist)))
            print(time.time()-start)
        
        name=each['Name']
         
        namebreak=name.split(' ')
         
        if len(namebreak)==2:
            s1=affil_merged.query('Forename=="'+namebreak[0]+'"').query('LastName=="'+namebreak[1]+'"')
        else:
            s1=affil_merged.query('Forename=="'+namebreak[0]+'"').query('LastName=="'+namebreak[2]+'"')

        compiled.append(s1)   

pickle.dump(compiled,open(r'E:\Cartographer\TermSeeded\author_affil_list','wb'))         


author_affil_contact=[]
start=time.time()

for c1,row in enumerate(compiled):
    
    if c1%10000==0 and c1!=0:
        print('Working on '+str(c1)+'/'+str(len(compiled)))
        print(time.time()-start)
    
    if row.shape[0]==0:
        continue
    
    if np.sum(row['Affil']!='')==0:
#        print('hello')
        continue
    else:
        author=row['Name'].iloc[0]
        cleanaffil=[]
        emaillist=[]
        for count,each in enumerate(row['Affil']):
            split_af=each.split(' ')
            email=False
            removelist=[]
            emailignore=[]
            emailcount=0
            
            for phrase in split_af:
                if '@' in phrase and phrase not in emaillist and email==False:
                    original=phrase
                    if phrase[-1]=='.':
                        
                        phraselist=list(phrase)
                        phraselist[-1]=''
                        
                        phrase=''.join(phraselist)
                    
                    if 'Address:' in phrase:
                        phrase=phrase.replace('Address:','')
                    
                    emaillist.append(phrase)
                    email=True
    
    #                    emailcount=emailcount+1
                    removelist.append(original)
                
                elif 'electronic' in str.lower(phrase) or 'address:' in str.lower(phrase) or 'e-mail:' in str.lower(phrase):
                    original=phrase
                    if phrase[-1]=='.':
                        
                        phraselist=list(phrase)
                        phraselist[-1]=''
                        
                        phrase=''.join(phraselist)
                    
                    removelist.append(original)
            
            for term in removelist:
                split_af.remove(term)
            
            
            cleanaffil.append([' '.join(split_af),row['PMID'].iloc[count],row['PageRank'].iloc[count]])            
    
        clean_affil_df=pd.DataFrame(cleanaffil,columns=['Affil','PMID','PageRank']).sort_values('PageRank',ascending=False)
        
        sorted_affil_df=clean_affil_df.sort_values('PageRank')
        
        clean_email=pd.DataFrame(emaillist,columns=['email']).drop_duplicates()
        
        author_affil_contact.append([author,clean_affil_df,emaillist])
   
pickle.dump(author_affil_contact,open(r'E:\Cartographer\TermSeeded\author_affil_contact','wb'))         

  #%%
  
  #Notes:
  
  #looks more and more like pubtypes doesn't deserve its own type, should just be a property of a publication, make sit easier to search/filter
  #Descriptors are capable of giving more general overview, but ideally, need the abstract to give more details<-need better way or organizing it 
  
  #Afill is still a mess 
  #incomplete citations, look for elink to known site, ie elsevier, and then extract references that way? 

#%%

#implementing Crossref to supplement
#testing
import urllib
import requests
from xml.etree import ElementTree
import time
import pickle
import pandas as pd

#%
#get doi from pubmed
result_list=pickle.load(open(r'E:\Cartographer\TermSeeded\articledata-termseed.pkl','rb'))

pmidlist=[]
for each in result_list:
    pmidlist.append(each['PMID'])

df_pmid=pd.DataFrame(pmidlist)
df_pmid=df_pmid.drop_duplicates()

pmid_series=df_pmid[0]



batches=Batching(1000,pmid_series)
    
#for count,pmid in enumerate(pmid_series):
    
#    basePath='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml&id='+str(pmid)
#        
#    s=requests.Session()



doi_pmid=[]
time_taken=[]
for count,batch in enumerate(batches):
    
    start=time.time()
    print('Working on Batch '+str(count)+'//'+str(len(batches)))
    
    basePath='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=xml'
        
    s=requests.Session()
    
#    response=s.post(basePath,data={'id':batch})
    
    response_good=False
    
    while response_good==False:
        
        response=s.post(basePath,data={'id':batch})
        
        if response.status_code==200:
            response_good=True
        else:
            response_good=False
            print('Error encountered on api request, pausing for 1 min')
            time.sleep(60)
        
    if response_good==True:
    
        tree=ElementTree.fromstring(response.content)
        
        for i in range(len(tree)):
    
            pmid=tree[i].find('.//ArticleId[@IdType="pubmed"]').text
            
            doi_pre=tree[i].find('.//ArticleId[@IdType="doi"]')
            
            if doi_pre!=None:
                doi=tree[i].find('.//ArticleId[@IdType="doi"]').text
            else:
                doi=''
    
            doi_pmid.append({'PMID':pmid,'DOI':doi})

    print(time.time()-start)
    time_taken.append(time.time()-start)
    time.sleep(5)
    if count%5==0 and count!=0:
                
        pickle.dump(doi_pmid,open(r'E:\Cartographer\pmid_doi_pull.pkl','wb'))
     
        
doi_pmid=pickle.load(open(r'E:\Cartographer\pmid_doi_pull.pkl','rb'))

 
    
df_doi_pmid=pd.DataFrame(doi_pmid)

df_doi_pmid_nonblank=df_doi_pmid.query('DOI!=""')



#%%pull from crossref
df_crossref_detail=[]
finish_list=[]
error_list=[]
error_list_detail=[]

#%%
start=time.time()
for count,row in df_doi_pmid_nonblank.iterrows():
    

    if count%100==0 and count>100000:
        print(count,time.time()-start)
        
    doi=row['DOI']
    
    if doi in finish_list or doi in error_list:
        continue
    else:
    
        params=urllib.parse.urlencode({'filter':'doi:'+doi,'mailto':'kcchang@ucdavis.edu'})
        
        path='https://api.crossref.org/works?'+params
        
        response_good=False
        response_count=0
        major_error=False
        
        with requests.Session() as session:
        
            while response_good==False:
                
                response=session.get(path)
                
                if response.status_code==200:
                    response_good=True
                
                #new
                if response.status_code==400:
                    major_error=True
                    print('Error Code 400, adding to list')#status code 400 is usually something wrong with the doi
                    error_list.append(doi)
                    error_list_detail.append([doi,row['PMID'],response.status_code])
                    break

                #new
                
                if response_count<5 and response.status_code!=200:
                    response_good=False
                    print('Error encountered on api request, retry in 1 min')
                    print(response.status_code)
                    time.sleep(60)
                    response_count=response_count+1
                    
                if response_count>=5:
                    major_error=True#if repeat attempts have not worked, trigger a major_error and add to error list
                    error_list.append(doi)
                    error_list_detail.append([doi,row['PMID'],response.status_code])
                    break
        
#        if major_error==True:
#            pickle.dump(df_crossref_detail,open(r'E:\Cartographer\doi_crossref_pull.pkl','wb'))
#            break #if major error flag thrown, save, and stop the program.
        
        if response_good==True:
    
            response_json=response.json()
            
            if len(response_json['message']['items'])!=0:
                response_detail=response_json['message']['items'][0]
            else:
                response_detail=''
            
            df_crossref_detail.append({'DOI':doi,'PMID':row['PMID'],'Details':response_detail})
            finish_list.append(doi)
            
            time.sleep(.5)
            
        if count%1000==0:
            time.sleep(15)
            pickle.dump(df_crossref_detail,open(r'E:\Cartographer\doi_crossref_pull.pkl','wb'))
            
#    except:
#        pickle.dump(df_crossref_detail,open(r'E:\Cartographer\doi_crossref_pull.pkl','wb'))
#        print(count)
     
        
#%%

#reset our neo4j instance to local, no point in remote access rn. 
            
#add Doi to all publication nodes in neo4j
#df_doi_pmid.to_csv('E:\Cartographer\TermSeeded\publication_doi.csv')

#use dict.keys() to look through what is available, pull the following

#pubdate: published-print or published-online
#funder: funder
#references: reference
#url: URL
#'DOI' need to link with articles
# ref count-calculate descrepency with how much doi we can pull, some don't have doi, but would like record for how much it actually has

supp_detail_list=[]

for each in df_crossref_detail:
    
    if each['Details']!='':
    
        detail=each['Details']
        keys=list(detail.keys())
        
        doi=detail['DOI']
        
        
        if 'published-print' in keys:
            pubdate=detail['published-print']
        elif 'published-online' in keys:
            pubdate=detail['published-online']
        else:
            pubdate=''
            
        if 'funder' in keys:
            funder=detail['funder']
        else:
            funder=''
            
        if 'URL' in keys:
            url='URL'
        else:
            url=''
        
        referencelist=[]
        if 'reference' in keys:
            references=detail['reference']
            
            for reference in references:#really only need the doi
                
                if 'DOI' in reference:
                    referencelist.append(reference['DOI'])
                else:
                    continue
                
        if 'reference-count' in keys:
            refcount=detail['reference-count']
        elif 'references-count' in keys:
            refcount=detail['references-count']
        else:
            refcount=0
                
                
        item={'DOI':doi,'PubDate':pubdate,'Funder':funder,'URL':url,'References':referencelist,'ReferenceCount':refcount}
        
        supp_detail_list.append(item)
  
pickle.dump(supp_detail_list,open(r'E:\Cartographer\crossref_supp_detail.pkl','wb'))
#%%

#doi is case insensitive, regex is nice, but more robust to just lower everything, and match for consistency (str.lower before matching)
doi_pmid=pickle.load(open(r'E:\Cartographer\pmid_doi_pull.pkl','rb'))

for each in doi_pmid:
    orig_doi=each['DOI']
    
    new_doi=str.lower(orig_doi)
    
    each['DOI']=new_doi


df_doi_pmid=pd.DataFrame(doi_pmid)

df_doi_pmid.to_csv('E:\Cartographer\TermSeeded\publication_doi.csv')

#%%
supp_detail_list=pickle.load(open(r'E:\Cartographer\crossref_supp_detail.pkl','rb'))


fundlist=[]
for each in supp_detail_list:
    if len(each['Funder'])!=0:
        
        for funder in each['Funder']:
            
            fundlist.append(funder)

df_funders=pd.DataFrame(fundlist)

#run through checks first- some articles aren't made, some already have relationships 

nonblank_ref=[]

for each in supp_detail_list:
    if each['ReferenceCount']!=0:
        nonblank_ref.append(each)
    
#check for existing 

#create univeral list
ref_check=[]

for each in nonblank_ref:
    reflist=each['References']
    
    temp_list=[]
    
    temp_list.append(str.lower(each['DOI']))
    
    for ref in reflist:
        temp_list.append(str.lower(ref))
    
    ref_check.extend(temp_list)
    
df_ref_check=pd.DataFrame(ref_check,columns=['DOI'])
df_ref_check=df_ref_check.drop_duplicates()


uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

with driver.session() as session:
    cypher='match (a:Publication) return a.DOI as doi, a.PMID as pmid'
    
    existing=session.run(cypher)
    
    response=existing.values()
    
exist_doi=[]
for each in response:
    exist_doi.append([each[0],each[1]])

df_exist_doi=pd.DataFrame(exist_doi,columns=['DOI','PMID'])

#create relationship for references , check against the existing doi

starting=time.time()
timelist=[]
reference_cite=[]
skip_list=[]
for count,each in enumerate(nonblank_ref):
    start=time.time()
    if count%1000==0:
        print(count,time.time()-starting)
    
    reflist=each['References']
    
    orig_doi=str.lower(each['DOI'])
    

    if orig_doi in list(df_exist_doi['DOI']):
        
        
        orig_pmid=df_exist_doi.query('DOI=="'+str.lower(orig_doi)+'"')['PMID'].iloc[0]
        
        df_ref=pd.DataFrame(reflist,columns=['Ref'])
        
        df_ref_exist_mask=df_ref.merge(df_exist_doi,how='left',left_on='Ref',right_on='DOI')
            
        df_ref_exist=df_ref_exist_mask[df_ref_exist_mask['PMID'].isna()==False]
        
        for ref in df_ref_exist['PMID']:
            
#            if ref in list(df_exist_doi['DOI']):
            
            item={'Base':orig_pmid,'Reference':ref}
            reference_cite.append(item)
                
    timelist.append(time.time()-start)

print(time.time()-starting)
        
df_ref_cite=pd.DataFrame(reference_cite)
#%%
pickle.dump(df_ref_cite,open(r'E:\Cartographer\Ref_Cite_PMID.pkl','wb'))

df_ref_cite=pickle.load(open(r'E:\Cartographer\Ref_Cite_PMID.pkl','rb'))

current_cite=pickle.load(open(r'E:\Cartographer\article_citation_data.pkl','rb'))
#current cite is set up in reverse rn

df_ref_cite_renamed=df_ref_cite.rename(columns={'Base':'PMID_CitedIn','Reference':'PMID_Origin'})

df_ref_cite.to_csv(r'E:\Cartographer\New_Cite_PMID.csv')

print(df_ref_cite_renamed.columns)

df_ref_combined_cite=current_cite.append(df_ref_cite_renamed)
df_ref_combined_cite=df_ref_combined_cite.drop_duplicates()

pickle.dump(df_ref_combined_cite,open(r'E:\Cartographer\Combined_Cite_PMID.pkl','wb'))
df_ref_combined_cite.to_csv(r'E:\Cartographer\Combined_Cite_PMID.csv')

#%%

#replace, will instead append onto the existing citation excel, and then detach/delete all citation relationships, then append new ones in.
#check if rel exists, and if not, add cite relation

start=time.time()

cylist_temp=[]

with driver.session() as session:

    for count,row in enumerate(df_ref_cite.iloc[10:].iterrows()):
        
        if count%1000==0:
            print([count,time.time()-start])
        
        rel=row[1]
        
        cypher='match (a:Publication)-[r]-(b:Publication) where a.DOI="'+rel['Base']+'" and b.DOI="'+rel['Reference']+'" return r as rel'
        
        relcheck=session.run(cypher)
    
        response=relcheck.values()
        
        if len(response)!=0:
            continue#skip if relationship already exists
        else:
            
            cypher_create='Match (a:Publication),(b:Publication) WHERE a.DOI="'+rel['Base']+'" AND b.DOI="'+rel['Reference']+'" Create (a)-[r:Cited]->(b) return a.PMID'
            cylist_temp.append(cypher)
            session.run(cypher_create)

print(time.time()-start)
#%%
    
#extract author most citied from 
target='Henry Hyunshik Woo'
#cypher='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Cited]- >(c:Publication)-[r3:Authored]-(d:Author) where a.Name="'+target+'" return distinct c,r3 as rel,d as Author'  

cypher='match (a:Author)-[r1:Authored]-(b:Publication)< -[r2:Cited]-(c:Publication)-[r3:Authored]-(d:Author) where a.Name="'+target+'" return distinct c,r3 as rel,d as Author'  


authlist=[]
with driver.session() as session:
    
    data=session.run(cypher)
    
    response=data.values()
    
    for each in response:
        
        var=each[2]
        authlist.append([var['Name'],var['avgPageRank']])
        
df_cited_auth=pd.DataFrame(authlist,columns=['Name','PR'])

cited_auth_values=df_cited_auth.value_counts()

df_cited_general=cited_auth_values.reset_index()

#most cited for a specific topic


#cypher_init='match (a:Author)-[r1:Authored]-(b:Publication)-[r2:Cited]- >(c:Publication)-[r3:Describes]-(d:Descriptor) where a.Name="Henry Hyunshik Woo" and d.Name="Lower Urinary Tract Symptoms" return distinct c.PMID as PMID'

#use this one to do reverse
cypher_init='match (a:Author)-[r1:Authored]-(b:Publication)< -[r2:Cited]- (c:Publication)-[r3:Describes]-(d:Descriptor) where a.Name="Henry Hyunshik Woo" and d.Name="Lower Urinary Tract Symptoms" return distinct c.PMID as PMID'

uri='bolt://localhost:7687'
driver=GraphDatabase.driver(uri,auth=('neo4j','password'))

with driver.session() as session: 
    response=session.run(cypher_init)

    result=response.values()
    
idlist=[]
for each in result:
    idlist.append(each[0])
    
cypher_influ_topic='match (a:Author)-[r:Authored]-(b:Publication) where b.PMID in ['

for count,each in enumerate(idlist):
    cypher_influ_topic=cypher_influ_topic+'"'+each+'"'
    
    if count==len(idlist)-1:
        cypher_influ_topic=cypher_influ_topic+'] return a.Name as Name,a.avgPageRank as PR'
    else:
        cypher_influ_topic=cypher_influ_topic+','

with driver.session() as session:
    response=session.run(cypher_influ_topic)

    result=response.values()
    
namelist=[]

for each in result:
    namelist.append([each[0],each[1]])
    
df_namelist=pd.DataFrame(namelist,columns=[['Name','PageRank']])

namecounts=df_namelist.value_counts()  

nameper=namecounts/len(idlist)

nameper_pr=nameper.reset_index()
    