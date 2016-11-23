#!/usr/bin/env python
# -*- coding: utf-8 -*-
from lxml import html
import requests
import json
from HTMLParser import HTMLParser
import csv, codecs, cStringIO
import time
import traceback
from neo4j.v1 import GraphDatabase, basic_auth

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "crielbkn8"))
session = driver.session()

# Pagina de los top 60 que se quiere scrapear #
PAGE_TOP_60 = "https://play.google.com/store/apps/category/EDUCATION?hl=en"
# Numero de "lotes" de 40 reviews #
NUM_REVIEWS = 3 
# Nuevo documento (el txt)
NUEVO = True
# Numero de apps max 60
NUMERO_APPS = 60
# header user agent para simular la llamada desde un browser
HEADERS = {
		    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.94 Safari/537.36',
		      # This is another valid field
		}

# funciones #

def agregaGl(url):
    return url.split("?hl=")[0] + "?gl=us"

def scrapDatos(url):
    page = requests.get(url, headers = HEADERS)
    tree = html.fromstring(page.content)
    titulo = tree.xpath('//div[@class= "id-app-title"]/text()')[0]
    descripcion = tree.xpath('//div[@class = "show-more-content text-body"]//p/text()')
    desc = ''
    for e in descripcion:
        desc += e
    descargas = tree.xpath('//div[@itemprop = "numDownloads"]/text()')[0].strip()
    return {'titulo': titulo, 'descripcion':desc, 'descargas':descargas}

def scrapIdsApps(url):
    page = requests.get(url, headers = HEADERS)
    tree = html.fromstring(page.content)
    ids = tree.xpath('//a[@class = "title"]/@href')
    idsCompletas = []
    for id in ids:
        idsCompletas.append(id.split("?id=")[-1])
    return idsCompletas

def reviewsGet(numeroPagina, idPackage):
    url = "https://play.google.com/store/getreviews?authuser=0&hl=en"

    querystring = {"authuser":"0"}
    payload = "-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"pageNum\"\r\n\r\n"+str(numeroPagina)+"\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"reviewType\"\r\n\r\n1\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"id\"\r\n\r\n"+str(idPackage)+"\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"reviewSortOrder\"\r\n\r\n4\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"xhr\"\r\n\r\n1\r\n-----011000010111000001101001\r\nContent-Disposition: form-data; name=\"token\"\r\n\r\nCKG1yQEIh7bJAQimtskBCMS2yQEI9JzKAQ==\r\n-----011000010111000001101001--"
    headers = {
        'content-type': "multipart/form-data; boundary=---011000010111000001101001",
        'authorization': "Basic b2thbWltYW5nYS5hcHBzQGdtYWlsLmNvbTpva210b2thd2E3Ng==",
        'cache-control': "no-cache",
        'postman-token': "d3fc04f9-5c99-52fa-79b5-a75189fb294a"
        }

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
    #print json.loads(response.text[5:])
    return response.text[5:]


def getReviews(veces, id, appNum):
    i = 0
    app = scrapDatos('https://play.google.com/store/apps/details?id='+id+'&hl=en')
    app['id'] = id
    app['reviews'] = []
    total = 0
    while i < veces:
        texto = ''
        try:
            texto = reviewsGet(i, id)
            textoListo = json.loads(texto)
            tree = html.fromstring(textoListo[0][2])
            reviews = tree.xpath('//div[@class="developer-reply"]|//div[@class="single-review"]')
            print "len de los reviews : ", len(reviews)
            listaReviews = []
            contador = 0
            for review in reviews:
                header = review.xpath('./div[@class="review-header"]')
                if len(header) > 0:
                    header = header[0]
                    listaReviews.append({'usuario': header.xpath('./div[@class="review-info"]/span[@class="author-name"]/text()')[0].strip()})
                    listaReviews[contador]['fecha'] = header.xpath('./div[@class="review-info"]/span[@class="review-date"]/text()')[0].strip()
                    listaReviews[contador]['valoracion'] = header.xpath('./div[@class="review-info"]/div[@class="review-info-star-rating"]//@aria-label')[0]

                    body = review.xpath('./div[@class="review-body with-review-wrapper"]')[0]
                    title = body.xpath('./span[@class="review-title"]/text()')
                    if len(title) == 0:
                        title = ""
                    else:
                        title = title[0]
                    listaReviews[contador]['review'] = title + ". "+ body.xpath('./text()')[1]
                    contador += 1
                    
                else:
                    reply = review.xpath('./text()')[1].strip()
                    listaReviews[contador - 1]['dev-reply'] = reply
            
            #print listaReviews
            app['reviews'] += listaReviews
            cantRev = len(listaReviews)
            total += cantRev
            print "\tcantidad reviews : ", cantRev
            print "\tpagina ", i+1, "de ", veces 
            if cantRev < 40:
                i = veces
        except:
            traceback.print_exc()
            #i = veces
            print "\terror en la pagina numero : ", i+1
        i+=1
        time.sleep( 3 )
    #try:
    #    escribirTxt(app, appNum)
    #except:
    #    print "error al escribir archivo"
    #try:
    #    escribirTxtBonito(app, appNum)
    #except:
    #   print "error al escribir bonito"
    dbGraphos(app, appNum)
    print "\tTOTAL_REVIEWS: ", total
    
def dbGraphos(dic, appNum):

    result = session.run("MERGE (a:Aplicacion {id: {n}, name: {o}, descripcion: {descripcion}, descargas: {descargas}}) RETURN a.id AS id, a.name AS name", {'n': dic['id'], 'o': dic['titulo'], 'descripcion': dic['descripcion'], 'descargas': dic['descargas']})
    for reviews in dic['reviews']:
        r = session.run("MATCH (Aplicacion {id:{n}})"
                    "MERGE (r:Review {review:{rev}, fecha:{fecha}, val:{val}})"
                    "CREATE UNIQUE (Aplicacion)-[:tiene]->(r)", {'n': dic['id'], 'o': dic['titulo'], 'autor': reviews['usuario'], 'val': reviews['valoracion'], 'rev': reviews['review'],'fecha': reviews['fecha']})
        p = session.run("MATCH (Review {review:{rev}, fecha:{fecha}})"
                        "MERGE (p: Person {nombre:{nombre}})"
                        "CREATE UNIQUE (Review)-[:escribe]->(p)", {'rev': reviews['review'], 'nombre': reviews['usuario'], 'fecha': reviews['fecha']})            
                    #"MERGE (p:Person {val:{val}, rev:{rev}, fecha:{fecha}})"
                    #"MERGE (p)-[:escribe]-> (x:Person {autor:{autor}})",  { 'o': dic['titulo'], 'autor': reviews['usuario'], 'val': reviews['valoracion'], 'rev': reviews['review'],'fecha': reviews['fecha']})
        for row in r:
            print dir(row)

def escribirTxt(dic, appNum):
    title = PAGE_TOP_60.split("category")[-1].split("/")
    name = ""
    for e in title:
        e = e.split("?")
        name = name + e[0]
    #print "nombre del archivo : ", name+".txt"
    if NUEVO and appNum == 0:
        archivo = open(name+'.json', 'w')
        archivo.write("[")
    else:
        archivo = open(name+'.json', 'a')
    jsonDic = json.dumps(dic)
    archivo.write(jsonDic)
    if appNum == NUMERO_APPS-1:
        archivo.write(']')
    else:
        archivo.write(',')

    archivo.close()

def escribirTxtBonito(dic, appNum):
    title = PAGE_TOP_60.split("category")[-1].split("/")
    name = ""
    for e in title:
        e = e.split("?")
        name = name + e[0]
    name+= "EASY"
    #print "nombre del archivo : ", name+".txt"
    if NUEVO and appNum == 0:
        archivo = open(name+'.json', 'w')
        archivo.write("[\n")
    else:
        archivo = open(name+'.json', 'a')
    jsonDic = json.dumps(dic)
    archivo.write("id: " + json.dumps(dic["id"]) + "\n")
    archivo.write("\ttitulo: " + json.dumps(dic["titulo"]) + "\n")
    archivo.write("\tdescripcion: " + json.dumps(dic["descripcion"]) + "\n")
    archivo.write("\tdescargas: " + json.dumps(dic["descargas"]) + "\n")
    for index, review in enumerate(dic["reviews"]):
        archivo.write(str(index) + " :")
        archivo.write("\t\tAutor: " + json.dumps(review["usuario"]) + "\n")
        archivo.write("\t\tValoracion: " + json.dumps(review["valoracion"]) + "\n")
        archivo.write("\t\tReview: " + json.dumps(review["review"]) + "\n")
        if "dev-reply" in review:
            archivo.write("\t\tDev-Reply: " + json.dumps(review["dev-reply"]) + "\n")
        archivo.write("\t\tFecha: " + json.dumps(review["fecha"]) + "\n\n")
    #archivo.write(jsonDic)
    if appNum == NUMERO_APPS-1:
        archivo.write('\n]')
    else:
        archivo.write(',')
    archivo.close()


############ main #############
try:
    ids = scrapIdsApps(agregaGl(PAGE_TOP_60))[:NUMERO_APPS]
except:
    ids = scrapIdsApps(PAGE_TOP_60)
    NUMERO_APPS = len(ids)
print ids
cant = len(ids)
contador = 0
for indx,id in enumerate(ids):
    print "(app ", indx, " de ", cant-1, ") id: ", id
    getReviews(NUM_REVIEWS, id, contador)
    contador += 1