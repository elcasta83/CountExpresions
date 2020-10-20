#!/usr/bin/python
 # -*- coding: utf-8 -*- 
#Cuenta las expresions más comunes de un texto pasado por un archivo

from pyspark import SparkContext
import re
def dospalabras(listpal,dospal):
	"""Función que convierte a una lista de dos palabras"""
	i=0
	while i<=(len(listpal)-2):
		dospal[i]=listpal[i]+' '+listpal[i+1]
		#print("Intento número: %d" % i +" resultado: " +dospal[i])
		i+=1
	return dospal
def trespalabras(listpal,trespal):
	"""Función que convierte a una lista de tres palabras"""
	j=0
	while j<=(len(listpal)-3):
		trespal[j]=listpal[j]+' '+listpal[j+1]+' '+listpal[j+2]
		#print("Intento número: %d" % j +" resultado: " +trespal[j])
		j+=1
	return trespal
def cuatropalabras(listpal,cuatropal):
	"""Función que convierte a una lista de cuatro palabras"""
	j=0
	while j<=(len(listpal)-4):
		cuatropal[j]=listpal[j]+' '+listpal[j+1]+' '+listpal[j+2]+' '+listpal[j+3]
		#print("Intento número: %d" % j +" resultado: " +cuatropal[j])
		j+=1
	return cuatropal
def cincopalabras(listpal,cincopal):
	"""Función que convierte a una lista de cinco palabras"""
	j=0
	while j<=(len(listpal)-5):
		cincopal[j]=listpal[j]+' '+listpal[j+1]+' '+listpal[j+2]+' '+listpal[j+3]+' '+listpal[j+4]
		#print("Intento número: %d" % j +" resultado: " +cincopal[j])
		j+=1
	return cincopal
#Configuramos el SparkContext como local
sc = SparkContext("local", "Simple App")
#Cargamos el fichero en el rdd qui
qui = sc.textFile("quijote.txt")
#Pasamos todo el texto a un rdd en el que aparecen las palabras
palabras = (qui
	.flatMap(lambda linea: re.compile("\W").split(linea))
	.filter(lambda palabra: palabra != '')
	.map(lambda palabra: palabra.lower()))
"""Pasamos a un rdd que pasa  auna tupla de palabra y número, haciendo un reduce que cuenta las veces que aparece cada palabra"""
histograma = (palabras
	.map(lambda palabra : (palabra,1))
	.reduceByKey(lambda x,y: x+y))
#Pasamos a un RDD que almacena las más frecuentes
masFrecuentes = (histograma
	.sortBy(lambda v: -v[1]).take(10))
print("AQUI LAS PALABRAS MAS FRECUENTES")
for p in masFrecuentes:
	print (p[0] + ": %d" % p[1])
#Almacenamos el RDD palabras en cache para evitar volver a realizar lso cálculos
palabras.cache()
#Llamamos a las funciones para calcular las expresiones más comunes de dos, tres, cuatro o cinco palabras
#Las pasamos a listas para tratarlas con el .collect()
dospal=trespala= cuatropala=cincopala=palabras.collect()
dospal=dospalabras(palabras.collect(),palabras.collect())
dospalrdd=sc.parallelize(dospal)
histodos=(dospalrdd.map(lambda p:(p,1)).reduceByKey(lambda x,y: x+y))
masdos=(histodos.sortBy(lambda v:-v[1]).take(25))
print("AQUI LAS DOS PALABRAS MAS FRECUENTES: ",masdos)
for p in masdos:
	print(p[0] + ":%d" %p[1])
trespala=trespalabras(palabras.collect(),palabras.collect())
trespalrdd=sc.parallelize(trespala)
histotres=(trespalrdd.map(lambda p:(p,1)).reduceByKey(lambda x,y: x+y))
mastres=(histotres.sortBy(lambda v:-v[1]).take(25))
cuatropala=cuatropalabras(palabras.collect(),palabras.collect())
cuatropalrdd=sc.parallelize(cuatropala)
histo4=(cuatropalrdd.map(lambda p:(p,1)).reduceByKey(lambda x,y: x+y))
mas4=(histo4.sortBy(lambda v:-v[1]).take(25))
cincopala=cincopalabras(palabras.collect(),palabras.collect())
cincopalrdd=sc.parallelize(cincopala)
histo5=(cincopalrdd.map(lambda p:(p,1)).reduceByKey(lambda x,y: x+y))
mas5=(histo5.sortBy(lambda v:-v[1]).take(25))
print("AQUI LAS cinco PALABRAS MAS FRECUENTES: ",mas5)
for p in mas5:
	print(p[0] + ":%d" %p[1])

