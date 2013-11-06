#! /usr/bin/env python

#IMPORTS
from __future__ import division
from fractions import Fraction
import sys

#VARIABLES GLOBALES
speed = 0           #Velocidad para cada momento (pendiente)
results = []        #lista con los resultados de GPS
queues = []         #lista de colas
packets = []        #lista con los paquetes
n_packets = 0       #numero de paquetes
max_arrive_time = 0 #ultimo tiempo de llegada
time = 0            #tiempo global
active_packets = [] #paquetes activos

##########################################
######
######            FUNCIONES
######
##########################################

def get_active_queues(t):
    count = 0
    for x in xrange(0, len(t)):
        if t[x] != 0:
            count+=1
    return count

def calc_active_queues(ap):
    u_queue = [0]*len(queues)
    count = 0
    for x in xrange(0, len(ap)):
        p = ap[x]
        if u_queue[p['q']] == 0:
            count += 1
            u_queue[p['q']] = 1

    return count

def recalc_estimated_times(ap, aq):

    ap_aux = list(ap)
    u_queue = [0]*len(queues)
    global speed

    for x in xrange(0, len(ap_aux)):
        p = ap_aux[x]
        if u_queue[p['q']] == 0:
            u_queue[p['q']] = 1

            p['s'] += speed

            p['ts'] = (time-p['tl']) + (p['mb']-p['s'])/speed

            if float(p['s']) == p['mb']:
                ap.remove(p)
                result = {
                    'q' : p['q'],
                    'n' : p['n'],
                    'tl': p['tl'],
                    'ts': p['ts'],
                    'gps':time+1,
                    'mb' : p['mb']
                }
                results.append(result)
            
            
##########################################
######
######         FIN FUNCIONES
######
##########################################


#Argumentos
if len(sys.argv) < 3:
    print 'Parametros incorrectos, ejecutar el programa como se indica:\n'
    print sys.argv[0] + " fichero_cola1.txt fichero_cola2.txt ...\n"
    print 'El formato de entrada es:'
    print 't_llegada\tMB\n'
    print 'El formato de salida es:'
    print '#paquete\t#cola\tt_llegada\tWFQ\tGPS'
    exit(0)


#LISTA CON LAS COLAS
queues_names = []

for x in xrange(1, len(sys.argv)):
    queues_names.append(sys.argv[x])

#PROCESAR FICHEROS
for i in xrange(0, len(queues_names)):
    queues.append([])                             #crear una lista para cada cola
    actual_queue = queues[i]                      #cola actual a leer (fichero)
    f = open(queues_names[i], 'r').readlines()    #leer lineas del fichero i
    for l in f:                                   #para cada linea
        l = l.split()                             #Splitear linea
        n_packets+=1                              #contador de paquetes
        packet = {                                #Cada paquete esta compuesto por
            "n" : n_packets,                      #el numero de paquete
            "mb" : int(l[1]),                     #los MB de datos del paquete
            "tl" : int(l[0]),                     #tiempo de llegada
            "q" : i,                              #numero de cola
            "ts" : 0,                             #tiempo estimado de salida
            "s" : 0                               #MB servidos
        }
        packets.append(packet)                    #meter paquete en lista de paquetes
        actual_queue.append(packet)               #meter paquete en cola actual
        max_arrive_time = max(int(l[0]), max_arrive_time) #ultimo tiempo de llegada

# CREAR ARRAY DE MOMENTOS CON ESTADOS DE LAS COLAS
# EL FORMATO ES
# 
# t0{
#    cola0: ...
#    cola1: ...
#    colan: ... 
#  },
# t1{
#    cola0: ...
#    cola1: ...
#    colan: ... 
#  },
#
times = [dict() for x in range(max_arrive_time+1)] #array para todos los momentos en el tiempo
for t in xrange(0, max_arrive_time+1):             #asignar diccionario a cada uno inicializado a 0
    for i in xrange(len(queues)):
        times[t][i] = 0

#para cada paquete
for p in packets: 
    times[p['tl']][p['q']] = p #meter paquete en el momento de tiempo de su llegada y su cola

#CALCULO DE GPS
#para cada momento de tiempo
for t in times:
    active_queues = get_active_queues(t)                                    #No de colas activas
    aux_time = time
    flag = 0
    for q in xrange(0, len(queues)):                                        #Para cada cola
        p = t[q]                                                            
        if p != 0:                                                          #si esta activa
            flag = 1
            time = p['tl']                                                  #t llegada
            p['ts'] = time + p['mb']*active_queues                          #tiempo estimado salida
            active_packets.append(p)                                        #meter en lista activos
            active_queues = calc_active_queues(active_packets)              #recalc colas activas
            speed = Fraction(1,active_queues) if active_queues != 0 else 0  #velocidad (pendiente)
    recalc_estimated_times(active_packets, active_queues)                   #recalc tiempos estimados
    if (aux_time == time and flag == 0) or time==0:                         #para el caso en que nunca
        time+=1                                                             #haya dos colas activas

#Una vez no llegan mas paquetes
while len(active_packets):
    time+=1                                                 #el tiempo sigue avanzando
    aq = calc_active_queues(active_packets)                 #colas activas
    speed = Fraction(1,aq) if aq != 0 else 0                #recalcular velocidad
    recalc_estimated_times(active_packets, active_queues)   #recalcular tiempos estimados

results = sorted(results, key=lambda r: r['n'])             #ordenar por numero de paquete

#CABECERA SALIDA
print '# q t w s'

wfq = 0     #wfq tiempo de salida
last_q = -1 #ultima cola usada
while len(results): #Mientras haya resultados en la lista
    for r in results: #para todos los resultados
        #Si el resultado es de la cola siguiente a la procesada
        if r['q'] > last_q or (last_q == len(queues)-1 and r['q'] == 0) :
            last_q = r['q'] #actualizar indicador de ultima cola
            #calcular tiempo de salida wfq. el anterior mas los megas si el anterior es superior
            #al tiempo de llegada. Es decir si el paquete tuvo que esperar para ser procesado
            wfq = wfq + r['mb'] if wfq > r['tl'] else r['tl'] + r['mb']
            print '{0} {1} {2} {3} {4}'.format(r['n'], r['q'], r['tl'], wfq, r['gps'])
            results.remove(r) #eliminar resultado de la lista


