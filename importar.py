#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import psycopg2
import psycopg2.extensions

pasta = '/tmp/importacao'
arquivos = os.listdir(pasta)

try:
    conn = psycopg2.connect("dbname='fotosensores' user='fotosensores' host='localhost' password='1234'")
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
except:
    print "Erro ao conectar ao banco de dados"
    exit()

c = conn.cursor()
c.execute("set client_encoding to latin1")

for a in arquivos:
    nome_tabela = a.split(".")[0]
    try:   
        c.execute("""select * from log_importacao where tabela = '%s'""" % (nome_tabela))
        dados_tabela = c.fetchone()
        if dados_tabela == None or dados_tabela[5] == False:
            c.execute("""alter table %s disable trigger all""" % (nome_tabela))
            c.execute("""insert into log_importacao(tabela,inicio) values ('%s',current_timestamp)""" % (nome_tabela)) 
            c.execute("""copy %s from '%s/%s' csv""" % (nome_tabela,pasta,a) )
            c.execute("""select count(*) from %s""" % (nome_tabela))
            num_linhas = int(c.fetchone()[0])
            c.execute("""update log_importacao set linhas = %d, sucesso = true, fim = current_timestamp where tabela = '%s'""" % (num_linhas,nome_tabela))
    except psycopg2.Error as erro:
        print """Erro co copiar tabela %s""" % (nome_tabela)
        c.execute("""update log_importacao set fim = current_timestamp, erro = '%s' where tabela = '%s' """ % (erro, nome_tabela)) 


