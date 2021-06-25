#!/bin/sh

dbmcli db_create $1 dbm,dbm
dbmcli -d $1 -u dbm,dbm -i hoteldb.ini

