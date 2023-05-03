#!/bin/sh

mysql -e "DROP DATABASE IF EXISTS archivesspace; CREATE DATABASE archivesspace DEFAULT CHARACTER SET utf8; CREATE USER IF NOT EXISTS 'as'@'localhost' IDENTIFIED BY 'as123'; GRANT ALL ON archivesspace.* to 'as'@'localhost'; ALTER USER 'as'@'localhost' IDENTIFIED WITH mysql_native_password BY 'as123';"
zcat "$1" | mysql archivesspace
/opt/archivesspace/scripts/password-reset.sh admin admin
