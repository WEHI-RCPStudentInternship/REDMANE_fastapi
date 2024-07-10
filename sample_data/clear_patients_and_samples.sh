#!/bin/bash

sqlite3 ../data/data_redmane.db "delete from patients;"
sqlite3 ../data/data_redmane.db "delete from samples;"
