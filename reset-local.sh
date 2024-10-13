#!/bin/bash
valkey-cli flushall
psql 'postgresql://localhost:5432/emojistats' -c 'truncate table posts; truncate table emojis;'
