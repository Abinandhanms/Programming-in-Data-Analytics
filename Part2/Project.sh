#!/bin/bash
cat /usr/local/hadoop/Project/Raw.txt | sed 's/"//g' >> /usr/local/hadoop/Project/PIG.txt
sed 1d /usr/local/hadoop/Project/PIG.txt > /usr/local/hadoop/Project/Pig1.txt
pig -x local /usr/local/hadoop/Project/Project.pig
sudo rm -R /usr/local/hadoop/Project/Average.txt
cat /usr/local/hadoop/Project/Test/part-m-000* >> /usr/local/hadoop/Project/Average.txt
cat /usr/local/hadoop/Project/HIVE/part-m-000* >> /usr/local/hadoop/Project/Hive.txt

