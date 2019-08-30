#!/bin/bash
sudo cp /home/pda/eclipse-workspace/Project/Final/part-000*  /usr/local/hadoop/Project/
sudo sed -e 's/\s\+/,/g' part-00000 > Final.txt
sudo chown -R hduser:hadoopgroup Final.txt

