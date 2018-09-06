#!/bin/bash
i=$1
dt=$(date +"%Y%m%d")
while [[ $i < ${dt}  ]];do
    sh url_simhash_main.sh $i
    sh url_fastttext_main.sh $i  
    i=$(date -d "+1 day $i" +"%Y%m%d")
done

