#!/bin/bash
# Unzip a file in HDFS, example how to run zipmapred with streaming

case $1 in
    -h | --help ) echo "usage: $(basename $0) INDIR OUTDIR"; exit;;
esac

if [ $# -ne 2 ]; then
    $0 -h
    exit 1
fi

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -libjars zipmapred-1.0-SNAPSHOT.jar \
    -mapper /bin/cat \
    -reducer /bin/cat \
    -inputformat com.mikitebeka.mapred.ZipInputFormat \
    -input $1 -output $2

