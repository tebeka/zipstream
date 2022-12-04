Zip Reader for Hadoop Streaming
===============================

*I no longer maintain this repository, if you want to take over - drop me a line.*

This is a reader that will return (filename, line) key value pairs for a zip
file in Hadoop streaming.

Note that currently only the first file in the zip will be processed, if you
want more - submit a pull request :)

Usage
=====

::
    
    #!/bin/bash
    # Unzip a file in HDFS

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


FAQ
===

| Q. Why not http://cotdp.com/2012/07/hadoop-processing-zip-files-in-mapreduce/?
| A. It uses the old(?) `mapreduce` API and doesn't work with CDH4

| Q. Where does this project live?
| A.  https://github.com/tebeka/zipstream


