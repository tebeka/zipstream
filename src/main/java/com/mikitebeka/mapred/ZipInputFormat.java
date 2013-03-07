package com.mikitebeka.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

class ZipInputFormat extends FileInputFormat<Text, Text> {
    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> getRecordReader(InputSplit genericSplit,
            JobConf job, Reporter reporter) throws IOException {
        return new ZipFileRecordReader(genericSplit, job, reporter);
    }

}
