package com.mikitebeka.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class ZipFileRecordReader implements RecordReader<Text, Text> {
    private FileSystem fs = null;
    private FSDataInputStream fsin = null;
    private ZipLineIterator iter = null;

    public ZipFileRecordReader(InputSplit inputSplit, JobConf job,
            Reporter reporter) throws IOException {
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        fs = path.getFileSystem(job);

        // Open the stream
        fsin = fs.open(path);
        iter = new ZipLineIterator(fsin);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public boolean next(Text key, Text value) throws IOException {
        if (iter == null) {
            throw new IOException();
        }

        if (!iter.hasNext()) {
            return false;
        }

        key.set(iter.getName());
        value.set(iter.next());
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public Text createKey() {
        return new Text();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public Text createValue() {
        return new Text();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        if (iter == null) {
            throw new IOException();
        }

        return iter.getPos();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        if (iter == null) {
            return;
        }

        iter.close();
        iter = null;
        fsin.close();
        fsin = null;
        fs.close();
        fs = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        return iter.getProgress();
    }
}
