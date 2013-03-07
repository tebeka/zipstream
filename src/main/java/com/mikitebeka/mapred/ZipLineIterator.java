package com.mikitebeka.mapred;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class ZipLineIterator implements Iterator<String> {

    private ZipInputStream zin = null;
    private String name;
    private int pos = 0;
    private long size;
    private String nextLine;
    private BufferedReader reader;

    public ZipLineIterator(InputStream in) throws IOException {
        zin = new ZipInputStream(in);
        ZipEntry entry = zin.getNextEntry();
        if (entry == null) {
            return;
        }

        size = entry.getSize();
        name = entry.getName();
        reader = new BufferedReader(new InputStreamReader(zin));

        nextLine = reader.readLine();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return nextLine != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#next()
     */
    @Override
    public String next() {
        String line = nextLine;
        try {
            nextLine = reader.readLine();
        } catch (IOException e) {
            nextLine = null;
        }

        return line;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    public void close() throws IOException {
        if (zin == null) {
            return;
        }
        zin.close();
    }

    public float getProgress() {
        if (size == 0) {
            return 1.0f;
        }

        return (float) (((double) pos) / size);
    }

    /**
     * @return the pos
     */
    public int getPos() {
        return pos;
    }

}
