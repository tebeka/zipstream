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
    private String name = null;
    private String nextLine = null;
    private BufferedReader reader = null;
    private int pos = 0;

    public ZipLineIterator(InputStream in) throws IOException {
        zin = new ZipInputStream(in);

        nextEntry();
    }

    private void nextEntry() {
        nextLine = null;

        if (entry != null) {
            zin.closeEntry();
        }

        entry = zin.getNextEntry();
        if (entry == null) {
            return;
        }

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
        pos += line.length();

        try {
            nextLine = reader.readLine();
        } catch (IOException e) {
            nextEntry();
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
        // FIXME
        return 0.5f;
    }

    /**
     * @return the pos
     */
    public int getPos() {
        return pos;
    }

}
