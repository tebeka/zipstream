package com.mikitebeka.mapred;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ZipLineReaderTest {

    private ZipLineIterator getZip(String file) throws IOException {
        InputStream in = getClass().getResourceAsStream("/" + file);
        return new ZipLineIterator(in);
    }

    public void TestSmallFile() throws IOException {
        ZipLineIterator it = getZip("3.zip");
        ArrayList<String> lines = new ArrayList<String>();
        while (it.hasNext()) {
            lines.add(it.next());
        }

        @SuppressWarnings("serial")
        ArrayList<String> expected = new ArrayList<String>() {
            {
                add("1");
                add("2");
                add("3");
            }
        };

        Assert.assertTrue(lines.equals(expected));
    }

    public void TestBigFile() throws IOException {
        return;
        // FIXME: Generate a big one
        
        /*
        ZipLineIterator it = getZip("events.zip");

        int i = 0;
        String line = null;

        for (; it.hasNext(); i++) {
            line = it.next();
        }
        Assert.assertEquals(i, 8330);
        String prefix = "...";
        Assert.assertTrue(line.startsWith(prefix));
        */
    }
}
