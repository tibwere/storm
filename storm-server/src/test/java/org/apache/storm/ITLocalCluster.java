package org.apache.storm;

import org.apache.storm.components.FileBolt;
import org.apache.storm.components.FileSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class ITLocalCluster implements Serializable {

    private File spoutTuples;
    private File boltTuples;

    private static final String BOLT_PREFIX = "bolt";
    private static final String SPOUT_PREFIX = "spout";
    private static final String FILE_SUFFIX = ".tmp";
    private final String SPOUT_STREAM = "spout";
    private final String BOLT_STREAM = "bolt";
    private final String TOPOLOGY_NAME = "test-topology";
    private final int RUNTIME_MSEC = 10000;

    @Before
    public void setUpEnvironment() {
        try {
            this.spoutTuples = File.createTempFile(BOLT_PREFIX, FILE_SUFFIX);
            this.boltTuples = File.createTempFile(SPOUT_PREFIX, FILE_SUFFIX);
        } catch (IOException e) {
            Assert.fail("The test requires the creation of files to work");
        }
    }

    @After
    public void tearDownEnvironment() {
        this.boltTuples.delete();
        this.spoutTuples.delete();
    }

    @Test
    public void testBoltAndSpoutShouldCommunicate() throws Exception {
        try (LocalCluster cluster = new LocalCluster()) {

            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout(SPOUT_STREAM, new FileSpout(this.spoutTuples), 1);
            builder.setBolt(BOLT_STREAM, new FileBolt(this.boltTuples), 1).shuffleGrouping(SPOUT_STREAM);

            cluster.submitTopology(TOPOLOGY_NAME, new Config(), builder.createTopology());
            Utils.sleep(RUNTIME_MSEC);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();

            Assert.assertTrue("The files should contain the same list of values", filesAreTheSame());
        }
    }

    private boolean filesAreTheSame() throws IOException {
        BufferedReader bReader = new BufferedReader(new FileReader(this.boltTuples));
        BufferedReader sReader = new BufferedReader(new FileReader(this.spoutTuples));

        String line1, line2;
        while ((line1  = bReader.readLine()) != null) {
            line2 = sReader.readLine();

            if (line2 == null || !line1.equals(line2))
                return false;
        }

        return (sReader.readLine() == null);
    }
}

