package org.apache.storm.components;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class FileSpout extends BaseRichSpout {
    private File file;

    public FileSpout(File file) {
        this.file = file;
    }

    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        Random r = new Random();
        int nextVal = r.nextInt();

        try (FileWriter writer = new FileWriter(this.file, true)) {
            writer.write(nextVal + "\n");
            this.collector.emit(new Values(nextVal));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("random-int"));
    }
}
