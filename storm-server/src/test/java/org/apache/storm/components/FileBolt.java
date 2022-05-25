package org.apache.storm.components;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class FileBolt extends BaseRichBolt {

        private File file;

        public FileBolt(File file) {
            this.file = file;
        }

        @Override
        public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {}

        @Override
        public void execute(Tuple input) {
            try (FileWriter writer = new FileWriter(this.file, true)) {
                int received = (int) input.getValue(0);
                writer.write(received + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
