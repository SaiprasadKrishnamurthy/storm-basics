package eventstrending;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class LoggingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        System.out.println("\n\n\n");
        Map<String, Map<String, Long>> allTrends = (Map<String, Map<String, Long>>) tuple.getValue(0);
        allTrends.forEach((key, value) -> {
            System.out.println("Trend name: " + key);
            System.out.println("-------------------------");
            value.forEach((k1, v1) -> {
                System.out.println("\t\t " + k1 + " = " + v1);
            });
        });
        System.out.println("\n\n\n");
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        // Nothing goes here.
    }
}
