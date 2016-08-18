package scratchpad;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class NationalityTrendingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, Long> nationalityCounts = new HashMap<>();
    private int boltId;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        nationalityCounts.computeIfAbsent(tuple.getString(3), key -> 0L);
        nationalityCounts.computeIfPresent(tuple.getString(3), (key, count) -> count + 1L);
        DataStore.save("nationalityTrend", nationalityCounts);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fullName", "gender", "dateOfBirth", "nationality", "placeOfBirth", "passportNumber"));
    }
}
