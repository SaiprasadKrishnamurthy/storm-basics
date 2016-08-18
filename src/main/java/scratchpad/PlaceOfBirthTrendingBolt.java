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
public class PlaceOfBirthTrendingBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private Map<String, Long> placeOfBirthCounts = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(final Tuple tuple) {
        placeOfBirthCounts.computeIfAbsent(tuple.getString(4), key -> 0L);
        placeOfBirthCounts.computeIfPresent(tuple.getString(4), (key, count) -> count + 1L);
        DataStore.save("placeOfBirthTrend", placeOfBirthCounts);
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fullName", "gender", "dateOfBirth", "nationality", "placeOfBirth", "passportNumber"));
    }
}
