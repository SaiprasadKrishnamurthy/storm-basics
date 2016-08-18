package eventstrending;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by saipkri on 18/08/16.
 */
public class ParticipantsIdentityVerificationBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(final Tuple tuple) {
        outputCollector.emit(tuple, new Values(tuple.getString(0), tuple.getString(1), tuple.getLong(2), tuple.getString(3), tuple.getString(4), tuple.getString(5)));
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fullName", "gender", "dateOfBirth", "nationality", "placeOfBirth", "passportNumber"));
    }
}
