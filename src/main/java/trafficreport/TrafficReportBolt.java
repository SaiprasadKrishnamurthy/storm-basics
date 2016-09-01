package trafficreport;

import moviestrending.DataStore;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by saipkri on 18/08/16.
 */
public class TrafficReportBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private int boltId;
    private Map<String, Long> genderCounts = new HashMap<>();
    private AtomicInteger vehicleIn = new AtomicInteger(0);
    private AtomicInteger vehicleOut = new AtomicInteger(0);
    private AtomicInteger totalVehicles = new AtomicInteger(0);


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.boltId = topologyContext.getThisTaskId();
    }

    @Override
    public void execute(final Tuple tuple) {
        String road = tuple.getStringByField("road");
        int in = tuple.getIntegerByField("vehicleIn");
        int out = tuple.getIntegerByField("vehicleOut");
        int currentIn = vehicleIn.getAndAdd(in);
        int currentOut = vehicleOut.getAndAdd(out);

        int currTotal = currentIn - currentOut;
        if(currTotal < 0) {
            currTotal = 0;
        }
        totalVehicles.getAndSet(currTotal);
        trafficreport.DataStore.save(road, totalVehicles.intValue());
        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("road", "vehicleIn", "vehicleOut"));
    }
}
