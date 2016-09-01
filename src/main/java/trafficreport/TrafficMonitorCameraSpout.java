package trafficreport;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Starting Point that streams the customer identity every 100 ms.
 *
 * @author Sai Kris.
 */
public class TrafficMonitorCameraSpout extends BaseRichSpout {
    private String[] roads = new String[]{"NH41", "NH1", "NH3", "NH4", "NH5", "NH6", "NH14", "EC41"};

    private SpoutOutputCollector _collector;
    private static final Random RANDOM = new Random();

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("road", "vehicleIn", "vehicleOut"));
    }

    @Override
    public void open(final Map map, final TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            Utils.sleep(1000);
            boolean vehicleIn = System.currentTimeMillis() % 2 == 0 || System.currentTimeMillis() % 5 == 0;
            int numberOfVehicles = RANDOM.nextInt(10);
            String road = roads[RANDOM.nextInt(roads.length)];
            _collector.emit(new Values(road, vehicleIn ? numberOfVehicles : 0, !vehicleIn ? numberOfVehicles : 0));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
