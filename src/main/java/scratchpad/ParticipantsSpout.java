package scratchpad;

import com.thedeanda.lorem.LoremIpsum;
import org.apache.storm.shade.org.apache.commons.lang.RandomStringUtils;
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
public class ParticipantsSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private static final LoremIpsum GENERATOR = LoremIpsum.getInstance();
    private static final Random RANDOM = new Random();

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("fullName", "gender", "dateOfBirth", "nationality", "placeOfBirth", "passportNumber"));
    }

    @Override
    public void open(final Map map, final TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        boolean isMale = System.currentTimeMillis() % 2 == 0;
        String fullName = isMale ? GENERATOR.getNameMale() : GENERATOR.getNameFemale();
        String gender = isMale ? "M" : "F";
        int yearsSubtract = RANDOM.nextInt(50);
        long dob = System.currentTimeMillis() - (yearsSubtract * 365 * 24 * 60 * 60 * 1000) - (RANDOM.nextInt(365) * 365 * 24 * 60 * 60 * 1000);
        String nationality = GENERATOR.getCountry();
        String placeOfBirth = GENERATOR.getCity();
        String passportNumber = RandomStringUtils.randomAlphanumeric(20);
        _collector.emit(new Values(fullName, gender, dob, nationality, placeOfBirth, passportNumber));
    }
}
