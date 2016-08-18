package eventstrending;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by saipkri on 18/08/16.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);


        TopologyBuilder builder = new TopologyBuilder();
        ParticipantsSpout participantsSpout = new ParticipantsSpout();
        EventsRealTimeTrendSpout eventsRealTimeTrendSpout = new EventsRealTimeTrendSpout();

        ParticipantsIdentityVerificationBolt participantsIdentityVerificationBolt = new ParticipantsIdentityVerificationBolt();
        GenderTrendingBolt genderTrendingBolt = new GenderTrendingBolt();
        NationalityTrendingBolt nationalityTrendingBolt = new NationalityTrendingBolt();
        PlaceOfBirthTrendingBolt placeOfBirthTrendingBolt = new PlaceOfBirthTrendingBolt();
        LoggingBolt loggingBolt = new LoggingBolt();

        builder.setSpout("participantsSpout", participantsSpout);
        builder.setBolt("participantsIdentityVerificationBolt",
                participantsIdentityVerificationBolt, 12).shuffleGrouping("participantsSpout");

        builder.setBolt("genderTrendingBolt",
                genderTrendingBolt).fieldsGrouping("participantsIdentityVerificationBolt", new Fields("gender"));

        builder.setBolt("nationalityTrendingBolt",
                nationalityTrendingBolt).fieldsGrouping("participantsIdentityVerificationBolt", new Fields("nationality"));

        builder.setBolt("placeOfBirthTrendingBolt",
                placeOfBirthTrendingBolt).fieldsGrouping("participantsIdentityVerificationBolt", new Fields("placeOfBirth"));

        builder.setSpout("eventsRealTimeTrendSpout", eventsRealTimeTrendSpout);
        builder.setBolt("loggingBolt",
                loggingBolt).shuffleGrouping("eventsRealTimeTrendSpout");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("EventSeating", config, builder.createTopology());
        Thread.sleep(100000);

        cluster.shutdown();

    }
}
