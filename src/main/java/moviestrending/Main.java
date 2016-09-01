package moviestrending;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by saipkri on 18/08/16.
 */
public class Main {

    public static void main(String[] args) throws Exception {
        String participantEventName = (args.length == 2) ? args[1] : "ABoringMovie";

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, true);


        TopologyBuilder builder = new TopologyBuilder();
        WatchersSpout watchersSpout = new WatchersSpout(participantEventName);
        EventsRealTimeTrendSpout eventsRealTimeTrendSpout = new EventsRealTimeTrendSpout();

        WatchersIdentityVerificationBolt watchersIdentityVerificationBolt = new WatchersIdentityVerificationBolt();
        GenderTrendingBolt genderTrendingBolt = new GenderTrendingBolt();
        NationalityTrendingBolt nationalityTrendingBolt = new NationalityTrendingBolt();
        PlaceOfBirthTrendingBolt placeOfBirthTrendingBolt = new PlaceOfBirthTrendingBolt();
        LoggingBolt loggingBolt = new LoggingBolt();

        builder.setSpout("watchersSpout", watchersSpout);
        builder.setBolt("watchersIdentityVerificationBolt",
                watchersIdentityVerificationBolt)
                .shuffleGrouping("watchersSpout");

        builder.setBolt("genderTrendingBolt",
                genderTrendingBolt)
                .fieldsGrouping("watchersIdentityVerificationBolt", new Fields("gender"));

        builder.setBolt("nationalityTrendingBolt",
                nationalityTrendingBolt).fieldsGrouping("watchersIdentityVerificationBolt", new Fields("nationality"));

        builder.setBolt("placeOfBirthTrendingBolt",
                placeOfBirthTrendingBolt).fieldsGrouping("watchersIdentityVerificationBolt", new Fields("placeOfBirth"));

        builder.setSpout("eventsRealTimeTrendSpout", eventsRealTimeTrendSpout);
        builder.setBolt("loggingBolt",
                loggingBolt).shuffleGrouping("eventsRealTimeTrendSpout");

        if (args != null && args.length > 0) {
            System.out.println("Submitting...");
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            System.out.println("Submitted...");
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("FutureOfPancakesTrending", config, builder.createTopology());
            Thread.sleep(100000);
            cluster.shutdown();
        }
        Thread.sleep(100000);
    }
}
