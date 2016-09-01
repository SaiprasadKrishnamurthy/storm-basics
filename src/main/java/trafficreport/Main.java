package trafficreport;

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

        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, true);


        TopologyBuilder builder = new TopologyBuilder();
        TrafficMonitorCameraSpout trafficMonitorCameraSpout = new TrafficMonitorCameraSpout();
        TrafficReportBolt trafficReportBolt = new TrafficReportBolt();

        builder.setSpout("trafficMonitorCameraSpout", trafficMonitorCameraSpout);
        builder.setBolt("trafficReportBolt",
                trafficReportBolt)
                .setNumTasks(10)
                .fieldsGrouping("trafficMonitorCameraSpout", new Fields("road"));

        if (args != null && args.length > 0) {
            System.out.println("Submitting...");
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            System.out.println("Submitted...");
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("TrafficReport", config, builder.createTopology());
            Thread.sleep(100000);
            cluster.shutdown();
        }
        Thread.sleep(100000);
    }
}
