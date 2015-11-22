package kr.ac.korea;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by ideapad on 2015-11-22.
 */
public class TopologySGSF {
    private static final String SINGLE_SPOUT_FOR_SOCKET_CLIENT = "SINGLE_SPOUT_FOR_SOCKET_CLIENT";
    private static final String MESSAGE_ANALYZE_BOLT = "MESSAGE_ANALYZE_BOLT";
    private static final String SGSF_TOPOLOGY = "SGSF_TOPOLOGY";

    public static void main(String[] args){
        SingleSpoutForSocketClient singleSpoutForSocketClient = new SingleSpoutForSocketClient();
        MessageAnalizeBolt messageAnalizeBolt = new MessageAnalizeBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(SINGLE_SPOUT_FOR_SOCKET_CLIENT, singleSpoutForSocketClient);

        builder.setBolt(MESSAGE_ANALYZE_BOLT, messageAnalizeBolt).fieldsGrouping(SINGLE_SPOUT_FOR_SOCKET_CLIENT, new Fields("clientMessage"));

        Config config = new Config();

        if(args.length == 0){
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(SGSF_TOPOLOGY, config, builder.createTopology());
        }else{
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }
}
