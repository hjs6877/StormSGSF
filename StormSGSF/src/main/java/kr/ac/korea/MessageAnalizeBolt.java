package kr.ac.korea;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by ideapad on 2015-11-22.
 */
public class MessageAnalizeBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(SingleSpoutForSocketClient.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String clientMessage = tuple.getStringByField("clientMessage");
        logger.info("## clientMessage in MessageAnalizeBolt: " + clientMessage);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
