package kr.ac.korea;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by ideapad on 2015-11-22.
 */
public class MessageAnalizeBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(MessageAnalizeBolt.class);

    private OutputCollector collector;

    private Notifier notifier;

    private Map<String, String> sgsfMessageMap;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        notifier = new Notifier();
        sgsfMessageMap = new HashMap<String, String>();
    }

    @Override
    public void execute(Tuple tuple) {
        String clientMessage = tuple.getStringByField("clientMessage");


        if(clientMessage != null){
//            logger.info("## Now clientMessage in MessageAnalizeBolt: " + clientMessage);

            String[] clientMessages = clientMessage.split(",");
            double dB = Double.parseDouble(clientMessages[0]);
            String latitude = clientMessages[1];
            String longitude = clientMessages[2];
            String alertMessage = "Alert message in SGSF";

            String sgsfMessage = latitude + "," + longitude + "," + alertMessage;
            /**
             * Amplitude가 15,000 이상 값이면 푸시 알림 메시지를 전송한다.
             */
            if(dB >= 18000.0){
                if(!sgsfMessage.equals(sgsfMessageMap.get("sgsfMessage"))){
                    logger.info("## send sgsfMessage to another member: " + sgsfMessage);
                    notifier.sendMessage(sgsfMessage);

                    String response = notifier.readGcmResponse();
                    logger.info("## read response message: " + response);

                    sgsfMessageMap.put("sgsfMessage", sgsfMessage);
                }

            }
        }else{
            logger.info("## clientMessage in MessageAnalizeBolt: " + "There is no message");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
