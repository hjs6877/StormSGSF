package kr.ac.korea;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.mortbay.jetty.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

/**
 * Created by ideapad on 2015-11-22.
 */
public class SingleSpoutForSocketClient extends BaseRichSpout{
    private static final Logger logger = LoggerFactory.getLogger(SingleSpoutForSocketClient.class);

    private SpoutOutputCollector collector;

    private ServerSocket serverSocket;
    String clientMessage;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("clientMessage"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        Thread t = new Thread(new ClientHandleRunnable());
        t.start();
    }

    public void nextTuple() {
        this.collector.emit(new Values(clientMessage));
    }

    public class ClientHandleRunnable implements Runnable {
        BufferedReader br;
        ServerSocket serverSocket;


        @Override
        public void run() {
            /**
             * 클라이언트 소켓에서 들어온 스트림을 처리
             */
            try {
                serverSocket = new ServerSocket(5000);
                while(true){
                    logger.info("waiting client connection..");

                    Socket clientSocket = serverSocket.accept();
                    InputStreamReader isr = new InputStreamReader(clientSocket.getInputStream());
                    br = new BufferedReader(isr);

                    logger.info("got a connection!!");

                    String message;

                    while((message = br.readLine()) != null){
                        clientMessage = message;
                    }
                    clientMessage = null;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }



        }
    }
}
