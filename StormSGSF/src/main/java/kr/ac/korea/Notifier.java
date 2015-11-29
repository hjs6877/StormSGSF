package kr.ac.korea;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by ideapad on 2015-11-26.
 */
public class Notifier {
    private static final Logger logger = LoggerFactory.getLogger(Notifier.class);
    private static final String API_KEY = "AIzaSyDV3uCP_9cw05IreXiBe7ur0Q-0oUMSnuI";
    // Create connection to send GCM Message request.
    private URL url = null;
    private HttpURLConnection conn = null;

    public Notifier(){

        try {
            url = new URL("https://android.googleapis.com/gcm/send");

            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "key=" + API_KEY);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void sendMessage(String message){
        // Send GCM message content.
        OutputStream outputStream = null;

        JSONObject jGcmData = new JSONObject();
        JSONObject jData = new JSONObject();
        jData.put("message", message);
        jGcmData.put("to", "/topics/global");
        jGcmData.put("data", jData);
        try {
            outputStream = conn.getOutputStream();
            outputStream.write(jGcmData.toString().getBytes());
            logger.info("### send sgsfMessage to GCM: " + message);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String readGcmResponse(){
        // Read GCM response.
        InputStream inputStream = null;
        String resp = null;
        try {
            inputStream = conn.getInputStream();
            resp = IOUtils.toString(inputStream);
//            System.out.println(resp);
//            System.out.println("Check your device/emulator for notification or logcat for " +
//                    "confirmation of the receipt of the GCM message.");

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resp;
    }
}
