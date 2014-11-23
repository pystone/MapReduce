
import java.util.Iterator;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import mapreduce.MRBase;
import mapreduce.PairContainer;

public class Twitter implements MRBase {
    @Override
    public void map(String key, String value, PairContainer output) {
        Scanner scan = new Scanner(value);
        scan.useDelimiter("\n");
        String line = null;
        
        while(scan.hasNext()) {
            line = scan.next();
            if (line != null && line.isEmpty()) {
                continue;
            }
            
            JSONObject json = null;
            try {
                json = new JSONObject(line);
                String userID = extractUserID(json);
                JSONArray medias = extractMedias(json);
                int numOfPhoto = 0;
                if (medias != null) {
                    for (int i = 0; i < medias.length(); i++) {
                        JSONObject media = medias.getJSONObject(i);
                        if (media != null) {
                            String type = media.getString("type");
                            if (type != null && type.equals("photo")) {
                                numOfPhoto++;
                            }
                        }
                    }
                }
                output.emit(userID, String.valueOf(numOfPhoto));
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(String key, Iterator<String> values, PairContainer output) {
        Integer sum = 0;
        while (values.hasNext()) {
            sum += Integer.parseInt(values.next());
        }
        output.emit(key, sum.toString());
        
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public JSONArray extractMedias(JSONObject json) {
        JSONArray medias = null;
        try {
            medias = json.getJSONObject("entities").getJSONArray("media");
        } catch (JSONException e) {
            return null;
        }
        return medias;
    }

    public String extractUserID(JSONObject json) throws JSONException {
        String id = json.getJSONObject("user").getString("id_str");
        return id;
    }
    
    public String extractTweetID(JSONObject json) throws JSONException {
        String id = json.getString("id_str");
        return id;
    }
}