package trafficreport;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by saipkri on 18/08/16.
 */
public class DataStore {
    private static MongoClient mongo;

    static {
        mongo = new MongoClient("localhost", 27017);

    }



    public static void save(final String roadName, final Integer noOfVehicles) {
        DB db = mongo.getDB("TrafficReport");
        DBCollection table = db.getCollection("Highways");
        BasicDBObject query = new BasicDBObject();

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.put(roadName, noOfVehicles);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        if (table.findOne(query) != null) {
            table.update(query, updateObj);
        } else {
            BasicDBObject d = new BasicDBObject();
            d.put(roadName, noOfVehicles);
            table.insert(d);
        }
    }

    public static Map<String, Object> trafficReport() {
        DB db = mongo.getDB("TrafficReport");
        DBCollection table = db.getCollection("Highways");
        return table.findOne().toMap();
    }
}
