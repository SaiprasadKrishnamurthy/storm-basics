package moviestrending;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
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


    private static ConcurrentHashMap<String, Map<String, Long>> trends = new ConcurrentHashMap<>();

    public static void save(final String movieName, final String trendType, final Map<String, Long> trend) {
        DB db = mongo.getDB("MovieTrends");
        DBCollection table = db.getCollection(movieName);
        BasicDBObject query = new BasicDBObject();
        query.put("trendType", trendType);

        BasicDBObject newDocument = new BasicDBObject();
        newDocument.putAll(trend);

        BasicDBObject updateObj = new BasicDBObject();
        updateObj.put("$set", newDocument);

        if (table.findOne(query) != null) {
            table.update(query, updateObj);
        } else {
            BasicDBObject d = new BasicDBObject();
            d.put("trendType", trendType);
            d.putAll(trend);
            table.insert(d);
        }
    }

    public static Map<String, Map<String, Long>> findAllTrends() {
        return trends;
    }

}
