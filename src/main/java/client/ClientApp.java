package client;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import trafficreport.DataStore;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static java.util.stream.Collectors.joining;
import static spark.Spark.get;

public class ClientApp {
    private static MongoClient mongo;

    static {
        mongo = new MongoClient("localhost", 27017);

    }

    public static void main(String[] args) {
        DB db = mongo.getDB("MovieTrends");
        get("/movies", (req, res) -> {
            res.type("application/json");
            res.status(200);
            System.out.println(db.getCollectionNames());
            Map<String, Object> map = new TreeMap<>();
            map.put("movies", db.getCollectionNames());
            return map;
        }, new JsonTransformer());

        get("/gender/:movie", (req, res) -> {
            String movie = req.params(":movie");
            BasicDBObject query = new BasicDBObject();
            query.put("trendType", "genderTrend");
            res.type("application/json");
            res.status(200);
            System.out.println(movie);
            DBObject d = db.getCollection(movie).findOne(query);
            System.out.println(d);
            Map m = d.toMap();
            List<Map<String, Object>> l = new ArrayList<>();
            m.forEach((k, v) -> {
                Map<String, Object> agg = new HashMap<String, Object>();
                if (!k.toString().startsWith("_") && !k.toString().startsWith("trend")) {
                    agg.put("name", k.toString());
                    agg.put("y", Long.parseLong(v.toString()));
                    l.add(agg);
                }
            });
            return l;
        }, new JsonTransformer());

        get("/nationality/:movie", (req, res) -> {
            String movie = req.params(":movie");
            BasicDBObject query = new BasicDBObject();
            query.put("trendType", "nationalityTrend");
            res.type("application/json");
            res.status(200);
            DBObject d = db.getCollection(movie).findOne(query);
            Map m = d.toMap();
            List<Map<String, Object>> l = new ArrayList<>();
            m.forEach((k, v) -> {
                Map<String, Object> agg = new HashMap<String, Object>();
                if (!k.toString().startsWith("_") && !k.toString().startsWith("trend")) {
                    agg.put("code", k.toString());
                    agg.put("z", Long.parseLong(v.toString()));
                    l.add(agg);
                }
            });
            return l;
        }, new JsonTransformer());


        get("/", (req, res) -> {
            res.type("text/html");
            return Files.lines(Paths.get("movieTrend.html")).collect(joining("\n"));
        });

        get("/traffic", (req, res) -> {
            String road = req.queryString().split("=")[1];
            res.type("text/html");
            String content = Files.lines(Paths.get("trafficReport.html")).collect(joining("\n"));
            return content.replace("$$ROAD$$", road);
        });

        get("/trafficdata", (req, res) -> {
            res.type("application/json");
            return DataStore.trafficReport();
        }, new JsonTransformer());
    }
}
