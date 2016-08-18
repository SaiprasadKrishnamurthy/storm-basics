package eventstrending;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by saipkri on 18/08/16.
 */
public class DataStore {
    private static ConcurrentHashMap<String, Map<String, Long>> trends = new ConcurrentHashMap<>();

    public static void save(final String trendType, final Map<String, Long> trend) {
        trends.computeIfAbsent(trendType, key -> trend);
        trends.computeIfPresent(trendType, (key, value) -> trend);
    }

    public static Map<String, Map<String, Long>> findAllTrends() {
        return trends;
    }


}
