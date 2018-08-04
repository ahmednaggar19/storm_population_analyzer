package populationanalyzer.bolts.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Range {
    private ArrayList<Integer> rangePoints;
    private Map<String, Integer> rangesMap;
    private  final String SEPARATOR = "_";

    public Range (ArrayList<Integer> rangePoints) {
        this.rangePoints = rangePoints;
        rangesMap = new HashMap<>();
        initializeMapKeys();
    }

    private void initializeMapKeys() {
        String previousRangePrefix = "";
        for (Integer rangePoint : rangePoints) {
            rangesMap.put(previousRangePrefix + SEPARATOR + rangePoint.toString(), 0);
            previousRangePrefix = rangePoint.toString();
        }
        rangesMap.put(rangePoints.get(rangePoints.size() - 1).toString() + SEPARATOR, 0);
        return;
    }

    private void incrementRangesMap (String key) {
        Integer count = rangesMap.get(key);
        rangesMap.put(key, count + 1);
        return;
    }

    public void updateRangeCount (Integer point) {
        String previousRangePrefix = "";
        for (Integer rangePoint : rangePoints) {
            if (point < rangePoint) {
                incrementRangesMap(previousRangePrefix + SEPARATOR + rangePoint.toString());
                return;
            }
            previousRangePrefix = rangePoint.toString();
        }
        incrementRangesMap(rangePoints.get(rangePoints.size() - 1).toString() + SEPARATOR);
        return;
    }

    public Map<String, Integer> getMap() {
        return this.rangesMap;
    }
}
