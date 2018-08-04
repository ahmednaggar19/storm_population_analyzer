package populationanalyzer;

public class TimeTracker {

    private static long START_TIME = 0;

    public static void setStartTime (long currentStartTime) {
        START_TIME = currentStartTime;
        return;
    }

    public static long getCurrentRelativeMillis (long currentMillies) {
        return currentMillies - START_TIME;
    }
}
