package newint.aggregator.shared;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class StoreData {

    public int stationId;
    public String stationName;
    public double min = Double.MAX_VALUE;
    public double max = Double.MIN_VALUE;
    public int count;
    public double avg;

    private StoreData(int stationId, String stationName, double min, double max, int count, double avg) {
        this.stationId = stationId;
        this.stationName = stationName;
        this.min = min;
        this.max = max;
        this.count = count;
        this.avg = avg;
    }

    public static StoreData from(Aggregation aggregation) {
        return new StoreData(
                aggregation.stationId,
                aggregation.stationName,
                aggregation.min,
                aggregation.max,
                aggregation.count,
                aggregation.avg);
    }
}
