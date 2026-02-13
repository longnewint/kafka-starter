package newint.aggregator.shared;

import java.time.Instant;

public class Transaction {

    public int stationId;
    public String stationName;
    public Instant timestamp;
    public double value;

    public Transaction(int stationId, String stationName, Instant timestamp, double value) {
        this.stationId = stationId;
        this.stationName = stationName;
        this.timestamp = timestamp;
        this.value = value;
    }
}
