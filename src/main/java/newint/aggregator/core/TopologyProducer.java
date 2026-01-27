package newint.aggregator.core;

import java.time.Duration;
import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import newint.aggregator.shared.Aggregation;
import newint.aggregator.shared.Store;
import newint.aggregator.shared.Transaction;
import newint.producer.ValuesGenerator;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TopologyProducer {

  static final String VMART_STORE = "vmart-store";

  static final String STORE_TOPIC = "store";
  static final String ORDER_TOPIC = "order";
  static final String ORDER_AGGREGATED_TOPIC = "order-aggregated";

  private static final Logger LOG = Logger.getLogger(TopologyProducer.class);

  @Produces
  public Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    ObjectMapperSerde<Store> weatherStationSerde = new ObjectMapperSerde<>(Store.class);
    ObjectMapperSerde<Aggregation> aggregationSerde = new ObjectMapperSerde<>(Aggregation.class);
    ObjectMapperSerde<Transaction> transactionSerde = new ObjectMapperSerde<>(Transaction.class);

    Serde<Windowed<Integer>> timeWindowedSerde =
      WindowedSerdes.timeWindowedSerdeFrom(Integer.class, 500L);

    var storeSupplier = Stores.inMemoryWindowStore(
      VMART_STORE,
      Duration.ofMinutes(3),
      Duration.ofMinutes(1),
      false);

    GlobalKTable<Integer, Store> stores = builder.globalTable(
      STORE_TOPIC,
      Consumed.with(Serdes.Integer(), weatherStationSerde));

    var enrichedTransactions = builder.stream(
      ORDER_TOPIC,
      Consumed.with(Serdes.Integer(), Serdes.String())
    ).join(
      stores,
      (stationId, timestamp) -> stationId,
      (timestampAndValue, station) -> {
        String[] parts = timestampAndValue.split(";");
        return new Transaction(station.id, station.name, Instant.parse(parts[0]),
          Double.parseDouble(parts[1]));
      });

      enrichedTransactions
      .groupBy((key, value) -> key, Grouped.with(Serdes.Integer(), transactionSerde))
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
      .aggregate(
        Aggregation::new,
        (stationId, value, aggregation) -> aggregation.updateFrom(value),
        Materialized.<Integer, Aggregation> as(storeSupplier)
          .withKeySerde(Serdes.Integer())
          .withValueSerde(aggregationSerde))
      .toStream()
        .peek((key, value) -> System.out.println(String.format("storeId: %d, avg: %.2f", value.stationId, value.avg)));
//      .to(ORDER_AGGREGATED_TOPIC);

    return builder.build();
  }
}
