package newint.aggregator.core;

import java.time.Instant;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import newint.aggregator.shared.Aggregation;
import newint.aggregator.shared.Store;
import newint.aggregator.shared.TemperatureMeasurement;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import io.quarkus.kafka.client.serialization.ObjectMapperSerde;

@ApplicationScoped
public class TopologyProducer {

    static final String VMART_STORE = "vmart-store";

    static final String STORE_TOPIC = "store";
    static final String ORDER_TOPIC = "order";
    static final String ORDER_AGGREGATED_TOPIC = "order-aggregated";

    @Produces
    public Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        ObjectMapperSerde<Store> weatherStationSerde = new ObjectMapperSerde<>(Store.class);
        ObjectMapperSerde<Aggregation> aggregationSerde = new ObjectMapperSerde<>(Aggregation.class);

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(VMART_STORE);

        GlobalKTable<Integer, Store> stations = builder.globalTable(
                STORE_TOPIC,
                Consumed.with(Serdes.Integer(), weatherStationSerde));

        builder.stream(
                ORDER_TOPIC,
                Consumed.with(Serdes.Integer(), Serdes.String()))
                .join(
                        stations,
                        (stationId, timestampAndValue) -> stationId,
                        (timestampAndValue, station) -> {
                            String[] parts = timestampAndValue.split(";");
                            return new TemperatureMeasurement(station.id, station.name, Instant.parse(parts[0]),
                                    Double.valueOf(parts[1]));
                        })
                .groupByKey()
                .aggregate(
                        Aggregation::new,
                        (stationId, value, aggregation) -> aggregation.updateFrom(value),
                        Materialized.<Integer, Aggregation> as(storeSupplier)
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(aggregationSerde))
                .toStream()
                .to(
                        ORDER_AGGREGATED_TOPIC,
                        Produced.with(Serdes.Integer(), aggregationSerde));

        return builder.build();
    }
}
