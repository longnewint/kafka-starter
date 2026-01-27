package newint.aggregator.core;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import io.smallrye.common.net.HostName;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import newint.aggregator.shared.Aggregation;
import newint.aggregator.shared.StoreData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.jboss.logging.Logger;

@ApplicationScoped
public class InteractiveQueries {

    private static final Logger LOG = Logger.getLogger(InteractiveQueries.class);

    @Inject
    KafkaStreams streams;

    public List<PipelineMetadata> getMetaData() {
        return streams.streamsMetadataForStore(TopologyProducer.VMART_STORE)
                .stream()
                .map(m -> new PipelineMetadata(
                        m.hostInfo().host() + ":" + m.hostInfo().port(),
                        m.topicPartitions()
                                .stream()
                                .map(TopicPartition::toString)
                                .collect(Collectors.toSet())))
                .collect(Collectors.toList());
    }

    public void test() {
        var windowStore =
          streams.store(StoreQueryParameters.fromNameAndType(
            "vmart-store",
            QueryableStoreTypes.timestampedWindowStore()
          ));

        Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
        Instant timeTo = Instant.now(); // now (in processing-time)
        try(WindowStoreIterator<ValueAndTimestamp<Object>> iterator = windowStore.fetch(1, timeFrom, timeTo)) {
            while (iterator.hasNext()) {
                KeyValue<Long, ValueAndTimestamp<Object>> next = iterator.next();
                Long windowTimestamp = next.key;
                LOG.infov("Count of 'world' @ time " + windowTimestamp + " is " + next.value);
            }
        }


// Fetch values for the key "world" for all of the windows available in this application instance.
// To get *all* available windows we fetch windows from the beginning of time until now.
//        Instant timeFrom = Instant.ofEpochMilli(0); // beginning of time = oldest available
//        Instant timeTo = Instant.now(); // now (in processing-time)
//        WindowStoreIterator<Long> iterator = windowStore.fetch("world", timeFrom, timeTo);
//        while (iterator.hasNext()) {
//            KeyValue<Long, Long> next = iterator.next();
//            long windowTimestamp = next.key;
//            System.out.println("Count of 'world' @ time " + windowTimestamp + " is " + next.value);
//        }
    }

    public GetWeatherStationDataResult getWeatherStationData(int id) {
        test();

        KeyQueryMetadata metadata = streams.queryMetadataForKey(
                          TopologyProducer.VMART_STORE,
                          id,
                          Serdes.Integer().serializer());

        if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
            LOG.warnv("Found no metadata for key {0}", id);
            return GetWeatherStationDataResult.notFound();
        } else if (metadata.activeHost().host().equals(HostName.getQualifiedHostName())) {
            LOG.infov("Found data for key {0} locally", id);
            Aggregation result = getWeatherStationStore().get(id);

            if (result != null) {
                return GetWeatherStationDataResult.found(StoreData.from(result));
            } else {
                return GetWeatherStationDataResult.notFound();
            }
        } else {
            LOG.infov("Found data for key {0} on remote host {1}:{2}", id, metadata.activeHost().host(), metadata.activeHost().port());
            return GetWeatherStationDataResult.foundRemotely(metadata.activeHost().host(), metadata.activeHost().port());
        }
    }

    private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(TopologyProducer.VMART_STORE, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
