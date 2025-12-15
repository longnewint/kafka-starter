package newint.producer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.Record;
import newint.aggregator.shared.Store;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ValuesGenerator {

    private static final Logger LOG = Logger.getLogger(ValuesGenerator.class);

    private Random random = new Random();

    private List<Store> stores = List.of(
                    new Store(1, "vMart Ottawa", 70),
                    new Store(2, "vMart Kanata", 50),
                    new Store(3, "vMart Gatineau", 60));

    @Outgoing("order")
    public Multi<Record<Integer, String>> generate() {
        return Multi.createFrom().ticks().every(Duration.ofMillis(500))
                .onOverflow().drop()
                .map(tick -> {
                    Store store = stores.get(random.nextInt(stores.size()));
                    double orderTotal = BigDecimal.valueOf(random.nextGaussian() * 20 + store.averageOrderTotal)
                            .setScale(1, RoundingMode.HALF_UP)
                            .doubleValue();

                    LOG.infov("store: {0}, orderTotal: {1}", store.name, orderTotal);
                    return Record.of(store.id, Instant.now() + ";" + orderTotal);
                });
    }

    @Outgoing("store")
    public Multi<Record<Integer, String>> saveStoreInformation() {
        return Multi.createFrom().items(stores.stream()
                .map(s -> Record.of(s.id, "{ \"id\" : " + s.id + ", \"name\" : \"" + s.name + "\" }"))
        );
    }

    private static class Store {

        int id;
        String name;
        int averageOrderTotal;

        public Store(int id, String name, int averageTotal) {
            this.id = id;
            this.name = name;
            this.averageOrderTotal = averageTotal;
        }
    }
}
