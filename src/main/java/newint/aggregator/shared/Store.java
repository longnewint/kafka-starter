package newint.aggregator.shared;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class Store{

    public int id;
    public String name;

    public Store() {}

    public Store(int id, String name) {
        this.id = id;
        this.name = name;
    }
}
