package good;

import org.neo4j.graphalgo.annotation.Configuration;

@Configuration("KeyRenamesConfig")
public interface KeyRenames {

    @Configuration.Key("key could also be an invalid identifier")
    int lookupUnderAnotherKey();
}
