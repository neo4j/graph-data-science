package org.neo4j.graphalgo.pregel.cc;

import java.util.Map;
import javax.annotation.processing.Generated;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

@Generated("org.neo4j.graphalgo.pregel.PregelProcessor")
public final class ComputationStreamProc {
    @Procedure(
        name = "gds.pregel.fancy",
        mode = Mode.READ
    )
    @Description("My fancy computation")
    public void stream(@Name("graphName") Object graphNameOrConfig,
                       @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration) {
        // return stream(compute(graphNameOrConfig, configuration));
    }
}