package org.neo4j.graphalgo.impl.labelprop;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.logging.Log;

public class LabelPropagationFactory extends AlgorithmFactory<LabelPropagation> {

    @Override
    public LabelPropagation build(
            final Graph graph,
            final ProcedureConfiguration configuration,
            final AllocationTracker tracker,
            final Log log) {
        int concurrency = configuration.getConcurrency();
        int batchSize = configuration.getBatchSize();
        return new LabelPropagation(
                graph,
                batchSize,
                concurrency,
                Pools.DEFAULT,
                tracker
        );
    }
}
