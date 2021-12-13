package org.neo4j.gds.similarity.overlap;

import org.neo4j.gds.impl.similarity.OverlapAlgorithm;
import org.neo4j.gds.impl.similarity.OverlapConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.similarity.AlphaSimilarityStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class OverlapStatsProc extends OverlapProc<AlphaSimilarityStatsResult> {

    @Procedure(name = "gds.alpha.similarity.overlap.stats", mode = READ)
    @Description(DESCRIPTION)
    public Stream<AlphaSimilarityStatsResult> overlapStats(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(configuration);
    }

    @Override
    public ComputationResultConsumer<OverlapAlgorithm, SimilarityAlgorithmResult, OverlapConfig, Stream<AlphaSimilarityStatsResult>> computationResultConsumer() {
        return statsResultConsumer();
    }
}
