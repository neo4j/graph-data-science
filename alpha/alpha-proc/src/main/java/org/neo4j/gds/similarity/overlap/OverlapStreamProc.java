package org.neo4j.gds.similarity.overlap;

import org.neo4j.gds.impl.similarity.OverlapAlgorithm;
import org.neo4j.gds.impl.similarity.OverlapConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.results.SimilarityResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class OverlapStreamProc extends OverlapProc<SimilarityResult> {

    @Procedure(name = "gds.alpha.similarity.overlap.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<SimilarityResult> overlapStream(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stream(configuration);
    }

    @Override
    public ComputationResultConsumer<OverlapAlgorithm, SimilarityAlgorithmResult, OverlapConfig, Stream<SimilarityResult>> computationResultConsumer() {
        return streamResultConsumer();
    }
}
