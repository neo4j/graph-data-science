package org.neo4j.gds.similarity.overlap;

import org.neo4j.gds.impl.similarity.OverlapAlgorithm;
import org.neo4j.gds.impl.similarity.OverlapConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithmResult;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.similarity.AlphaSimilaritySummaryResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.WRITE;

public class OverlapWriteProc extends OverlapProc<AlphaSimilaritySummaryResult> {

    @Procedure(name = "gds.alpha.similarity.overlap.write", mode = WRITE)
    @Description(DESCRIPTION)
    public Stream<AlphaSimilaritySummaryResult> overlapWrite(
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(configuration);
    }

    @Override
    public ComputationResultConsumer<OverlapAlgorithm, SimilarityAlgorithmResult, OverlapConfig, Stream<AlphaSimilaritySummaryResult>> computationResultConsumer() {
        return writeResultConsumer();
    }
}
