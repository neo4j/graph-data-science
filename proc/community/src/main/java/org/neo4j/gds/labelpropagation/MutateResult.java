package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.api.ProcedureReturnColumns;

import java.util.Map;

@SuppressWarnings("unused")
public final class MutateResult extends LabelPropagationStatsProc.StatsResult {

    public final long mutateMillis;
    public final long nodePropertiesWritten;

    private MutateResult(
        long ranIterations,
        boolean didConverge,
        long communityCount,
        Map<String, Object> communityDistribution,
        long preProcessingMillis,
        long computeMillis,
        long postProcessingMillis,
        long mutateMillis,
        long nodePropertiesWritten,
        Map<String, Object> configuration
    ) {
        super(
            ranIterations,
            didConverge,
            communityCount,
            communityDistribution,
            preProcessingMillis,
            computeMillis,
            postProcessingMillis,
            configuration
        );
        this.mutateMillis = mutateMillis;
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<MutateResult> {

        Builder(ProcedureReturnColumns returnColumns, int concurrency) {
            super(returnColumns, concurrency);
        }

        @Override
        protected MutateResult buildResult() {
            return new MutateResult(
                ranIterations,
                didConverge,
                maybeCommunityCount.orElse(0L),
                communityHistogramOrNull(),
                preProcessingMillis,
                computeMillis,
                postProcessingDuration,
                mutateMillis,
                nodePropertiesWritten,
                config.toMap()
            );
        }
    }
}
