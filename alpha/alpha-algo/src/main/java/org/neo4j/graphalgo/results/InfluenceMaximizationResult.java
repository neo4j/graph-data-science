package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.result.AbstractResultBuilder;

public class InfluenceMaximizationResult
{
    public final long nodeId;
    public final double spreadGain;

    public InfluenceMaximizationResult( long nodeId, double spreadGain )
    {
        this.nodeId = nodeId;
        this.spreadGain = spreadGain;
    }

    @Override
    public String toString()
    {
        return "GreedyResult{nodeId=" + nodeId + ", spreadGain=" + spreadGain + "}";
    }

    public static final class Stats
    {
        public final long nodes;
        public final long computeMillis;

        public Stats(
                long nodes,
                long computeMillis )
        {
            this.nodes = nodes;
            this.computeMillis = computeMillis;
        }

        public static final class Builder extends AbstractResultBuilder<InfluenceMaximizationResult.Stats>
        {

            @Override
            public InfluenceMaximizationResult.Stats build()
            {
                return new InfluenceMaximizationResult.Stats(
                        nodeCount,
                        computeMillis
                );
            }
        }
    }
}
