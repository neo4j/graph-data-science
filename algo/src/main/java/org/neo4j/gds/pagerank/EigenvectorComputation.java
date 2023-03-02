/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.pagerank;

import com.carrotsearch.hppc.LongSet;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.DoubleNodePropertyValues;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.scaling.L2Norm;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.Optional;
import java.util.function.LongToDoubleFunction;

public final class EigenvectorComputation implements PregelComputation<PageRankConfig> {

    private static final String RANK = PageRankComputation.PAGE_RANK;
    private static final String NEXT_RANK = "next_rank";

    private final boolean hasSourceNodes;
    private final LongSet sourceNodes;
    private final LongToDoubleFunction weightDenominator;

    private final double tolerance;
    private final double initialValue;

    EigenvectorComputation(
        long nodeCount,
        PageRankConfig config,
        LongSet sourceNodes,
        LongToDoubleFunction weightDenominator
    ) {
        this.tolerance = config.tolerance();
        this.sourceNodes = sourceNodes;
        this.hasSourceNodes = !sourceNodes.isEmpty();

        // The initial value needs to be normalized. If there are no source nodes,
        // every node gets the same value (1/|V|). If there are source nodes,
        // every non-source nodes gets 0.
        this.initialValue = hasSourceNodes
            ? 1.0 / sourceNodes.size()
            : 1.0 / nodeCount;

        this.weightDenominator = weightDenominator;
    }

    @Override
    public PregelSchema schema(PageRankConfig config) {
        return new PregelSchema.Builder()
            .add(RANK, ValueType.DOUBLE)
            .add(NEXT_RANK, ValueType.DOUBLE)
            .build();
    }

    @Override
    public void init(InitContext<PageRankConfig> context) {
        context.setNodeValue(RANK, initialValue(context));
    }

    private double initialValue(InitContext<PageRankConfig> context) {
        if (!hasSourceNodes || sourceNodes.contains(context.nodeId())) {
            return initialValue;
        }
        return 0;
    }

    @Override
    public void compute(ComputeContext<PageRankConfig> context, Messages messages) {
        // Instead of just using the adjacency matrix A, we add
        // the centrality score from the previous iteration (A + I).
        // This makes the difference between dominant eigenvalues
        // more distinguishable.
        double nextRank = context.doubleNodeValue(RANK);

        for (var message : messages) {
            nextRank += message;
        }

        // The degree function returns either 1 if the graph is unweighted
        // or the sum of relationship weights if the graph is weighted.
        // For weighted graphs, we multiply the sent values with the relationship
        // weight and need to make sure that those weights are normalized.
        context.sendToNeighbors(nextRank / weightDenominator.applyAsDouble(context.nodeId()));
        context.setNodeValue(NEXT_RANK, nextRank);
    }

    @Override
    public boolean masterCompute(MasterComputeContext<PageRankConfig> context) {
        var concurrency = context.config().concurrency();

        var properties = new DoubleNodePropertyValues() {
            @Override
            public long valuesStored() {
                return context.nodeCount();
            }

            @Override
            public long maxIndex() {
                return context.nodeCount();
            }

            @Override
            public double doubleValue(long nodeId) {
                return context.doubleNodeValue(nodeId, NEXT_RANK);
            }
        };

        // Normalize using L2-Norm (Power iteration)
        var scaler = ScalerFactory.parse(L2Norm.TYPE).create(
            properties,
            context.nodeCount(),
            concurrency,
            ProgressTracker.NULL_TRACKER,
            context.executorService()
        );

        // We use a mutable boolean instead of an AtomicBoolean
        // since we only ever flip from true to false. If multiple
        // threads try to change the value, only one needs to succeed.
        var didConverge = new MutableBoolean(true);
        var tasks = PartitionUtils.rangePartition(concurrency, context.nodeCount(),
            partition -> (Runnable) () -> partition.consume(nodeId -> {
                var normalizedNextRank = scaler.scaleProperty(nodeId);
                var normalizedCurrRank = context.doubleNodeValue(nodeId, RANK);

                // check for convergence
                if (Math.abs(normalizedNextRank - normalizedCurrRank) > tolerance) {
                    didConverge.setFalse();
                }

                context.setNodeValue(
                    nodeId,
                    RANK,
                    normalizedNextRank
                );
            }),
            Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(context.executorService())
            .run();

        return !context.isInitialSuperstep() && didConverge.booleanValue();
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer.Sum());
    }

    @Override
    public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue * relationshipWeight;
    }

}
