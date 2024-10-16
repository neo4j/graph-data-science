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
package org.neo4j.gds.indirectExposure;

import org.eclipse.collections.api.block.function.primitive.LongToBooleanFunction;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.Reducer;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.degree.DegreeFunction;
import org.neo4j.gds.mem.MemoryEstimateDefinition;

import java.util.Map;
import java.util.Optional;

class IndirectExposureComputation implements PregelComputation<IndirectExposureConfig> {

    static final String EXPOSURE = "exposure";
    static final String HOP = "hop";
    static final String PARENT = "parent";
    static final String ROOT = "root";

    private final DegreeFunction totalTransfers;
    private final HugeAtomicBitSet visited;

    private LongToBooleanFunction isSanctioned;

    IndirectExposureComputation(
        LongToBooleanFunction isSanctioned,
        DegreeFunction totalTransfers,
        HugeAtomicBitSet visited
    ) {
        this.isSanctioned = isSanctioned;
        this.totalTransfers = totalTransfers;
        this.visited = visited;
    }

    @Override
    public void init(InitContext<IndirectExposureConfig> context) {
        var isSanctioned = this.isSanctioned.valueOf(context.nodeId());
        if (isSanctioned) {
            long originalId = context.toOriginalId(context.nodeId());
            context.setNodeValue(EXPOSURE, 1.0);
            context.setNodeValue(HOP, 0L);
            context.setNodeValue(ROOT, originalId);
            context.setNodeValue(PARENT, originalId);
            this.visited.set(context.nodeId());
        }
    }

    @Override
    public void compute(ComputeContext<IndirectExposureConfig> context, Messages messages) {
        double exposure = context.doubleNodeValue(EXPOSURE);

        if (context.isInitialSuperstep()) {
            // only sanctioned nodes send messages on the first iteration
            if (!this.visited.get(context.nodeId())) {
                context.voteToHalt();
                return;
            }
            context.sendToNeighbors(exposure);
        } else {
            // Each node is only visited once.
            if (this.visited.getAndSet(context.nodeId())) {
                context.voteToHalt();
                return;
            }
            var totalTransfers = this.totalTransfers.get(context.nodeId());
            // we take the first value as we use a MAX reducer
            var parentExposure = messages.doubleIterator().nextDouble();
            // since we use a MAX reducer, we can get the sender of the reduced message
            var sender = messages.sender().orElseThrow();
            // normalize the exposure
            var newExposure = parentExposure / totalTransfers;
            // update node state
            context.setNodeValue(EXPOSURE, newExposure);
            context.setNodeValue(PARENT, context.toOriginalId(sender));
            context.setNodeValue(HOP, context.superstep());
            context.setNodeValue(ROOT, context.longNodeValue(ROOT, sender));
            // propagate the exposure to neighbors
            context.sendToNeighbors(newExposure);
        }

        context.voteToHalt();
    }

    @Override
    public PregelSchema schema(IndirectExposureConfig config) {
        return new PregelSchema.Builder()
            .add(EXPOSURE, ValueType.DOUBLE, PregelSchema.Visibility.PRIVATE)
            .add(HOP, ValueType.LONG, PregelSchema.Visibility.PRIVATE)
            .add(PARENT, ValueType.LONG, PregelSchema.Visibility.PRIVATE)
            .add(ROOT, ValueType.LONG, PregelSchema.Visibility.PRIVATE)
            .build();
    }

    @Override
    public Optional<Reducer> reducer() {
        return Optional.of(new Reducer.Max());
    }

    @Override
    public double applyRelationshipWeight(double nodeValue, double relationshipWeight) {
        return nodeValue * relationshipWeight;
    }

    @Override
    public MemoryEstimateDefinition estimateDefinition(boolean isAsynchronous) {
        return () -> Pregel.memoryEstimation(
            Map.of(
                EXPOSURE, ValueType.DOUBLE,
                HOP, ValueType.LONG,
                PARENT, ValueType.LONG,
                ROOT, ValueType.LONG
            ), false, false, true
        );
    }
}
