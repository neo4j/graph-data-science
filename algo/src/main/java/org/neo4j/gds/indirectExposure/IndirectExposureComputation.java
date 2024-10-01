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
            context.setNodeValue(EXPOSURE, 1.0);
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
            if (this.visited.getAndSet(context.nodeId())) {
                context.voteToHalt();
                return;
            }
            var totalTransfers = this.totalTransfers.get(context.nodeId());
            // we take the first value as we use a MAX reducer
            var parentExposure = messages.doubleIterator().nextDouble();
            // normalize the exposure
            var newExposure = parentExposure / totalTransfers;

            context.setNodeValue(EXPOSURE, newExposure);
            context.sendToNeighbors(newExposure);
        }

        context.voteToHalt();
    }

    @Override
    public PregelSchema schema(IndirectExposureConfig config) {
        return new PregelSchema.Builder()
            .add(EXPOSURE, ValueType.DOUBLE, PregelSchema.Visibility.PUBLIC)
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
        return () -> Pregel.memoryEstimation(Map.of(EXPOSURE, ValueType.DOUBLE), false, false);
    }
}
