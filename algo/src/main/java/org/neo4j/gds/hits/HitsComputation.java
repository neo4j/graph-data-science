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
package org.neo4j.gds.hits;

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.BidirectionalPregelComputation;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.context.ComputeContext.BidirectionalComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext.BidirectionalInitContext;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;
import org.neo4j.gds.mem.MemoryEstimateDefinition;

import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;

import static org.neo4j.gds.hits.HitsState.CALCULATE_AUTHS;
import static org.neo4j.gds.hits.HitsState.CALCULATE_HUBS;
import static org.neo4j.gds.hits.HitsState.INIT;
import static org.neo4j.gds.hits.HitsState.NORMALIZE_AUTHS;
import static org.neo4j.gds.hits.HitsState.NORMALIZE_HUBS;


public class HitsComputation implements BidirectionalPregelComputation<HitsConfig> {

    // Global norm aggregator shared by all workers
    private final DoubleAdder globalNorm = new DoubleAdder();
    private HitsState state = INIT;

    @Override
    public PregelSchema schema(HitsConfig config) {
        return new PregelSchema.Builder()
            .add(config.authProperty(), ValueType.DOUBLE)
            .add(config.hubProperty(), ValueType.DOUBLE)
            .build();
    }

    @Override
    public MemoryEstimateDefinition estimateDefinition(boolean isAsynchronous) {
        return () -> Pregel.memoryEstimation(
            Map.of(
                "auth", ValueType.DOUBLE,
                "hub", ValueType.DOUBLE
            ),
            false,
            false
        );
    }

    @Override
    public void init(BidirectionalInitContext<HitsConfig> context) {
        context.setNodeValue(context.config().hubProperty(), 1D);
        context.setNodeValue(context.config().authProperty(), 1D);
    }

    @Override
    public void compute(BidirectionalComputeContext<HitsConfig> context, Messages messages) {
        switch (state) {
            case INIT:
                var auth = (double) context.incomingDegree();
                context.setNodeValue(context.config().authProperty(), auth);
                updateGlobalNorm(auth);
                break;
            case CALCULATE_AUTHS:
                calculateValue(context, messages, context.config().authProperty());
                break;
            case NORMALIZE_AUTHS:
                normalizeAuthValue(context);
                break;
            case CALCULATE_HUBS:
                calculateValue(context, messages, context.config().hubProperty());
                break;
            case NORMALIZE_HUBS:
                normalizeHubValue(context);
                break;
        }
    }

    @Override
    public boolean masterCompute(MasterComputeContext<HitsConfig> context) {
        if (state == INIT || state == CALCULATE_AUTHS || state == CALCULATE_HUBS) {
            var norm = globalNorm.sumThenReset();
            globalNorm.add(Math.sqrt(norm));
        } else if (state == NORMALIZE_AUTHS || state == NORMALIZE_HUBS) {
            globalNorm.reset();
        }
        state = state.advance();

        return false;
    }

    private void calculateValue(
        BidirectionalComputeContext<HitsConfig> context,
        Messages messages,
        String authProperty
    ) {
        var auth = 0D;
        for (Double message : messages) {
            auth += message;
        }
        context.setNodeValue(authProperty, auth);
        updateGlobalNorm(auth);
    }

    private void normalizeHubValue(BidirectionalComputeContext<HitsConfig> context) {
        // normalise hub
        var normalizedValue = normalize(context, context.config().hubProperty());
        // send normalised hubs to outgoing neighbors
        context.sendToNeighbors(normalizedValue);
    }

    private void normalizeAuthValue(BidirectionalComputeContext<HitsConfig> context) {
        // normalise auth
        var normalizedValue = normalize(context, context.config().authProperty());
        // send normalised auths to incoming neighbors

        context.sendToIncomingNeighbors(normalizedValue);
    }

    private void updateGlobalNorm(double value) {
        globalNorm.add(Math.pow(value, 2));
    }

    private double normalize(BidirectionalComputeContext<HitsConfig> context, String property) {
        var value = context.doubleNodeValue(property);
        var norm = globalNorm.sum();
        var normalizedValue = value / norm;
        context.setNodeValue(property, normalizedValue);
        return normalizedValue;
    }

}
