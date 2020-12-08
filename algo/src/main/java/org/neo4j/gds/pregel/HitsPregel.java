/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.gds.pregel;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;
import org.neo4j.graphalgo.beta.pregel.context.ComputeContext;
import org.neo4j.graphalgo.beta.pregel.context.InitContext;
import org.neo4j.graphalgo.beta.pregel.context.MasterComputeContext;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.StreamSupport;

@PregelProcedure(
    name = "gds.alpha.hits",
    description = "TODO"
)
public class HitsPregel implements PregelComputation<HitsPregel.HitsConfig> {

    static final String HUB_PROPERTY = "hub";
    static final String AUTH_PROPERTY = "auth";
    private static final String NEIGHBOR_IDS = "neighborIds";

    private final DoubleAdder globalNorm = new DoubleAdder();
    private HitsState state = HitsState.SEND_IDS;

    @Override
    public PregelSchema schema() {
        return new PregelSchema.Builder()
            .add(AUTH_PROPERTY, ValueType.DOUBLE)
            .add(HUB_PROPERTY, ValueType.DOUBLE)
            .add(NEIGHBOR_IDS, ValueType.LONG_ARRAY, PregelSchema.Visibility.PRIVATE)
            .build();
    }

    @Override
    public void init(InitContext<HitsConfig> context) {
        context.setNodeValue(AUTH_PROPERTY, 1D);
        context.setNodeValue(HUB_PROPERTY, 1D);
    }

    @Override
    public void compute(
        ComputeContext<HitsConfig> context, Pregel.Messages messages
    ) {
        if (state == HitsState.SEND_IDS) {
            context.sendToNeighbors(context.nodeId());

        } else if (state == HitsState.RECEIVE_IDS) {
            // will only work with directed graphs
            var neighborIds = StreamSupport
                .stream(messages.spliterator(), false)
                .mapToLong(Double::longValue)
                .toArray();
            context.setNodeValue(NEIGHBOR_IDS, neighborIds);

            // compute auths
            var auth = neighborIds.length;
            context.setNodeValue(AUTH_PROPERTY, (double) auth);
            updateGlobalNorm(auth);

        } else if (state == HitsState.CALCULATE_AUTHS) {
            var auth = 0D;
            for (Double message : messages) {
                auth += message;
            }
            context.setNodeValue(AUTH_PROPERTY, auth);
            updateGlobalNorm(auth);

        } else if (state == HitsState.NORMALIZE_AUTHS) {
            // normalise auth
            var normalizedValue = normalize(context, AUTH_PROPERTY);
            // send normalised auths to incomming neighbors
            for (long neighbor : context.longArrayNodeValue(NEIGHBOR_IDS)) {
                context.sendTo(neighbor, normalizedValue);
            }

        } else if (state == HitsState.CALCULATE_HUBS) {
            var hub = 0D;
            for (Double message : messages) {
                hub += message;
            }
            context.setNodeValue(HUB_PROPERTY, hub);
            updateGlobalNorm(hub);

        } else if (state == HitsState.NORMALIZE_HUBS) {
            // normalise hub
            var normalizedValue = normalize(context, HUB_PROPERTY);
            // send normalised hubs to outgoing neighbors
            context.sendToNeighbors(normalizedValue);
        }
    }

    @Override
    public void masterCompute(MasterComputeContext<HitsConfig> context) {
        if (state == HitsState.RECEIVE_IDS || state == HitsState.CALCULATE_AUTHS || state == HitsState.CALCULATE_HUBS) {
            var norm = globalNorm.sumThenReset();
            globalNorm.add(Math.sqrt(norm));
        } else if (state == HitsState.NORMALIZE_AUTHS || state == HitsState.NORMALIZE_HUBS) {
            globalNorm.reset();
        }
        state = state.advance();
    }

    private void updateGlobalNorm(double value) {
        globalNorm.add(Math.pow(value,2));
    }

    private double normalize(ComputeContext<HitsConfig> context, String property) {
        var value = context.doubleNodeValue(property);
        var norm = globalNorm.sum();
        var normalizedValue = value / norm;
        context.setNodeValue(property, normalizedValue);
        return normalizedValue;
    }


    @ValueClass
    @Configuration
    public interface HitsConfig extends PregelConfig {
        @Value
        int k();

        @Override
        @Value.Derived
        default int maxIterations() {
            return k() * 4 + 1;
        }

        @Override
        @Configuration.Ignore
        @Value.Derived
        default boolean isAsynchronous() {
            return false;
        }
    }

    private enum HitsState {
        SEND_IDS {
            @Override
            HitsState advance() {
                return RECEIVE_IDS;
            }
        },
        RECEIVE_IDS {
            @Override
            HitsState advance() {
                return NORMALIZE_AUTHS;
            }
        },
        CALCULATE_AUTHS {
            @Override
            HitsState advance() {
                return NORMALIZE_AUTHS;
            }
        },
        NORMALIZE_AUTHS {
            @Override
            HitsState advance() {
                return CALCULATE_HUBS;
            }
        },
        CALCULATE_HUBS {
            @Override
            public HitsState advance() {
                return NORMALIZE_HUBS;
            }
        },
        NORMALIZE_HUBS {
            @Override
            HitsState advance() {
                return CALCULATE_AUTHS;
            }
        };

        abstract HitsState advance();
    }
}

