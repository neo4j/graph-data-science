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
package org.neo4j.gds.beta.pregel.lp;

import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.Messages;
import org.neo4j.gds.beta.pregel.PregelComputation;
import org.neo4j.gds.beta.pregel.PregelProcedureConfig;
import org.neo4j.gds.beta.pregel.PregelSchema;
import org.neo4j.gds.beta.pregel.annotation.GDSMode;
import org.neo4j.gds.beta.pregel.annotation.PregelProcedure;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.SeedConfig;

import java.util.Arrays;
import java.util.Optional;

/**
 * Basic implementation potentially suffering from oscillating vertex states due to synchronous computation.
 */
@PregelProcedure(name = "example.pregel.lp", modes = {GDSMode.STREAM})
public class LabelPropagationPregel implements PregelComputation<LabelPropagationPregel.LabelPropagationPregelConfig> {

    public static final String LABEL_KEY = "label";

    @Override
    public PregelSchema schema(LabelPropagationPregel.LabelPropagationPregelConfig config) {
        return new PregelSchema.Builder().add(LABEL_KEY, ValueType.LONG).build();
    }

    @Override
    public void init(InitContext<LabelPropagationPregel.LabelPropagationPregelConfig> context) {
        context.setNodeValue(LABEL_KEY, context.nodeId());
    }

    @Override
    public void compute(ComputeContext<LabelPropagationPregel.LabelPropagationPregelConfig> context, Messages messages) {
        if (context.isInitialSuperstep()) {
            context.sendToNeighbors(context.nodeId());
        } else {
            if (messages != null) {
                long oldValue = context.longNodeValue(LABEL_KEY);
                long newValue = oldValue;

                // TODO: could be shared across compute functions per thread
                // We receive at most |degree| messages
                long[] buffer = new long[context.degree()];

                int messageCount = 0;

                for (var message : messages) {
                    buffer[messageCount++] = message.longValue();
                }

                int maxOccurences = 1;
                if (messageCount > 1) {
                    // Sort to compute the most frequent id
                    Arrays.sort(buffer, 0, messageCount);
                    int currentOccurences = 1;
                    for (int i = 1; i < messageCount; i++) {
                        if (buffer[i] == buffer[i - 1]) {
                            currentOccurences++;
                            if (currentOccurences > maxOccurences) {
                                maxOccurences = currentOccurences;
                                newValue = buffer[i];
                            }
                        } else {
                            currentOccurences = 1;
                        }
                    }
                }

                // All with same frequency, pick smallest id
                if (maxOccurences == 1) {
                    newValue = Math.min(oldValue, buffer[0]);
                }

                if (newValue != oldValue) {
                    context.setNodeValue(LABEL_KEY, newValue);
                    context.sendToNeighbors(newValue);
                }
            }
        }
        context.voteToHalt();
    }

    @ValueClass
    @Configuration("LabelPropagationPregelConfigImpl")
    @SuppressWarnings("immutables:subtype")
    public interface LabelPropagationPregelConfig extends PregelProcedureConfig, SeedConfig {

        static LabelPropagationPregelConfig of(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper userInput
        ) {
            return new LabelPropagationPregelConfigImpl(graphName, maybeImplicitCreate, username, userInput);
        }
    }
}
