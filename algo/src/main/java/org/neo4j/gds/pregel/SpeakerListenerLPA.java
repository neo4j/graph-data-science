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
import org.neo4j.collection.pool.LinkedQueuePool;
import org.neo4j.collection.pool.Pool;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@PregelProcedure(
    name = "gds.pregel.sllpa",
    description = "TODO"
)
public class SpeakerListenerLPA implements PregelComputation<SpeakerListenerLPA.SpeakerListenerLPAConfig> {

    public static final String LABELS_PROPERTY = "labels";
    private Pool<Map<Long, Integer>> mapPool;
    private final Random random;

    public SpeakerListenerLPA() {random = new Random();}

    @Override
    public PregelSchema schema() {
        return new PregelSchema.Builder()
            .add(LABELS_PROPERTY, ValueType.LONG_ARRAY)
            .build();
    }

    @Override
    public void init(PregelContext.InitContext<SpeakerListenerLPA.SpeakerListenerLPAConfig> context) {
        context.setNodeValue(LABELS_PROPERTY, new long[context.config().maxIterations()]);
        this.mapPool = new LinkedQueuePool<>(context.config().concurrency(), HashMap::new);
    }

    @Override
    public void compute(
        PregelContext.ComputeContext<SpeakerListenerLPA.SpeakerListenerLPAConfig> context, Pregel.Messages messages
    ) {
        var labels = context.longArrayNodeValue(LABELS_PROPERTY);

        if (context.isInitialSuperstep()) {
            labels[0] = context.nodeId();
            context.sendToNeighbors(context.nodeId());
        } else if (context.superstep() < context.config().propagationSteps()) {
            listen(context, messages, labels);
            speak(context, labels);
        } else {
            listen(context, messages, labels);
            prune(context, labels);
        }
    }

    private void listen(PregelContext.ComputeContext<SpeakerListenerLPAConfig> context, Pregel.Messages messages, long[] labels) {
        var labelVotes = mapPool.acquire();
        for (Double message : messages) {
            labelVotes.compute(message.longValue(), (key, frequency) -> 1 + (frequency == null ? 0 : frequency));
        }

        Map.Entry<Long, Integer> winningVote = Map.entry(0L, Integer.MIN_VALUE);
        for (Map.Entry<Long, Integer> labelVote : labelVotes.entrySet()) {
            if (labelVote.getValue() > winningVote.getValue()) {
                winningVote = labelVote;
            }
        }

        labels[context.superstep()] = winningVote.getKey();

        labelVotes.clear();
        mapPool.release(labelVotes);
    }

    private void speak(PregelContext.ComputeContext<SpeakerListenerLPAConfig> context, long[] labels) {
        var randomLabelPosition = random.nextInt(context.superstep() + 1);
        var labelToSend = labels[randomLabelPosition];
        context.sendToNeighbors(labelToSend);
    }

    private void prune(PregelContext.ComputeContext<SpeakerListenerLPAConfig> context, long[] labels) {
        var labelVotes = mapPool.acquire();
        for (long label : labels) {
            labelVotes.compute(label, (key, frequency) -> 1 + (frequency == null ? 0 : frequency));
        }

        var filteredLabels = labelVotes
            .entrySet().stream()
            .filter(entry -> {
                var relativeFrequency = entry.getValue().doubleValue() / labels.length;
                return relativeFrequency > context.config().r();
            }).mapToLong(Map.Entry::getKey).toArray();

        context.setNodeValue(LABELS_PROPERTY, filteredLabels);
    }


    @ValueClass
    @Configuration
    interface SpeakerListenerLPAConfig extends PregelConfig {

        @Value.Derived
        @Configuration.Ignore
        @Override
        default boolean isAsynchronous() {
            return true;
        }

        @Value.Default
        default double r() {
            return 0.2;
        }

        @Value.Derived
        @Configuration.Ignore
        default int propagationSteps() {
            return maxIterations() - 1;
        }
    }
}

