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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntScatterMap;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.Configuration;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.Pregel;
import org.neo4j.graphalgo.beta.pregel.PregelComputation;
import org.neo4j.graphalgo.beta.pregel.PregelConfig;
import org.neo4j.graphalgo.beta.pregel.PregelContext;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.beta.pregel.annotation.PregelProcedure;

import java.util.Arrays;
import java.util.Random;

@PregelProcedure(
    name = "gds.pregel.sllpa",
    description = "TODO"
)
public class SpeakerListenerLPA implements PregelComputation<SpeakerListenerLPA.SpeakerListenerLPAConfig> {

    public static final String LABELS_PROPERTY = "labels";

    private final ThreadLocal<Random> random;

    public SpeakerListenerLPA() {
        this(System.currentTimeMillis());
    }

    public SpeakerListenerLPA(long seed) {
        random = ThreadLocal.withInitial(() -> new Random(seed));
    }

    @Override
    public PregelSchema schema() {
        return new PregelSchema.Builder()
            .add(LABELS_PROPERTY, ValueType.LONG_ARRAY)
            .build();
    }

    @Override
    public void init(PregelContext.InitContext<SpeakerListenerLPA.SpeakerListenerLPAConfig> context) {
        var initialLabels = new long[context.config().maxIterations()];
        // when nodes do not have incoming rels, it should vote for itself always
        Arrays.fill(initialLabels, context.nodeId());
        context.setNodeValue(LABELS_PROPERTY, initialLabels);
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

    private void listen(
        PregelContext.ComputeContext<SpeakerListenerLPAConfig> context,
        Pregel.Messages messages,
        long[] labels
    ) {
        if (!messages.isEmpty()) {
            var labelVotes = new LongIntScatterMap();
            for (Double message : messages) {
                labelVotes.addTo(message.longValue(), 1);
            }

            long winningLabel = 0;
            int maxFrequency = Integer.MIN_VALUE;
            for (LongIntCursor labelVote : labelVotes) {
                if (labelVote.value > maxFrequency || (labelVote.value == maxFrequency && winningLabel > labelVote.key)) {
                    winningLabel = labelVote.key;
                    maxFrequency = labelVote.value;
                }
            }

            labels[context.superstep()] = winningLabel;
        }
    }

    private void speak(PregelContext.ComputeContext<SpeakerListenerLPAConfig> context, long[] labels) {
        var randomLabelPosition = random.get().nextInt(context.superstep() + 1);
        var labelToSend = labels[randomLabelPosition];
        context.sendToNeighbors(labelToSend);
    }

    // IDEA: Instead of just returning every community the current node is part of, keep the frequency of each community as a, sort of, weight
    private void prune(PregelContext.ComputeContext<SpeakerListenerLPAConfig> context, long[] labels) {
        var labelVotes = new LongIntScatterMap();
        for (long label : labels) {
            labelVotes.addTo(label, 1);
        }

        var labelsToKeep = new LongArrayList(labels.length);

        for (LongIntCursor labelVote : labelVotes) {
            var relativeFrequency = ((double) labelVote.value) / labels.length;
            if (relativeFrequency > context.config().r()) {
                labelsToKeep.add(labelVote.key);
            }
        }

        context.setNodeValue(
            LABELS_PROPERTY,
            labelsToKeep.size() == labels.length ? labelsToKeep.buffer : labelsToKeep.toArray()
        );
    }


    @ValueClass
    @Configuration
    public interface SpeakerListenerLPAConfig extends PregelConfig {

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

