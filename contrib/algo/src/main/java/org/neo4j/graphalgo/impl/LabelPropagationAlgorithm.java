/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphdb.Direction;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public abstract class LabelPropagationAlgorithm<Self extends LabelPropagationAlgorithm<Self>> extends Algorithm<Self> {

    public static final String PARTITION_TYPE = "property";
    public static final String WEIGHT_TYPE = "weight";

    public static class StreamResult {
        public final long nodeId;
        public final long label;

        public StreamResult(long nodeId, long label) {
            this.nodeId = nodeId;
            this.label = label;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    public final Self compute(Direction direction, long maxIterations) {
        return compute(direction, maxIterations, DefaultRandom.INSTANCE);
    }

    public final Self compute(Direction direction, long maxIterations, long randomSeed) {
        return compute(direction, maxIterations, new Random(randomSeed));
    }

    public final Self compute(Direction direction, long maxIterations, Random random) {
        return compute(direction, maxIterations, new ProvidedRandom(random));
    }

    abstract Self compute(
            Direction direction,
            long maxIterations,
            RandomProvider random);

    public abstract long ranIterations();

    public abstract boolean didConverge();

    public abstract Labels labels();

    public interface Labels {
        long labelFor(long nodeId);

        void setLabelFor(long nodeId, long label);

        long size();
    }

    public static final PropertyTranslator.OfLong<Labels> LABEL_TRANSLATOR =
            Labels::labelFor;

    static final class LabelArray implements Labels {
        final long[] labels;

        LabelArray(final long[] labels) {
            this.labels = labels;
        }

        @Override
        public long labelFor(final long nodeId) {
            return labels[Math.toIntExact(nodeId)];
        }

        @Override
        public void setLabelFor(final long nodeId, final long label) {
            labels[Math.toIntExact(nodeId)] = label;
        }

        @Override
        public long size() {
            return (long) labels.length;
        }
    }

    static final class HugeLabelArray implements Labels {
        final HugeLongArray labels;

        HugeLabelArray(final HugeLongArray labels) {
            this.labels = labels;
        }

        @Override
        public long labelFor(final long nodeId) {
            return labels.get(nodeId);
        }

        @Override
        public void setLabelFor(final long nodeId, final long label) {
            labels.set(nodeId, label);
        }

        @Override
        public long size() {
            return labels.size();
        }
    }

    interface RandomProvider {
        Random randomForNewIteration();

        boolean isRandom();
    }

    private enum DefaultRandom implements RandomProvider {
        INSTANCE {
            @Override
            public Random randomForNewIteration() {
                return ThreadLocalRandom.current();
            }

            @Override
            public boolean isRandom() {
                return true;
            }
        }
    }

    private static final class ProvidedRandom implements RandomProvider {
        private final Random random;

        private ProvidedRandom(final Random random) {
            this.random = random;
        }

        @Override
        public Random randomForNewIteration() {
            return random;
        }

        @Override
        public boolean isRandom() {
            return true;
        }
    }
}
