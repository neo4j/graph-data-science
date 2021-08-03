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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.util.concurrent.ThreadLocalRandom;

public class NegativeSampleProducer {

    private final HugeLongArray contextNodeDistribution;
    private final long cumulativeProbability;

    public NegativeSampleProducer(
        HugeLongArray contextNodeDistribution
    ) {
        this.contextNodeDistribution = contextNodeDistribution;
        this.cumulativeProbability = contextNodeDistribution.get(contextNodeDistribution.size() - 1);
    }

    public long next() {
        long index = contextNodeDistribution.binarySearch(ThreadLocalRandom.current().nextLong(cumulativeProbability));

        if (index < contextNodeDistribution.size() - 1) {
            index++;
        }

        return index;
    }
}
