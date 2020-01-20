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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.results.SimilarityResult;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class SimilarityRecorder<T extends SimilarityInput> implements Computations, SimilarityComputer<T> {

    private final SimilarityComputer<T> computer;

    private final LongAdder computations = new LongAdder();
    private final Map<Set<Long>, AtomicLong> comparisons = new ConcurrentHashMap<>();

    public SimilarityRecorder(SimilarityComputer<T> computer) {
        this.computer = computer;
    }

    @Override
    public long count() {
        return computations.longValue();
    }

    public Map<Set<Long>, AtomicLong> comparisons() {
        return comparisons;
    }

    @Override
    public SimilarityResult similarity(RleDecoder decoder, T source, T target, double cutoff) {

        computations.increment();
        return computer.similarity(decoder, source, target, cutoff);
    }
}

