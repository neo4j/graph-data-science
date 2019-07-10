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
package org.neo4j.graphalgo.impl.unionfind;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.stream.Collectors;

public class UnionFindFactory<A extends GraphUnionFindAlgo<A>> extends AlgorithmFactory<A> {

    public static final String CONFIG_PARALLEL_ALGO = "parallel_algo";

    private final UnionFindAlgorithmType algorithmType;
    private final double threshold;

    public UnionFindFactory(UnionFindAlgorithmType algorithmType, double threshold) {
        this.algorithmType = algorithmType;
        this.threshold = threshold;
    }

    public static <T extends GraphUnionFindAlgo<T>> UnionFindFactory<T> create(ProcedureConfiguration configuration, double threshold) {
        UnionFindAlgorithmType result;
        if (configuration.getConcurrency() <= 1) {
            result = UnionFindAlgorithmType.SEQ;
        } else {
            final String algoName = configuration.getString(CONFIG_PARALLEL_ALGO, UnionFindAlgorithmType.QUEUE.name()).toUpperCase();

            UnionFindAlgorithmType algoImpl;
            try {
                algoImpl = UnionFindAlgorithmType.valueOf(algoName);
            } catch (IllegalArgumentException e) {
                algoImpl = UnionFindAlgorithmType.SEQ;
            }

            if (algoImpl == UnionFindAlgorithmType.SEQ) {
                String errorMsg = String.format("Parallel configuration %s is invalid. Valid names are %s", algoName,
                        Arrays.stream(UnionFindAlgorithmType.values())
                                .filter(ufa -> ufa != UnionFindAlgorithmType.SEQ)
                                .map(UnionFindAlgorithmType::name)
                                .collect(Collectors.joining(", ")));
                throw new IllegalArgumentException(errorMsg);
            }
            result = algoImpl;
        }
        return new UnionFindFactory<>(result, threshold);
    }

    @SuppressWarnings("unchecked")
    @Override
    public A build(
            final Graph graph,
            final ProcedureConfiguration configuration,
            final AllocationTracker tracker,
            final Log log) {
        int concurrency = configuration.getConcurrency();
        int minBatchSize = configuration.getBatchSize();
        final GraphUnionFindAlgo<?> algo = algorithmType.create(
                graph,
                Pools.DEFAULT,
                minBatchSize,
                concurrency,
                threshold,
                tracker);
        return (A) algo;
    }

    @Override
    public MemoryEstimation memoryEstimation() {
        return algorithmType.memoryEstimation();
    }
}
