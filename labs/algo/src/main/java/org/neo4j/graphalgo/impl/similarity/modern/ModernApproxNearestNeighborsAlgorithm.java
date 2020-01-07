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
package org.neo4j.graphalgo.impl.similarity.modern;

import org.neo4j.graphalgo.impl.nn.ApproxNearestNeighbors;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.AnnTopKConsumer;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class ModernApproxNearestNeighborsAlgorithm<INPUT extends SimilarityInput> extends ModernSimilarityAlgorithm<ModernApproxNearestNeighborsAlgorithm<INPUT>, INPUT> {

    private final ModernApproximateNearestNeighborsConfig config;
    private final ModernSimilarityAlgorithm<?, INPUT> algorithm;
    private final Log log;

    public ModernApproxNearestNeighborsAlgorithm(
        ModernApproximateNearestNeighborsConfig config,
        ModernSimilarityAlgorithm<?, INPUT> algorithm,
        GraphDatabaseAPI api,
        Log log
    ) {
        super(config, api);
        this.config = config;
        this.algorithm = algorithm;
        this.log = log;
    }

    @Override
    INPUT[] prepareInputs(Object rawData, ModernSimilarityConfig config) {
        return algorithm.prepareInputs(rawData, config);
    }

    @Override
    protected Supplier<RleDecoder> createDecoderFactory(int size) {
        return algorithm.createDecoderFactory(size);
    }

    @Override
    Supplier<RleDecoder> inputDecoderFactory(INPUT[] inputs) {
        return algorithm.inputDecoderFactory(inputs);
    }

    @Override
    SimilarityComputer<INPUT> similarityComputer(
        Double skipValue,
        int[] sourceIndexIds,
        int[] targetIndexIds
    ) {
        return algorithm.similarityComputer(skipValue, sourceIndexIds, targetIndexIds);
    }

    @Override
    protected Stream<SimilarityResult> similarityStream(
        INPUT[] inputs,
        int[] sourceIndexIds,
        int[] targetIndexIds,
        SimilarityComputer<INPUT> computer,
        Supplier<RleDecoder> decoderFactory,
        double cutoff,
        int topK
    ) {
        ApproxNearestNeighbors<INPUT> approxNearestNeighbors = new ApproxNearestNeighbors<>(
            inputs,
            config.maxIterations(),
            config.precision(),
            config.p(),
            config.randomSeed(),
            config.sampling(),
            config.concurrency(),
            cutoff,
            decoderFactory,
            computer,
            topK,
            log
        );
        approxNearestNeighbors.compute();
        AnnTopKConsumer[] topKConsumers = approxNearestNeighbors.result();
        return Arrays.stream(topKConsumers).flatMap(AnnTopKConsumer::stream);
    }
}
