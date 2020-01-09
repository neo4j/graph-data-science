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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Comparator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.similarity.SimilarityInput.indexesFor;

public abstract class SimilarityAlgorithm<ME extends SimilarityAlgorithm<ME, INPUT>, INPUT extends SimilarityInput> extends Algorithm<ME, SimilarityAlgorithmResult> {

    final SimilarityConfig config;
    final GraphDatabaseAPI api;

    public SimilarityAlgorithm(SimilarityConfig config, GraphDatabaseAPI api) {
        this.config = config;
        this.api = api;
    }

    @Override
    public SimilarityAlgorithmResult compute() {
        ImmutableSimilarityAlgorithmResult.Builder builder = ImmutableSimilarityAlgorithmResult.builder();

        INPUT[] inputs = prepareInputs(config.data(), config);
        long[] inputIds = SimilarityInput.extractInputIds(inputs);
        int[] sourceIndexIds = indexesFor(inputIds, config.sourceIds(), "sourceIds");
        int[] targetIndexIds = indexesFor(inputIds, config.targetIds(), "targetIds");
        SimilarityComputer<INPUT> computer = similarityComputer(config.skipValue(), sourceIndexIds, targetIndexIds);

        builder.nodes(inputIds.length)
            .sourceIdsLength(sourceIndexIds.length)
            .targetIdsLength(targetIndexIds.length);

        if (inputs.length == 0) {
            return builder
                .stream(Stream.empty())
                .isEmpty(true)
                .build();
        }

        if (config.showComputations()) {
            RecordingSimilarityRecorder<INPUT> recorder = new RecordingSimilarityRecorder<>(computer);
            builder.computations(recorder);
            computer = recorder;
        }

        Stream<SimilarityResult> resultStream = generateWeightedStream(
            inputs,
            sourceIndexIds,
            targetIndexIds,
            config.normalizedSimilarityCutoff(),
            config.normalizedTopN(),
            config.normalizedTopK(),
            computer
        );

        return builder
            .stream(resultStream)
            .isEmpty(false)
            .build();
    }

    @Override
    public ME me() {
        return (ME) this;
    }

    @Override
    public void release() {}

    abstract INPUT[] prepareInputs(Object rawData, SimilarityConfig config);

    abstract SimilarityComputer<INPUT> similarityComputer(
        Double skipValue,
        int[] sourceIndexIds,
        int[] targetIndexIds
    );

    SimilarityResult modifyResult(SimilarityResult result) {
        return result;
    }

    abstract Supplier<RleDecoder> inputDecoderFactory(INPUT[] inputs);

    Stream<SimilarityResult> generateWeightedStream(
        INPUT[] inputs,
        int[] sourceIndexIds,
        int[] targetIndexIds,
        double similarityCutoff,
        int topN,
        int topK,
        SimilarityComputer<INPUT> computer
    ) {
        Supplier<RleDecoder> decoderFactory = inputDecoderFactory(inputs);
        return topN(
            similarityStream(
                inputs,
                sourceIndexIds,
                targetIndexIds,
                computer,
                decoderFactory,
                similarityCutoff,
                topK
            ),
            topN
        ).map(this::modifyResult);
    }

    protected Supplier<RleDecoder> createDecoderFactory(int size) {
        if (ProcedureConstants.CYPHER_QUERY_KEY.equals(config.graph())) {
            return () -> new RleDecoder(size);
        }
        return () -> null;
    }

    public static Stream<SimilarityResult> topN(Stream<SimilarityResult> stream, int topN) {
        if (topN == 0) {
            return stream;
        }
        Comparator<SimilarityResult> comparator = topN > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topN = Math.abs(topN);

        if (topN > 10000) {
            return stream.sorted(comparator).limit(topN);
        }
        return TopKConsumer.topK(stream, topN, comparator);
    }

    protected Stream<SimilarityResult> similarityStream(
        INPUT[] inputs,
        int[] sourceIndexIds,
        int[] targetIndexIds,
        SimilarityComputer<INPUT> computer,
        Supplier<RleDecoder> decoderFactory,
        double cutoff,
        int topK
    ) {
        SimilarityStreamGenerator<INPUT> generator = new SimilarityStreamGenerator<>(
            terminationFlag,
            config.concurrency(),
            decoderFactory,
            computer
        );
        if (sourceIndexIds.length == 0 && targetIndexIds.length == 0) {
            return generator.stream(inputs, cutoff, topK);
        } else {
            return generator.stream(inputs, sourceIndexIds, targetIndexIds, cutoff, topK);
        }
    }
}
