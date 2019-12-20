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

package org.neo4j.graphalgo.impl.similarity.modern;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.RecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityStreamGenerator;
import org.neo4j.graphalgo.impl.similarity.TopKConsumer;
import org.neo4j.graphalgo.impl.similarity.WeightedInput;
import org.neo4j.graphalgo.impl.similarity.Weights;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.impl.similarity.SimilarityInput.indexesFor;

public class ModernCosineAlgorithm extends Algorithm<ModernCosineAlgorithm, ModernSimilarityAlgorithmResult> {

    private final ModernCosineConfig config;
    private final GraphDatabaseAPI api;

    public ModernCosineAlgorithm(ModernCosineConfig config, GraphDatabaseAPI api) {
        this.config = config;
        this.api = api;
    }

    @Override
    public ModernSimilarityAlgorithmResult compute() {
        ImmutableModernSimilarityAlgorithmResult.Builder builder = ImmutableModernSimilarityAlgorithmResult.builder();

        Double skipValue = config.skipValue();

        WeightedInput[] inputs = prepareWeights(config.data(), skipValue);

        long[] inputIds = SimilarityInput.extractInputIds(inputs);
        int[] sourceIndexIds = indexesFor(inputIds, config.sourceIds(), "sourceIds");
        int[] targetIndexIds = indexesFor(inputIds, config.targetIds(), "targetIds");
        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue, sourceIndexIds, targetIndexIds);
        builder.nodes(inputIds.length)
            .sourceIdsLength(sourceIndexIds.length)
            .targetIdsLength(targetIndexIds.length);

        if (inputs.length == 0) {
            return builder.stream(Stream.empty())
                .isEmpty(true)
                .build();
        }

        if (config.showComputations()) {
            RecordingSimilarityRecorder<WeightedInput> recorder = new RecordingSimilarityRecorder<>(computer);
            builder.computations(recorder);
            computer = recorder;
        }

        Stream<SimilarityResult> resultStream = generateWeightedStream(
            inputs,
            sourceIndexIds,
            targetIndexIds,
            config.normalizedSimilarityCutoff(),
            config.top(),
            config.topK(),
            computer
        );
        return builder.stream(resultStream)
            .isEmpty(false)
            .build();
    }

    @Override
    public ModernCosineAlgorithm me() {
        return this;
    }

    @Override
    public void release() {
    }

    private WeightedInput[] prepareWeights(Object rawData, Double skipValue) {
        if (ProcedureConstants.CYPHER_QUERY_KEY.equals(config.graph())) {
            return prepareSparseWeights(api, (String) rawData, skipValue);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            return WeightedInput.prepareDenseWeights(data, config.degreeCutoff(), skipValue);
        }
    }

    private WeightedInput[] prepareSparseWeights(GraphDatabaseAPI api, String query, Double skipValue) {
        Map<String, Object> params = config.params();
        long degreeCutoff = config.degreeCutoff();
        int repeatCutoff = config.sparseVectorRepeatCutoff();

        Result result = api.execute(query, params);

        Map<Long, LongDoubleMap> map = new HashMap<>();
        LongSet ids = new LongHashSet();
        result.accept(resultRow -> {
            long item = resultRow.getNumber("item").longValue();
            long id = resultRow.getNumber("category").longValue();
            ids.add(id);
            double weight = resultRow.getNumber("weight").doubleValue();
            map.compute(item, (key, agg) -> {
                if (agg == null) agg = new LongDoubleHashMap();
                agg.put(id, weight);
                return agg;
            });
            return true;
        });

        WeightedInput[] inputs = new WeightedInput[map.size()];
        int idx = 0;

        long[] idsArray = ids.toArray();
        for (Map.Entry<Long, LongDoubleMap> entry : map.entrySet()) {
            Long item = entry.getKey();
            LongDoubleMap sparseWeights = entry.getValue();

            if (sparseWeights.size() > degreeCutoff) {
                List<Number> weightsList = new ArrayList<>(ids.size());
                for (long id : idsArray) {
                    weightsList.add(sparseWeights.getOrDefault(id, skipValue));
                }
                int size = weightsList.size();
                int nonSkipSize = sparseWeights.size();
                double[] weights = Weights.buildRleWeights(weightsList, repeatCutoff);

                inputs[idx++] = WeightedInput.sparse(item, weights, size, nonSkipSize);
            }
        }

        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
        Arrays.sort(inputs);
        return inputs;
    }

    private SimilarityComputer<WeightedInput> similarityComputer(
        Double skipValue,
        int[] sourceIndexIds,
        int[] targetIndexIds
    ) {
        boolean bidirectional = sourceIndexIds.length == 0 && targetIndexIds.length == 0;
        return skipValue == null ?
            (decoder, s, t, cutoff) -> s.cosineSquares(decoder, cutoff, t, bidirectional) :
            (decoder, s, t, cutoff) -> s.cosineSquaresSkip(decoder, cutoff, t, skipValue, bidirectional);
    }

    Stream<SimilarityResult> generateWeightedStream(
        WeightedInput[] inputs,
        int[] sourceIndexIds, int[] targetIndexIds, double similarityCutoff, int topN, int topK,
        SimilarityComputer<WeightedInput> computer
    ) {
        Supplier<RleDecoder> decoderFactory = createDecoderFactory(inputs[0]);
        return topN(similarityStream(
            inputs,
            sourceIndexIds,
            targetIndexIds,
            computer,
            decoderFactory,
            similarityCutoff,
            topK
        ), topN)
            .map(SimilarityResult::squareRooted);
    }

    private Supplier<RleDecoder> createDecoderFactory(WeightedInput input) {
        if (ProcedureConstants.CYPHER_QUERY_KEY.equals(config.graph())) {
            return () -> new RleDecoder(input.initialSize());
        }
        return () -> null;
    }

    protected <T> Stream<SimilarityResult> similarityStream(
        T[] inputs,
        int[] sourceIndexIds,
        int[] targetIndexIds,
        SimilarityComputer<T> computer,
        Supplier<RleDecoder> decoderFactory,
        double cutoff,
        int topK
    ) {
        SimilarityStreamGenerator<T> generator = new SimilarityStreamGenerator<>(
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
}
