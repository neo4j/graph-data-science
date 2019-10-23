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
package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.results.ApproxSimilaritySummaryResult;
import org.neo4j.graphalgo.impl.results.SimilarityExporter;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.results.SimilaritySummaryResult;
import org.neo4j.graphalgo.impl.similarity.CategoricalInput;
import org.neo4j.graphalgo.impl.similarity.Computations;
import org.neo4j.graphalgo.impl.similarity.NonRecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityRecorder;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SimilarityProc extends LabsProc {

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

    protected static SimilarityRecorder<WeightedInput> similarityRecorder(SimilarityComputer<WeightedInput> computer, ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }


    protected static SimilarityRecorder<CategoricalInput> categoricalSimilarityRecorder(
            SimilarityComputer<CategoricalInput> computer,
            ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }

    protected Long getDegreeCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("degreeCutoff", 0L).longValue();
    }

    Long getWriteBatchSize(ProcedureConfiguration configuration) {
        return configuration.get("writeBatchSize", 10000L);
    }

    Stream<SimilaritySummaryResult> writeAndAggregateResults(Stream<SimilarityResult> stream, int length, int sourceIdsLength, int targetIdsLength, ProcedureConfiguration configuration, boolean write, String writeRelationshipType, String writeProperty, Computations computations) {
        long writeBatchSize = getWriteBatchSize(configuration);
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (write) {
            TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty, terminationFlag, Pools.DEFAULT);
            similarityExporter.export(stream.peek(recorder), writeBatchSize);
        } else {
            stream.forEach(recorder);
        }

        return Stream.of(SimilaritySummaryResult.from(length, sourceIdsLength, targetIdsLength, similarityPairs, computations.count(), writeRelationshipType, writeProperty, write, histogram));
    }

    Stream<ApproxSimilaritySummaryResult> writeAndAggregateApproxResults(
            Stream<SimilarityResult> stream,
            int length,
            ProcedureConfiguration configuration,
            boolean write,
            String writeRelationshipType,
            String writeProperty,
            long iterations,
            Computations computations) {
        long writeBatchSize = getWriteBatchSize(configuration);
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (write) {
            TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty, terminationFlag, Pools.DEFAULT);
            similarityExporter.export(stream.peek(recorder), writeBatchSize);
        } else {
            stream.forEach(recorder);
        }

        return Stream.of(ApproxSimilaritySummaryResult.from(length,
                similarityPairs, computations.count(), writeRelationshipType, writeProperty, write, iterations, histogram));
    }

    protected Stream<SimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(SimilaritySummaryResult.from(0, 0,0, new AtomicLong(0), -1, writeRelationshipType,
                writeProperty, false, new DoubleHistogram(5)));
    }

    protected Stream<ApproxSimilaritySummaryResult> emptyApproxStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(ApproxSimilaritySummaryResult.from(0, new AtomicLong(0), -1,
                writeRelationshipType,
                writeProperty, false, -1, new DoubleHistogram(5)));
    }

    protected static Double getSimilarityCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("similarityCutoff", -1D).doubleValue();
    }

    protected <T> Stream<SimilarityResult> similarityStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, SimilarityComputer<T> computer, ProcedureConfiguration configuration, Supplier<RleDecoder> decoderFactory, double cutoff, int topK) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);

        SimilarityStreamGenerator<T> generator = new SimilarityStreamGenerator<>(terminationFlag, configuration, decoderFactory, computer);
        if (sourceIndexIds.length == 0 && targetIndexIds.length == 0) {
            return generator.stream(inputs, cutoff, topK);
        } else {
            return generator.stream(inputs, sourceIndexIds, targetIndexIds, cutoff, topK);
        }
    }

    protected CategoricalInput[] prepareCategories(List<Map<String, Object>> data, long degreeCutoff) {
        CategoricalInput[] ids = new CategoricalInput[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            List<Number> targetIds = SimilarityInput.extractValues(row.get("categories"));
            int size = targetIds.size();
            if (size > degreeCutoff) {
                long[] targets = new long[size];
                int i = 0;
                for (Number id : targetIds) {
                    targets[i++] = id.longValue();
                }
                Arrays.sort(targets);
                ids[idx++] = new CategoricalInput((Long) row.get("item"), targets);
            }
        }
        if (idx != ids.length) ids = Arrays.copyOf(ids, idx);
        Arrays.sort(ids);
        return ids;
    }

    protected WeightedInput[] prepareWeights(Object rawData, ProcedureConfiguration configuration, Double skipValue) throws Exception {
        if (ProcedureConstants.CYPHER_QUERY_KEY.equals(configuration.getGraphName("dense"))) {
            return prepareSparseWeights(api, (String) rawData,  skipValue, configuration);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            return WeightedInput.prepareDenseWeights(data, getDegreeCutoff(configuration), skipValue);
        }
    }

    protected Double readSkipValue(ProcedureConfiguration configuration) {
        return configuration.get("skipValue", Double.NaN);
    }

    private WeightedInput[] prepareSparseWeights(GraphDatabaseAPI api, String query, Double skipValue, ProcedureConfiguration configuration) throws Exception {
        Map<String, Object> params = configuration.getParams();
        Long degreeCutoff = getDegreeCutoff(configuration);
        int repeatCutoff = configuration.get("sparseVectorRepeatCutoff", Weights.REPEAT_CUTOFF).intValue();

        Result result = api.execute(query, params);

        Map<Long, LongDoubleMap> map = new HashMap<>();
        LongSet ids = new LongHashSet();
        result.accept((Result.ResultVisitor<Exception>) resultRow -> {
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

    protected int getTopK(ProcedureConfiguration configuration) {
        return configuration.getInt("topK", 0);
    }

    protected int getTopN(ProcedureConfiguration configuration) {
        return configuration.getInt("top", 0);
    }

    private Supplier<RleDecoder> createDecoderFactory(String graphType, int size) {
        if(ProcedureConstants.CYPHER_QUERY_KEY.equals(graphType)) {
            return () -> new RleDecoder(size);
        }

        return () -> null;
    }


    protected Supplier<RleDecoder> createDecoderFactory(ProcedureConfiguration configuration, WeightedInput input) {
        int size = input.initialSize();
        return createDecoderFactory(configuration.getGraphName("dense"), size);
    }


}
