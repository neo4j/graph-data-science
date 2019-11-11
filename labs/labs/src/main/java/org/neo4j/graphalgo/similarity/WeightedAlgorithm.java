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
package org.neo4j.graphalgo.similarity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.impl.similarity.NonRecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.WeightedInput;
import org.neo4j.graphalgo.impl.similarity.Weights;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class WeightedAlgorithm implements SimilarityAlgorithm<WeightedInput> {

    private final GraphDatabaseAPI api;
    protected final ProcedureConfiguration configuration;

    public WeightedAlgorithm(GraphDatabaseAPI api, ProcedureConfiguration configuration) {
        this.api = api;
        this.configuration = configuration;
    }

    @Override
    public WeightedInput[] prepareInputs(
            final Object rawData,
            final Double skipValue) throws Exception {
        if (ProcedureConstants.CYPHER_QUERY_KEY.equals(configuration.getGraphName("dense"))) {
            return prepareSparseWeights(api, (String) rawData,  skipValue, configuration);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            return WeightedInput.prepareDenseWeights(data, getDegreeCutoff(configuration), skipValue);
        }
    }

    private Long getDegreeCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("degreeCutoff", 0L).longValue();
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

    @Override
    public SimilarityRecorder<WeightedInput> similarityRecorder(
            final SimilarityComputer<WeightedInput> computer, final ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }

    @Override
    public Supplier<RleDecoder> createDecoderFactory(
            final WeightedInput input) {
        int size = input.initialSize();
        return createDecoderFactory(configuration.getGraphName("dense"), size);
    }

    private Supplier<RleDecoder> createDecoderFactory(String graphType, int size) {
        if(ProcedureConstants.CYPHER_QUERY_KEY.equals(graphType)) {
            return () -> new RleDecoder(size);
        }

        return () -> null;
    }
}
