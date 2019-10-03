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

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.CategoricalInput;
import org.neo4j.graphalgo.impl.similarity.NonRecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RecordingSimilarityRecorder;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityRecorder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class JaccardAlgorithm implements SimilarityAlgorithm<CategoricalInput> {
    private final ProcedureConfiguration configuration;

    public JaccardAlgorithm(ProcedureConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public CategoricalInput[] prepareInputs(
            final Object rawData, final Double skipValue) throws Exception {
        List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
        long degreeCutoff = getDegreeCutoff(configuration);

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

    protected Long getDegreeCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("degreeCutoff", 0L).longValue();
    }

    @Override
    public double similarityCutoff() {
        return configuration.getNumber("similarityCutoff", -1D).doubleValue();
    }

    @Override
    public SimilarityComputer<CategoricalInput> similarityComputer(
            final Double skipValue) {
        return (decoder, s, t, cutoff) -> s.jaccard(cutoff, t, false);
    }

    @Override
    public SimilarityRecorder<CategoricalInput> similarityRecorder(
            final SimilarityComputer<CategoricalInput> computer, final ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }

    @Override
    public Supplier<RleDecoder> createDecoderFactory(
            final CategoricalInput input) {
        return () -> null;
    }

    @Override
    public SimilarityResult postProcess(final SimilarityResult result) {
        return result;
    }

    @Override
    public int topK() {
        return configuration.getNumber("topK", 3).intValue();
    }
}
