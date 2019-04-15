/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
import org.neo4j.graphalgo.similarity.recorder.SimilarityRecorder;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.similarity.SimilarityInput.indexesFor;

public class CosineProc extends SimilarityProc {

    @Procedure(name = "algo.similarity.cosine.stream", mode = Mode.READ)
    @Description("CALL algo.similarity.cosine.stream([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD item1, item2, count1, count2, intersection, similarity - computes cosine distance")
    // todo count1,count2 = could be the non-null values, intersection the values where both are non-null?
    public Stream<SimilarityResult> cosineStream(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", Double.NaN);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        if(inputs.length == 0) {
            return Stream.empty();
        }

        long[] inputIds = SimilarityInput.extractInputIds(inputs);
        int[] sourceIndexIds = indexesFor(inputIds, configuration, "sourceIds");
        int[] targetIndexIds = indexesFor(inputIds, configuration, "targetIds");
        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue, sourceIndexIds, targetIndexIds);

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);

        return generateWeightedStream(configuration, inputs, sourceIndexIds, targetIndexIds,  similarityCutoff, topN, topK, computer);
    }



    @Procedure(name = "algo.similarity.cosine", mode = Mode.WRITE)
    @Description("CALL algo.similarity.cosine([{item:id, weights:[weights]}], {similarityCutoff:-1,degreeCutoff:0}) " +
            "YIELD p50, p75, p90, p99, p999, p100 - computes cosine similarities")
    public Stream<SimilaritySummaryResult> cosine(
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        Double skipValue = configuration.get("skipValue", Double.NaN);

        WeightedInput[] inputs = prepareWeights(rawData, configuration, skipValue);

        String writeRelationshipType = configuration.get("writeRelationshipType", "SIMILAR");
        String writeProperty = configuration.getWriteProperty("score");
        if(inputs.length == 0) {
            return emptyStream(writeRelationshipType, writeProperty);
        }

        long[] inputIds = SimilarityInput.extractInputIds(inputs);
        int[] sourceIndexIds = indexesFor(inputIds, configuration, "sourceIds");
        int[] targetIndexIds = indexesFor(inputIds, configuration, "targetIds");

        SimilarityComputer<WeightedInput> computer = similarityComputer(skipValue, sourceIndexIds, targetIndexIds);
        SimilarityRecorder<WeightedInput> recorder = similarityRecorder(computer, configuration);

        double similarityCutoff = similarityCutoff(configuration);
        int topN = getTopN(configuration);
        int topK = getTopK(configuration);


        Stream<SimilarityResult> stream = generateWeightedStream(configuration, inputs, sourceIndexIds, targetIndexIds, similarityCutoff, topN, topK, recorder);

        boolean write = configuration.isWriteFlag(false) && similarityCutoff > 0.0;
        return writeAndAggregateResults(stream, inputs.length, sourceIndexIds.length, targetIndexIds.length, configuration, write, writeRelationshipType, writeProperty, recorder);
    }

    private SimilarityComputer<WeightedInput> similarityComputer(Double skipValue, int[] sourceIndexIds, int[] targetIndexIds) {
        boolean bidirectional = sourceIndexIds.length == 0 && targetIndexIds.length == 0;

        return skipValue == null ?
                (decoder, s, t, cutoff) -> s.cosineSquares(decoder, cutoff, t, bidirectional) :
                (decoder, s, t, cutoff) -> s.cosineSquaresSkip(decoder, cutoff, t, skipValue, bidirectional);
    }

    Stream<SimilarityResult> generateWeightedStream(ProcedureConfiguration configuration, WeightedInput[] inputs,
                                                    int[] sourceIndexIds, int[] targetIndexIds, double similarityCutoff, int topN, int topK,
                                                    SimilarityComputer<WeightedInput> computer) {
        Supplier<RleDecoder> decoderFactory = createDecoderFactory(configuration, inputs[0]);
        return topN(similarityStream(inputs, sourceIndexIds, targetIndexIds, computer, configuration, decoderFactory, similarityCutoff, topK), topN)
                .map(SimilarityResult::squareRooted);
    }

    private double similarityCutoff(ProcedureConfiguration configuration) {
        double similarityCutoff = getSimilarityCutoff(configuration);
        // as we don't compute the sqrt until the end
        if (similarityCutoff > 0d) similarityCutoff *= similarityCutoff;
        return similarityCutoff;
    }


}
