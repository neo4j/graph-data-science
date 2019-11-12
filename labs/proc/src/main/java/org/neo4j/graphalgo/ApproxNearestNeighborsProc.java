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

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.impl.nn.ApproxNearestNeighbors;
import org.neo4j.graphalgo.impl.results.ApproxSimilaritySummaryResult;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.AnnTopKConsumer;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.impl.similarity.SimilarityRecorder;
import org.neo4j.graphalgo.similarity.CosineAlgorithm;
import org.neo4j.graphalgo.similarity.EuclideanAlgorithm;
import org.neo4j.graphalgo.similarity.JaccardAlgorithm;
import org.neo4j.graphalgo.similarity.PearsonAlgorithm;
import org.neo4j.graphalgo.similarity.SimilarityAlgorithm;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class ApproxNearestNeighborsProc extends SimilarityProc {

    @Procedure(name = "algo.labs.ml.ann.stream", mode = READ)
    @Description("CALL algo.labs.ml.ann.stream('jaccard|cosine|pearson|euclidean', [{item:id, weights:[weights]} or {item:id, categories:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
                 "YIELD item1, item2, count1, count2, intersection, similarity - computes nearest neighbors")
    public Stream<SimilarityResult> stream(
            @Name(value = "algorithm", defaultValue = "null") String algorithmName,
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        Double skipValue = configuration.getSkipValue(Double.NaN);

        SimilarityAlgorithm<SimilarityInput> algorithm = selectAlgorithm(algorithmName, configuration);

        SimilarityInput[] inputs = algorithm.prepareInputs(rawData, skipValue);

        if (inputs.length == 0) {
            return Stream.empty();
        }

        int topN = getTopN(configuration);
        Supplier<RleDecoder> decoderFactory = algorithm.createDecoderFactory(inputs[0]);

        double similarityCutoff = algorithm.similarityCutoff();
        SimilarityComputer<SimilarityInput> computer = algorithm.similarityComputer(skipValue);

        ApproxNearestNeighbors<SimilarityInput> approxNearestNeighbors = new ApproxNearestNeighbors<>(
                configuration,
                inputs,
                similarityCutoff,
                decoderFactory,
                computer,
                algorithm.topK(),
                log);
        approxNearestNeighbors.compute();

        AnnTopKConsumer[] topKConsumers = approxNearestNeighbors.result();

        return topN(Arrays.stream(topKConsumers).flatMap(AnnTopKConsumer::stream), topN).map(algorithm::postProcess);
    }

    @Procedure(name = "algo.labs.ml.ann", mode = Mode.WRITE)
    @Description("CALL algo.labs.ml.ann('jaccard|cosine|pearson|euclidean', [{item:id, weights:[weights]} or {item:id, categories:[ids]}], {similarityCutoff:-1,degreeCutoff:0}) " +
                 "YIELD item1, item2, count1, count2, intersection, similarity - computes nearest neighbors")
    public Stream<ApproxSimilaritySummaryResult> write(
            @Name(value = "algorithm", defaultValue = "null") String algorithmName,
            @Name(value = "data", defaultValue = "null") Object rawData,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(config, getUsername());
        Double skipValue = configuration.getSkipValue(Double.NaN);

        SimilarityAlgorithm<SimilarityInput> algorithm = selectAlgorithm(algorithmName, configuration);

        SimilarityInput[] inputs = algorithm.prepareInputs(rawData, skipValue);

        String writeRelationshipType = configuration.get("writeRelationshipType", "SIMILAR");
        String writeProperty = configuration.getWriteProperty("score");
        if (inputs.length == 0) {
            return Stream.empty();
        }

        int topN = getTopN(configuration);
        Supplier<RleDecoder> decoderFactory = algorithm.createDecoderFactory(inputs[0]);

        double similarityCutoff = algorithm.similarityCutoff();
        SimilarityRecorder<SimilarityInput> recorder = algorithm.similarityRecorder(algorithm.similarityComputer(skipValue), configuration);

        ApproxNearestNeighbors<SimilarityInput> approxNearestNeighbors = new ApproxNearestNeighbors<>(
                configuration,
                inputs,
                similarityCutoff,
                decoderFactory,
                recorder,
                algorithm.topK(),
                log);
        try {
        approxNearestNeighbors.compute();

        AnnTopKConsumer[] topKConsumers = approxNearestNeighbors.result();

        Stream<SimilarityResult> stream = topN(Arrays.stream(topKConsumers).flatMap(AnnTopKConsumer::stream), topN).map(
                algorithm::postProcess);

        boolean write = configuration.isWriteFlag(false);

        return writeAndAggregateApproxResults(
                stream,
                inputs.length,
                configuration,
                write,
                writeRelationshipType,
                writeProperty,
                approxNearestNeighbors.iterations(),
                recorder); }
        catch(Exception e) {
            e.printStackTrace();
            return Stream.empty();

        }
    }

    private SimilarityAlgorithm selectAlgorithm(String algorithm, ProcedureConfiguration configuration) {
        switch (algorithm) {
            case "cosine":
                return new CosineAlgorithm(api, configuration);
            case "pearson":
                return new PearsonAlgorithm(api, configuration);
            case "jaccard":
                return new JaccardAlgorithm(configuration);
            case "euclidean":
                return new EuclideanAlgorithm(api, configuration);
            default:
                throw new IllegalArgumentException("Unknown algorithm: " + algorithm);
        }
    }


}
