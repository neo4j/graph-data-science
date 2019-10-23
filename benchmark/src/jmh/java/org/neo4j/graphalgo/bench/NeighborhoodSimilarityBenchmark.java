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

package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.SimilarityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarity;
import org.neo4j.graphalgo.impl.results.SimilarityResult;
import org.neo4j.graphalgo.impl.similarity.CategoricalInput;
import org.neo4j.graphalgo.impl.similarity.RleDecoder;
import org.neo4j.graphalgo.impl.similarity.SimilarityComputer;
import org.neo4j.graphalgo.impl.similarity.SimilarityStreamGenerator;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.SimilarityProc.prepareCategories;

@Threads(1)
@Fork(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class NeighborhoodSimilarityBenchmark {

    private GraphDatabaseAPI db;
    private Graph graph;
    private NeighborhoodSimilarity algo;

    private List<Map<String, Object>> jaccardInput;

    @Setup
    public void setup() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();

        JaccardBenchmark.createGraph(db);

        this.graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withDirection(Direction.OUTGOING)
                .load(HugeGraphFactory.class);

        this.algo = new NeighborhoodSimilarity(
                graph,
                NeighborhoodSimilarity.Config.DEFAULT,
                AllocationTracker.EMPTY,
                NullLog.getInstance());

        this.jaccardInput = new ArrayList<>();
        graph.forEachNode(nodeId -> {
            List<Number> targetIds = new ArrayList<>();
            graph.forEachRelationship(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId) -> {
                targetIds.add(targetNodeId);
                return true;
            });
            jaccardInput.add(MapUtil.map(String.valueOf(nodeId), targetIds));
            return true;
        });
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public Object _neighborhoodSimilarity() {
        return algo
                .run(Direction.OUTGOING)
                .collect(Collectors.toList());
    }


    @Benchmark
    public Object _jaccardSimilarity() {
        CategoricalInput[] inputs = prepareCategories(jaccardInput, 0);

        if (inputs.length == 0) {
            return Stream.empty();
        }

        int[] sourceIndexIds = new int[0];
        int[] targetIndexIds = new int[0];

        SimilarityComputer<CategoricalInput> computer = (decoder, s, t, cutoff) -> s.jaccard(cutoff, t, false);

        Stream<SimilarityResult> resultStream = SimilarityProc.topN(similarityStream(
                inputs,
                sourceIndexIds,
                targetIndexIds,
                computer,
                ProcedureConfiguration.empty(),
                () -> null,
                0.0,
                0), 0);

        return resultStream.collect(Collectors.toList());
    }

    protected <T> Stream<SimilarityResult> similarityStream(
            T[] inputs,
            int[] sourceIndexIds,
            int[] targetIndexIds,
            SimilarityComputer<T> computer,
            ProcedureConfiguration configuration,
            Supplier<RleDecoder> decoderFactory,
            double cutoff,
            int topK) {
        TerminationFlag terminationFlag = TerminationFlag.wrap((KernelTransaction) db.beginTx());

        SimilarityStreamGenerator<T> generator = new SimilarityStreamGenerator<>(
                terminationFlag,
                configuration,
                decoderFactory,
                computer);
        if (sourceIndexIds.length == 0 && targetIndexIds.length == 0) {
            return generator.stream(inputs, cutoff, topK);
        } else {
            return generator.stream(inputs, sourceIndexIds, targetIndexIds, cutoff, topK);
        }
    }
}
