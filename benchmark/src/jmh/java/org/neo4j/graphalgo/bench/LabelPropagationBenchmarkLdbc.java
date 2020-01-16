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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.helper.ldbc.LdbcDownloader;
import org.neo4j.graphalgo.labelpropagation.ImmutableLabelPropagationStreamConfig;
import org.neo4j.graphalgo.labelpropagation.LabelPropagation;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms16g", "-Xmx16g", "-XX:+UseG1GC"})
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 4, time = 10)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.SECONDS)
public class LabelPropagationBenchmarkLdbc {

    private static final String SEED_PROPERTY = "seed";
    private static final String WEIGHT_PROPERTY = "weight";

    @Param({"15"})
    int iterations;

    private GraphDatabaseAPI db;
    private Graph graph;

    @Setup
    public void setup() throws IOException {
        db = LdbcDownloader.openDb("L10:8G");
        graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
                .withRelationshipProperties(PropertyMapping.of("weight", 1.0D))
                .withOptionalNodeProperties(
                        PropertyMapping.of(WEIGHT_PROPERTY, WEIGHT_PROPERTY, 1.0),
                        PropertyMapping.of(SEED_PROPERTY, SEED_PROPERTY, 0.0)
                )
                .withExecutorService(Pools.DEFAULT)
                .load(HugeGraphFactory.class);
    }

    @TearDown
    public void shutdown() {
        db.shutdown();
        Pools.DEFAULT.shutdownNow();
    }

    @Benchmark
    public LabelPropagation lpa() {
        return new LabelPropagation(
            graph,
            ImmutableLabelPropagationStreamConfig.builder()
                .seedProperty(SEED_PROPERTY)
                .weightProperty(WEIGHT_PROPERTY)
                .maxIterations(iterations)
                .build(),
            Pools.DEFAULT,
            AllocationTracker.EMPTY
        ).compute();
    }
}
