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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagationFactory;
import org.neo4j.graphalgo.impl.results.LabelPropagationStats;
import org.neo4j.graphalgo.impl.results.LabelPropagationStats.StreamResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.ArrayList;
import java.util.Map;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public final class LabelPropagationProc extends BaseAlgoProc<LabelPropagation> {

    private static final String CONFIG_WEIGHT_KEY = "weightProperty";
    private static final String CONFIG_WRITE_KEY = "writeProperty";
    private static final String CONFIG_SEED_KEY = "seedProperty";
    private static final String CONFIG_OLD_SEED_KEY = "partitionProperty";
    private static final Boolean DEFAULT_WRITE = Boolean.TRUE;

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.labelPropagation", mode = Mode.WRITE)
    @Description("CALL algo.labelPropagation(" +
                 "label:String, relationship:String, " +
                 "{iterations: 1, direction: 'OUTGOING', weightProperty: 'weight', seedProperty: 'seed', write: true, concurrency: 4}) " +
                 "YIELD nodes, iterations, didConverge, loadMillis, computeMillis, writeMillis, write, weightProperty, seedProperty - " +
                 "simple label propagation kernel")
    public Stream<LabelPropagationStats> labelPropagation(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "null") Map<String, Object> config) {

        final LabelPropagationStats.Builder stats = new LabelPropagationStats.Builder();

        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);

        final String writeProperty = configuration.getString(CONFIG_WRITE_KEY, null, CONFIG_OLD_SEED_KEY);
        final String seedProperty = configuration.getString(CONFIG_SEED_KEY, null, CONFIG_OLD_SEED_KEY);
        final String weightProperty = configuration.getString(CONFIG_WEIGHT_KEY, null);

        stats
                .seedProperty(seedProperty)
                .weightProperty(weightProperty)
                .writeProperty(writeProperty);

        if (configuration.isWriteFlag(DEFAULT_WRITE) && writeProperty == null) {
            throw new IllegalArgumentException(String.format("Write property '%s' not specified", CONFIG_WRITE_KEY));
        }

        Graph graph = this.loadGraph(configuration, tracker, stats);
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.of(LabelPropagationStats.EMPTY);
        }

        final LabelPropagation.Labels labels = compute(stats, tracker, configuration, graph);
        if (configuration.isWriteFlag()) {
            stats.withWrite(true);
            write(configuration.getWriteConcurrency(), writeProperty, graph, labels, stats);
        }

        return Stream.of(stats.build(tracker, graph.nodeCount(), labels::get));
    }

    @Procedure(value = "algo.labelPropagation.stream")
    @Description("CALL algo.labelPropagation.stream(label:String, relationship:String, config:Map<String, Object>) YIELD " +
                 "nodeId, label")
    public Stream<StreamResult> labelPropagationStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final LabelPropagationStats.Builder stats = new LabelPropagationStats.Builder();

        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);

        Graph graph = this.loadGraph(configuration, tracker, stats);
        if (graph.nodeCount() == 0) {
            graph.release();
            return Stream.empty();
        }

        final LabelPropagation.Labels labels = compute(stats, tracker, configuration, graph);
        return LongStream.range(0L, labels.size())
                .mapToObj(i -> new StreamResult(graph.toOriginalNodeId(i), labels.labelFor(i)));
    }

    private PropertyMapping[] createPropertyMappings(String seedProperty, String weightProperty) {
        ArrayList<PropertyMapping> propertyMappings = new ArrayList<>();
        if (seedProperty != null) {
            propertyMappings.add(PropertyMapping.of(LabelPropagation.SEED_TYPE, seedProperty, 0D));
        }
        if (weightProperty != null) {
            propertyMappings.add(PropertyMapping.of(LabelPropagation.WEIGHT_TYPE, weightProperty, 1D));
        }
        return propertyMappings.toArray(new PropertyMapping[0]);
    }

    @Override
    AlgorithmFactory<LabelPropagation> algorithmFactory(final ProcedureConfiguration config) {
        return new LabelPropagationFactory();
    }

    @Override
    GraphLoader configureLoader(final GraphLoader loader, final ProcedureConfiguration config) {
        final String seedProperty = config.getString(CONFIG_SEED_KEY, null, CONFIG_OLD_SEED_KEY);
        final String weightProperty = config.getString(CONFIG_WEIGHT_KEY, null);

        Direction direction = config.getDirection(Direction.OUTGOING);
        if (direction == Direction.BOTH) {
            loader.asUndirected(true);
        } else {
            loader.withDirection(direction);
        }

        return loader
                .withOptionalRelationshipWeightsFromProperty(weightProperty, 1.0d)
                .withOptionalNodeProperties(createPropertyMappings(seedProperty, weightProperty));
    }

    private LabelPropagation.Labels compute(
            final LabelPropagationStats.Builder stats,
            final AllocationTracker tracker,
            final ProcedureConfiguration configuration,
            final Graph graph) {

        LabelPropagation algo = newAlgorithm(graph, configuration, tracker);

        final LabelPropagation.Labels algoResult = runWithExceptionLogging(
                "LabelPropagation failed",
                () -> stats.timeEval(() -> algo.compute(configuration.getDirection(Direction.OUTGOING), configuration.getIterations(1)))).labels();

        stats
                .iterations(algo.ranIterations())
                .didConverge(algo.didConverge());

        algo.release();
        graph.release();

        return algoResult;
    }

    private void write(
            int concurrency,
            String labelKey,
            Graph graph,
            LabelPropagation.Labels labels,
            LabelPropagationStats.Builder stats) {
        try (ProgressTimer ignored = stats.timeWrite()) {
            Exporter.of(dbAPI, graph)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, concurrency, TerminationFlag.wrap(transaction))
                    .build()
                    .write(
                            labelKey,
                            labels,
                            LabelPropagation.LABEL_TRANSLATOR
                    );
        }
    }
}
