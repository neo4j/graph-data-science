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
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.loading.NullWeightMap;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagation;
import org.neo4j.graphalgo.impl.labelprop.LabelPropagationFactory;
import org.neo4j.graphalgo.impl.results.LabelPropagationStats;
import org.neo4j.graphalgo.impl.results.LabelPropagationStats.BetaStreamResult;
import org.neo4j.graphalgo.impl.results.LabelPropagationStats.StreamResult;
import org.neo4j.graphalgo.impl.results.MemRecResult;
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
import java.util.Objects;
import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@SuppressWarnings("unused")
public final class LabelPropagationProc extends BaseAlgoProc<LabelPropagation> {

    private static final String CONFIG_WEIGHT_KEY = "weightProperty";
    private static final String CONFIG_WRITE_KEY = "writeProperty";
    private static final String CONFIG_SEED_KEY = "seedProperty";
    private static final String CONFIG_OLD_SEED_KEY = "partitionProperty";
    private static final Boolean DEFAULT_WRITE = Boolean.TRUE;
    private static final int DEFAULT_ITERATIONS = 10;

    @SuppressWarnings("WeakerAccess")
    @Context
    public GraphDatabaseAPI dbAPI;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.beta.labelPropagation", mode = Mode.WRITE)
    @Description("CALL algo.beta.labelPropagation(" +
                 "label:String, relationship:String, " +
                 "{iterations: 10, direction: 'OUTGOING', weightProperty: 'weight', seedProperty: 'seed', write: true, concurrency: 4}) " +
                 "YIELD nodes, iterations, didConverge, loadMillis, computeMillis, writeMillis, write, weightProperty, seedProperty")
    public Stream<LabelPropagationStats> betaLabelPropagation(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "null") Map<String, Object> config) {

        return run(label, relationshipType, config);
    }

    @Procedure(value = "algo.beta.labelPropagation.stream")
    @Description("CALL algo.beta.labelPropagation.stream(label:String, relationship:String, " +
                 "{iterations: 10, direction: 'OUTGOING', weightProperty: 'weight', seedProperty: 'seed', concurrency: 4}) " +
                 "YIELD nodeId, community")
    public Stream<BetaStreamResult> betaLabelPropagationStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        return stream(label, relationshipType, config);
    }

    @Deprecated
    @Procedure(name = "algo.labelPropagation", mode = Mode.WRITE, deprecatedBy = "algo.beta.labelPropagation")
    @Description("CALL algo.labelPropagation(" +
                 "label:String, relationship:String, direction:String, " +
                 "{iterations: 10, weightProperty: 'weight', seedProperty: 'seed', write: true, concurrency: 4}) " +
                 "YIELD nodes, iterations, didConverge, loadMillis, computeMillis, writeMillis, write, weightProperty, seedProperty - " +
                 "simple label propagation kernel")
    public Stream<LabelPropagationStats> labelPropagation(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "null") Object directionOrConfig,
            @Name(value = "deprecatedConfig", defaultValue = "{}") Map<String, Object> config) {
        Map<String, Object> rawConfig = config;
        if (directionOrConfig == null) {
            if (!config.isEmpty()) {
                rawConfig.put("direction", "OUTGOING");
            }
        } else if (directionOrConfig instanceof Map) {
            rawConfig = (Map<String, Object>) directionOrConfig;
        } else if (directionOrConfig instanceof String) {
            rawConfig.put("direction", directionOrConfig);
        }

        return betaLabelPropagation(label, relationshipType, rawConfig);
    }

    @Deprecated
    @Procedure(value = "algo.labelPropagation.stream", deprecatedBy = "algo.beta.labelPropagation.stream")
    @Description("CALL algo.labelPropagation.stream(label:String, relationship:String, " +
                 "{iterations: 10, direction: 'OUTGOING', weightProperty: 'weight', seedProperty: 'seed', concurrency: 4}) " +
                 "YIELD nodeId, label")
    public Stream<StreamResult> labelPropagationStream(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        return betaLabelPropagationStream(label, relationshipType, config).map(StreamResult::new);
    }

    @Procedure(value = "algo.labelPropagation.memRec")
    @Description("CALL algo.labelPropagation.memRec(label:String, relationship:String, config:Map<String, Object>) " +
                 "YIELD nodeId, label")
    public Stream<MemRecResult> labelPropagationMemrec(
            @Name(value = "label", defaultValue = "") String label,
            @Name(value = "relationship", defaultValue = "") String relationshipType,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
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
    protected AlgorithmFactory<LabelPropagation> algorithmFactory(final ProcedureConfiguration config) {
        return new LabelPropagationFactory();
    }

    @Override
    protected GraphLoader configureAlgoLoader(final GraphLoader loader, final ProcedureConfiguration config) {
        return loader
                .withReducedRelationshipLoading(config.getDirection(Direction.OUTGOING))
                .withOptionalNodeProperties(createPropertyMappings(
                        config.getString(CONFIG_SEED_KEY, CONFIG_OLD_SEED_KEY, null),
                        config.getString(CONFIG_WEIGHT_KEY, null)));
    }

    @Override
    protected double getDefaultWeightProperty(ProcedureConfiguration config) {
        return LabelPropagation.DEFAULT_WEIGHT;
    }

    private Stream<LabelPropagationStats> run(
            final String label,
            final String relationshipType,
            final Map<String, Object> config) {
        ProcedureSetup setup = setup(label, relationshipType, config);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.of(LabelPropagationStats.EMPTY);
        }

        if (setup.procedureConfig.isWriteFlag(DEFAULT_WRITE) && setup.procedureConfig.getWriteProperty() == null) {
            throw new IllegalArgumentException(String.format("Write property '%s' not specified", CONFIG_WRITE_KEY));
        }

        final HugeLongArray labels = compute(setup);

        if (setup.procedureConfig.isWriteFlag()) {
            String seedProperty = setup.procedureConfig.getString(CONFIG_SEED_KEY, CONFIG_OLD_SEED_KEY, null);
            setup.statsBuilder.withWrite(true);
            write(
                    setup.procedureConfig.getWriteConcurrency(),
                    setup.procedureConfig.getWriteProperty(),
                    seedProperty,
                    setup.graph,
                    labels,
                    setup.statsBuilder);

            setup.graph.releaseProperties();
        }

        return Stream.of(setup.statsBuilder.build(setup.tracker, setup.graph.nodeCount(), labels::get));
    }

    private Stream<BetaStreamResult> stream(
            final String label,
            final String relationshipType,
            final Map<String, Object> config) {
        ProcedureSetup setup = setup(label, relationshipType, config);

        if (setup.graph.isEmpty()) {
            setup.graph.release();
            return Stream.empty();
        }

        final HugeLongArray labels = compute(setup);

        setup.graph.releaseProperties();

        return LongStream.range(0L, labels.size())
                .mapToObj(i -> new BetaStreamResult(setup.graph.toOriginalNodeId(i), labels.get(i)));
    }

    private ProcedureSetup setup(
            String label,
            String relationshipType,
            Map<String, Object> config) {

        final LabelPropagationStats.Builder statsBuilder = new LabelPropagationStats.Builder(callContext.outputFields());

        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);

        statsBuilder
                .seedProperty(configuration.getString(CONFIG_SEED_KEY, CONFIG_OLD_SEED_KEY, null))
                .weightProperty(configuration.getString(CONFIG_WEIGHT_KEY, null))
                .writeProperty(configuration.getString(CONFIG_WRITE_KEY, CONFIG_OLD_SEED_KEY, null));

        Graph graph = this.loadGraph(configuration, tracker, statsBuilder);

        return new ProcedureSetup(graph, tracker, configuration, statsBuilder);
    }

    private HugeLongArray compute(ProcedureSetup setup) {

        LabelPropagation algo = newAlgorithm(setup.graph, setup.procedureConfig, setup.tracker);

        Direction procedureDirection = setup.procedureConfig.getDirection(Direction.OUTGOING);
        Optional<Direction> computeDirection = setup.graph.compatibleDirection(procedureDirection);

        if (!computeDirection.isPresent()) {
            throw new IllegalArgumentException(String.format(
                    "Incompatible directions between loaded graph and requested compute direction. Load direction: '%s' Compute direction: '%s'",
                    setup.graph.getLoadDirection(),
                    procedureDirection));
        }

        final HugeLongArray algoResult = runWithExceptionLogging(
                "LabelPropagation failed",
                () -> setup.statsBuilder.timeEval(() -> algo.compute(
                        computeDirection.get(),
                        setup.procedureConfig.getIterations(DEFAULT_ITERATIONS)))).labels();

        setup.statsBuilder
                .iterations(algo.ranIterations())
                .didConverge(algo.didConverge());

        algo.release();
        setup.graph.releaseTopology();

        return algoResult;
    }

    private void write(
            int concurrency,
            String writeProperty,
            String seedProperty,
            Graph graph,
            HugeLongArray labels,
            LabelPropagationStats.Builder stats) {
        log.debug("Writing results");

        try (ProgressTimer ignored = stats.timeWrite()) {
            boolean writePropertyEqualsSeedProperty = Objects.equals(seedProperty, writeProperty);
            WeightMapping seedProperties = graph.nodeProperties(LabelPropagation.SEED_TYPE);
            boolean hasSeedProperties = seedProperties != null && !(seedProperties instanceof NullWeightMap);

            PropertyTranslator<HugeLongArray> translator = HugeLongArray.Translator.INSTANCE;
            if (writePropertyEqualsSeedProperty && hasSeedProperties) {
                translator = new PropertyTranslator.OfLongIfChanged<>(seedProperties, HugeLongArray::get);
            }
            Exporter.of(dbAPI, graph)
                    .withLog(log)
                    .parallel(Pools.DEFAULT, concurrency, TerminationFlag.wrap(transaction))
                    .build()
                    .write(
                            writeProperty,
                            labels,
                            translator
                    );
        }
    }

    public static class ProcedureSetup {
        final Graph graph;
        final AllocationTracker tracker;
        final ProcedureConfiguration procedureConfig;
        final LabelPropagationStats.Builder statsBuilder;

        ProcedureSetup(
                final Graph graph,
                final AllocationTracker tracker,
                final ProcedureConfiguration procedureConfig,
                final LabelPropagationStats.Builder statsBuilder) {
            this.graph = graph;
            this.tracker = tracker;
            this.procedureConfig = procedureConfig;
            this.statsBuilder = statsBuilder;
        }
    }
}
