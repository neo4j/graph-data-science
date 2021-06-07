/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.estimation.cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.nodemodels.ImmutableNodeClassificationTrainConfig;
import org.neo4j.gds.ml.nodemodels.logisticregression.NodeLogisticRegressionData;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.graphalgo.ElementProjection;
import org.neo4j.graphalgo.annotation.SuppressForbidden;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.config.WriteRelationshipConfig;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.core.model.Model;
import org.neo4j.graphalgo.core.model.ModelCatalog;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import picocli.CommandLine;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;
import static org.neo4j.gds.ml.nodemodels.NodeClassificationTrain.MODEL_TYPE;
import static org.neo4j.graphalgo.config.GraphCreateConfig.NODE_COUNT_KEY;
import static org.neo4j.graphalgo.config.GraphCreateConfig.RELATIONSHIP_COUNT_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROPERTIES_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.graphalgo.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.graphalgo.config.WritePropertyConfig.WRITE_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.humanReadable;
import static org.neo4j.graphalgo.utils.CheckedFunction.function;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal", "DefaultAnnotationParam"})
@SuppressForbidden(reason = "supposed to print")
@CommandLine.Command(
    description = "Estimates the memory consumption of a GDS procedure.",
    name = "estimation-cli",
    mixinStandardHelpOptions = true,
    version = "estimation-cli 0.4.2"
)
public class EstimationCli implements Runnable {

    private static final double GRAPH_CREATE_PEAK_MEMORY_FACTOR = 1.5;
    private static final double DEFAULT_PEAK_MEMORY_FACTOR = 1.0;

    private static final List<String> EXCLUDED_PROCEDURE_PREFIXES = List.of(
        "gds.testProc.test.estimate",
        "gds.beta.graphSage",
        "gds.alpha.ml.linkPrediction",
        "gds.beta.graph.export.csv"
    );

    private static final List<String> COMMUNITY_DETECTION_PREFIXES = List.of(
        "gds.beta.k1coloring",
        "gds.beta.modularityOptimization",
        "gds.labelPropagation",
        "gds.localClusteringCoefficient",
        "gds.louvain",
        "gds.triangleCount",
        "gds.wcc"
    );

    private static final List<String> CENTRALITY_PREFIXES = List.of(
        "gds.articleRank",
        "gds.betweenness",
        "gds.degree",
        "gds.eigenvector",
        "gds.pageRank"
    );

    private static final List<String> SIMILARITY_PREFIXES = List.of(
        "gds.beta.knn",
        "gds.nodeSimilarity"
    );

    private static final List<String> PATH_FINDING_PREFIXES = List.of(
        "gds.allShortestPaths",
        "gds.shortestPath"
    );

    private static final List<String> NODE_EMBEDDING_PREFIXES = List.of(
        "gds.beta.fastRPExtended",
        "gds.beta.node2vec",
        "gds.fastRP"
    );

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Command(name = "list-available")
    void listAvailable() {
        var availableProcedures = findAvailableMethods()
            .map(ProcedureMethod::name)
            .collect(joining(System.lineSeparator()));

        System.out.println(availableProcedures);
    }

    @CommandLine.Command(name = "estimate")
    void estimateOne(
        @CommandLine.Parameters(
            paramLabel = "procedure",
            description = "The procedure to estimate, e.g. gds.pagerank.stream.",
            converter = ProcedureNameNormalizer.class,
            defaultValue = ""
        )
            String procedureName,

        @CommandLine.Mixin
            CountOptions counts,

        @CommandLine.ArgGroup(exclusive = true)
            BlockSizeOptions blockSizeOptions,

        @CommandLine.ArgGroup(exclusive = true)
            PrintOptions printOptions,

        @CommandLine.Option(
            names = {"--category"},
            description = "Filter algorithms to estimate based on the category",
            arity = "*"
        ) List<String> categories
    ) throws Exception {
        GdsEdition.instance().setToEnterpriseEdition();
        var printOpts = printOptions == null ? new PrintOptions() : printOptions;

        Stream<ProcedureMethod> procedureMethods;
        if (categories == null || categories.isEmpty()) {
            procedureMethods = procedureName.isBlank()
              ? findAvailableMethods()
              : Stream.of(ImmutableProcedureMethod.of(procedureName, findProcedure(procedureName)));
        } else {
            if (!procedureName.isEmpty()) {
                throw new IllegalArgumentException("--category and explicit algo is not allowed");
            }

            if (categories.contains("machine-learning")) {
                procedureMethods = findAvailableMethods();
            } else {
                var includePrefixes = categories.stream()
                .flatMap(category -> {
                    switch (category.toLowerCase(Locale.ENGLISH)) {
                        case "community-detection":
                            return COMMUNITY_DETECTION_PREFIXES.stream();
                        case "centrality":
                            return CENTRALITY_PREFIXES.stream();
                        case "similarity":
                            return SIMILARITY_PREFIXES.stream();
                        case "path-finding":
                            return PATH_FINDING_PREFIXES.stream();
                        case "node-embedding":
                            return NODE_EMBEDDING_PREFIXES.stream();
                        case "machine-learning":
                            return Stream.empty();
                        default:
                            throw new IllegalArgumentException("Unknown category: " + category);
                    }
                })
                .collect(Collectors.toList());
                procedureMethods = findAvailableMethods(includePrefixes);
            }
        }

        var estimations = procedureMethods
            .map(function(proc -> estimateProcedure(proc.name(), proc.method(), counts)))
            .collect(Collectors.toList());

        var blockSizeOpts = Optional.ofNullable(blockSizeOptions).map(BlockSizeOptions::get);
        renderResults(counts, printOpts, blockSizeOpts, estimations);
    }

    static final class BlockSizeOptions {
        @CommandLine.Option(
            names = {"--block-size"},
            description = "Scale sizes by SIZE before printing them; e.g., '--block-size M' prints sizes in units of 1,048,576 bytes. Valid values are: ${COMPLETION-CANDIDATES}"
        )
        BlockSize blockSize;

        @CommandLine.Option(
            names = "-K",
            description = "Scale sizes by 1024"
        )
        private boolean K;

        @CommandLine.Option(
            names = "-M",
            description = "Scale sizes by 1024*1024"
        )
        private boolean M;

        @CommandLine.Option(
            names = "-G",
            description = "Scale sizes by 1024*1024*1024"
        )
        private boolean G;

        @CommandLine.Option(
            names = "-KB",
            description = "Scale sizes by 1000"
        )
        private boolean KB;

        @CommandLine.Option(
            names = "-MB",
            description = "Scale sizes by 1000*1000"
        )
        private boolean MB;

        @CommandLine.Option(
            names = "-GB",
            description = "Scale sizes by 1000*1000*1000"
        )
        private boolean GB;

        BlockSize get() {
            if (K) {
                return BlockSize.K;
            } else if (M) {
                return BlockSize.M;
            } else if (G) {
                return BlockSize.G;
            } else if (KB) {
                return BlockSize.KB;
            } else if (MB) {
                return BlockSize.MB;
            } else if (GB) {
                return BlockSize.GB;
            } else {
                return Objects.requireNonNull(blockSize);
            }
        }
    }

    static final class CountOptions {
        @CommandLine.Option(
            names = {"-n", "--nodes"},
            description = "Number of nodes in the fictitious graph.",
            required = true,
            converter = LongParser.class
        )
        private long nodeCount;

        @CommandLine.Option(
            names = {"-r", "--relationships"},
            description = "Number of relationships in the fictitious graph.",
            required = true,
            converter = LongParser.class
        )
        private long relationshipCount;

        @CommandLine.Option(
            names = {"-l", "--labels"},
            description = "Number of node labels in the fictitious graph.",
            converter = IntParser.class
        )
        private int labelCount = 0;

        // We don't make use of this because the number of types does not influence the estimation.
        // We specify it here so that the options look symmetric, the result just doesn't change.
        @CommandLine.Option(
            names = {"-t", "--types"},
            description = "Number of relationship types in the fictitious graph.",
            converter = IntParser.class
        )
        private int relationshipTypes = 0;

        @CommandLine.Option(
            names = {"-np", "--node-properties"},
            description = "Number of node properties in the fictitious graph.",
            converter = IntParser.class
        )
        private int nodePropertyCount = 0;

        @CommandLine.Option(
            names = {"-rp", "--relationship-properties"},
            description = "Number of relationship properties in the fictitious graph.",
            converter = IntParser.class
        )
        private int relationshipPropertyCount = 0;

        @CommandLine.Option(
            names = {"-c", "--config"},
            description = "Numeric configuration options of the given procedure.",
            split = ","
        )
        private Map<String, Number> config;

        @NotNull
        private Map<String, Object> procedureConfig(String procedureName) {
            HashMap<String, Object> actualConfig;
            if (config != null) {
                actualConfig = new HashMap<>(config);
            } else {
                actualConfig = new HashMap<>();
            }

            actualConfig.put(NODE_COUNT_KEY, nodeCount);
            actualConfig.put(RELATIONSHIP_COUNT_KEY, relationshipCount);
            actualConfig.put(NODE_PROJECTION_KEY, labelCount > 0
                ? createEntries(labelCount, "Label")
                : ElementProjection.PROJECT_ALL
            );
            actualConfig.put(RELATIONSHIP_PROJECTION_KEY, ElementProjection.PROJECT_ALL);

            if (nodePropertyCount > 0) {
                actualConfig.put(NODE_PROPERTIES_KEY, createEntries(nodePropertyCount, "prop"));
            }
            if (relationshipPropertyCount > 0) {
                actualConfig.put(RELATIONSHIP_PROPERTIES_KEY, createEntries(relationshipPropertyCount, "prop"));
            }
            if (procedureName.endsWith(".write.estimate")) {
                actualConfig.put(WRITE_PROPERTY_KEY, "ESTIMATE_FAKE_WRITE_PROPERTY");
            }
            if (procedureName.endsWith(".mutate.estimate")) {
                actualConfig.put(MUTATE_PROPERTY_KEY, "ESTIMATE_FAKE_MUTATE_PROPERTY");
            }
            if (procedureName.startsWith("gds.fastRP")) {
                actualConfig.put("embeddingDimension", 128);
            }
            if (procedureName.startsWith("gds.beta.fastRPExtended")) {
                actualConfig.put("embeddingDimension", 128);
                actualConfig.put("propertyDimension", 64);
            }
            if (procedureName.startsWith("gds.beta.knn")) {
                actualConfig.put("nodeWeightProperty", "ESTIMATE_FAKE_NODE_WEIGHT_PROPERTY");
            }
            if (procedureName.equals("gds.nodeSimilarity.write.estimate") || procedureName.equals("gds.beta.knn.write.estimate")) {
                actualConfig.put("writeRelationshipType", "ESTIMATE_FAKE_WRITE_RELATIONSHIP_PROPERTY");
            }
            if (procedureName.equals("gds.nodeSimilarity.mutate.estimate") || procedureName.equals("gds.beta.knn.mutate.estimate")) {
                actualConfig.put("mutateRelationshipType", "ESTIMATE_FAKE_MUTATE_RELATIONSHIP_PROPERTY");
            }
            if (procedureName.startsWith("gds.shortestPath.")) {
                actualConfig.put("sourceNode", 0L);
                actualConfig.put("targetNode", 1L);
            }
            if (procedureName.startsWith("gds.shortestPath.yens.")) {
                actualConfig.put("k", 3);
            }
            if (procedureName.startsWith("gds.shortestPath.astar.")) {
                actualConfig.put(ShortestPathAStarBaseConfig.LATITUDE_PROPERTY_KEY, "LAT");
                actualConfig.put(ShortestPathAStarBaseConfig.LONGITUDE_PROPERTY_KEY, "LON");
            }
            if (procedureName.startsWith("gds.shortestPath.") && procedureName.endsWith("write.estimate")) {
                actualConfig.put(WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "ESTIMATE_FAKE_WRITE_RELATIONSHIP_PROPERTY");
                actualConfig.remove(WRITE_PROPERTY_KEY);
            }
            if (procedureName.startsWith("gds.shortestPath.") && procedureName.endsWith("mutate.estimate")) {
                actualConfig.put(MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "ESTIMATE_FAKE_WRITE_RELATIONSHIP_PROPERTY");
                actualConfig.remove(MUTATE_PROPERTY_KEY);
            }
            if (procedureName.startsWith("gds.allShortestPaths.")) {
                actualConfig.put("sourceNode", 0L);
            }
            if (procedureName.startsWith("gds.allShortestPaths.") && procedureName.endsWith("write.estimate")) {
                actualConfig.put(WriteRelationshipConfig.WRITE_RELATIONSHIP_TYPE_KEY, "ESTIMATE_FAKE_WRITE_RELATIONSHIP_PROPERTY");
                actualConfig.remove(WRITE_PROPERTY_KEY);
            }
            if (procedureName.startsWith("gds.allShortestPaths.") && procedureName.endsWith("mutate.estimate")) {
                actualConfig.put(MutateRelationshipConfig.MUTATE_RELATIONSHIP_TYPE_KEY, "ESTIMATE_FAKE_WRITE_RELATIONSHIP_PROPERTY");
                actualConfig.remove(MUTATE_PROPERTY_KEY);
            }
            if (
                procedureName.matches("^.+\\.predict\\.(mutate|stream|write)\\.estimate$") ||
                procedureName.endsWith(".train.estimate")
            ) {
                actualConfig.put("modelName", "model");
            }
            if (procedureName.startsWith("gds.alpha.ml.nodeClassification.predict")) {
                addModelWithFeatures("", "model", List.of("a", "b"));
            }
            if (procedureName.startsWith("gds.alpha.ml.nodeClassification.train")) {
                actualConfig.put("holdoutFraction", 0.2);
                actualConfig.put("validationFolds", 5);
                actualConfig.put("params", List.of(
                    Map.of("penalty", 0.065),
                    Map.of("penalty", 0.125)
                ));
                actualConfig.put("metrics", List.of("F1_MACRO"));
                actualConfig.put("targetProperty", "target");
            }
            return actualConfig;
        }

        private List<String> createEntries(int count, String prefix) {
            return IntStream.range(0, count)
                .mapToObj(i -> formatWithLocale("%s%d", prefix, i))
                .collect(Collectors.toList());
        }
    }

    enum BlockSize {
        K(1024, 1),
        M(1024, 2),
        G(1024, 3),
        T(1024, 4),
        P(1024, 5),
        E(1024, 6),
        Z(1024, 7),
        Y(1024, 8),

        KB(1000, 1),
        MB(1000, 2),
        GB(1000, 3),
        TB(1000, 4),
        PB(1000, 5),
        EB(1000, 6),
        ZB(1000, 7),
        YB(1000, 8);

        int base;
        int factor;

        BlockSize(int base, int factor) {
            this.base = base;
            this.factor = factor;
        }
    }

    static final class PrintOptions {
        @CommandLine.Option(
            names = {"--tree"},
            description = "Print estimated memory as human readable tree view.",
            required = true
        )
        boolean printTree;

        @CommandLine.Option(
            names = {"--json"},
            description = "Print estimated memory in json format.",
            required = true
        )
        boolean printJson;
    }

    public static void main(String... args) {
        int exitCode = runWithArgs(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Specify a subcommand");
    }

    static int runWithArgs(String... args) {
        var commandLine = new CommandLine(new EstimationCli())
            .registerConverter(Number.class, new NumberParser());

        boolean parsed;
        try {
            parsed = commandLine.parseArgs(args).hasSubcommand();
        } catch (RuntimeException e) {
            parsed = false;
        }

        if (!parsed) {
            var newArgs = new String[args.length + 1];
            System.arraycopy(args, 0, newArgs, 1, args.length);
            newArgs[0] = "estimate";
            args = newArgs;
        }

        return commandLine.execute(args);
    }

    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.graphalgo",
        "org.neo4j.gds.paths",
        "org.neo4j.gds.embeddings",
        "org.neo4j.gds.ml"
    );

    private Method findProcedure(String procedure) {
        return PACKAGES_TO_SCAN.stream()
            .map(pkg -> new Reflections(pkg, new MethodAnnotationsScanner()))
            .map(reflections -> findProcedure(reflections, procedure))
            .flatMap(Optional::stream)
            .findFirst()
            .orElseThrow(() -> new CommandLine.ParameterException(
                spec.commandLine(),
                formatWithLocale("Procedure not found: %s", procedure),
                spec.findOption("procedure"),
                procedure
            ));
    }

    private Optional<Method> findProcedure(Reflections reflections, String procedureName) {
        return reflections
            .getMethodsAnnotatedWith(Procedure.class)
            .stream()
            .filter(method -> {
                var annotation = method.getDeclaredAnnotation(Procedure.class);
                return annotation.value().equalsIgnoreCase(procedureName) ||
                       annotation.name().equalsIgnoreCase(procedureName);
            })
            .findFirst();
    }

    private Stream<ProcedureMethod> findAvailableMethods() {
        return findAvailableMethods(List.of());
    }

    private Stream<ProcedureMethod> findAvailableMethods(List<String> inclusionFilter) {
        return PACKAGES_TO_SCAN.stream()
            .map(pkg -> new Reflections(pkg, new MethodAnnotationsScanner()))
            .flatMap(reflections -> reflections
                .getMethodsAnnotatedWith(Procedure.class)
                .stream())
            .flatMap(method -> {
                var annotation = method.getAnnotation(Procedure.class);
                var valueName = annotation.value();
                var definedName = annotation.name();
                var procName = definedName.trim().isEmpty() ? valueName : definedName;
                return Stream.of(procName)
                    .filter(name -> name.endsWith(".estimate"))
                    .filter(name -> inclusionFilter.isEmpty() || inclusionFilter.stream().anyMatch(name::startsWith))
                    .filter(not(name -> EXCLUDED_PROCEDURE_PREFIXES.stream().anyMatch(name::startsWith)))
                    .map(name -> ImmutableProcedureMethod.of(name, method));
            })
            .sorted(Comparator.comparing(ProcedureMethod::name, String.CASE_INSENSITIVE_ORDER));
    }

    private EstimatedProcedure estimateProcedure(
        String procedureName,
        Method procedure,
        CountOptions counts
    ) throws Exception {
        var config = counts.procedureConfig(procedureName);
        var estimateResult = runProcedure(procedure, config);
        ModelCatalog.removeAllLoadedModels();
        return ImmutableEstimatedProcedure.of(procedureName, estimateResult);
    }

    private static MemoryEstimateResult runProcedure(Method procedure, Map<String, Object> config) throws Exception {
        var parameters = procedure.getParameters();
        var args = new Object[parameters.length];
        var foundConfigParam = false;
        for (int i = 0; i < parameters.length; i++) {
            Parameter parameter = parameters[i];
            var name = parameter.getAnnotation(Name.class);
            if (name != null) {
                switch (name.value()) {
                    case NODE_QUERY_KEY:
                        args[i] = GraphCreateFromCypherConfig.ALL_NODES_QUERY;
                        config.remove(NODE_PROJECTION_KEY);
                        break;
                    case RELATIONSHIP_QUERY_KEY:
                        args[i] = GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
                        config.remove(RELATIONSHIP_PROJECTION_KEY);
                        break;
                    case NODE_PROJECTION_KEY: // explicit fall-through
                    case RELATIONSHIP_PROJECTION_KEY:
                        args[i] = ElementProjection.PROJECT_ALL;
                        break;
                    case "graphName":
                        if (!foundConfigParam) {
                            foundConfigParam = true;
                            args[i] = config;
                        } else {
                            throw new RuntimeException(
                                "found parameter annotated with `graphName`, expected to accept a config object, but there was already another parameter that accepted the config."
                            );
                        }
                        break;
                    case "configuration":
                        if (!foundConfigParam) {
                            foundConfigParam = true;
                            args[i] = config;
                        } else {
                            args[i] = Map.of();
                        }
                        break;
                    default:
                        throw new RuntimeException(
                            "Unexpected parameter name: `" + name.value() + "`. This is probably a bug in GDS."
                        );
                }
            }
        }

        var procInstance = procedure.getDeclaringClass().getConstructor().newInstance();
        var procResultStream = (Stream<?>) procedure.invoke(procInstance, args);
        return (MemoryEstimateResult) procResultStream.findFirst().orElseThrow();
    }

    private static void renderResults(
        CountOptions countOptions,
        PrintOptions printOptions,
        Optional<BlockSize> blockSize,
        Collection<EstimatedProcedure> estimatedProcedures
    ) throws IOException {
        if (printOptions.printTree) {
            for (EstimatedProcedure estimatedProcedure : estimatedProcedures) {
                System.out.printf(
                    Locale.ENGLISH,
                    "%s,%s%n",
                    estimatedProcedure.name(),
                    estimatedProcedure.estimation().treeView
                );
            }
        } else if (!printOptions.printJson) {
            String unit;
            double scale;
            if (blockSize.isPresent()) {
                var bs = blockSize.get();
                unit = bs.name();
                scale = Math.pow(bs.base, bs.factor);
            } else {
                unit = ""; // no unit specified
                scale = 1; // no scaling
            }
            for (EstimatedProcedure estimatedProcedure : estimatedProcedures) {
                var estimation = estimatedProcedure.estimation();
                System.out.printf(
                    Locale.ENGLISH,
                    "%s,%.0f%s,%.0f%s%n",
                    estimatedProcedure.name(),
                    estimation.bytesMin / scale, unit,
                    estimation.bytesMax / scale, unit
                );
            }
        } else {
            var mapper = new ObjectMapper();
            // Primary target consumes this from Python, snake_case is more pythonic.
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
            // Pretty print output is nicer to read and any possible overhead from having to parse
            // some whitespace is not important to our use-case
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            mapper.enable(SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED);

            var jsons = estimatedProcedures
                .stream()
                .map(p -> toJson(p.name(), countOptions, p.estimation()))
                .collect(Collectors.toList());

            mapper.writeValue(System.out, jsons);

        }
    }

    private static JsonOutput toJson(
        String procedureName,
        CountOptions countOptions,
        MemoryEstimateResult memoryEstimation
    ) {
        return ImmutableJsonOutput.builder()
            .bytesMin(memoryEstimation.bytesMin)
            .minMemory(humanReadable(memoryEstimation.bytesMin))
            .bytesMax(memoryEstimation.bytesMax)
            .maxMemory(humanReadable(memoryEstimation.bytesMax))
            .procedure(procedureName)
            .nodeCount(countOptions.nodeCount)
            .relationshipCount(countOptions.relationshipCount)
            .labelCount(countOptions.labelCount)
            .relationshipTypeCount(countOptions.relationshipTypes)
            .nodePropertyCount(countOptions.nodePropertyCount)
            .relationshipPropertyCount(countOptions.relationshipPropertyCount)
            .build();
    }

    static void addModelWithFeatures(String username, String modelName, List<String> properties) {
        var classIdMap = new LocalIdMap();
        classIdMap.toMapped(0);
        classIdMap.toMapped(1);
        var model = Model.of(
            username,
            modelName,
            MODEL_TYPE,
            GraphSchema.empty(),
            NodeLogisticRegressionData.builder()
                .weights(new Weights<>(new Matrix(new double[]{
                    1.12730619, -0.84532386, 0.93216654,
                    -1.12730619, 0.84532386, 0.0
                }, 2, 3)))
                .classIdMap(classIdMap)
                .build(),
            ImmutableNodeClassificationTrainConfig
                .builder()
                .modelName(modelName)
                .targetProperty("foo")
                .holdoutFraction(0.25)
                .validationFolds(4)
                .featureProperties(properties)
                .addParam(Map.of("penalty", 1.0))
                .build()
        );
        ModelCatalog.set(model);
    }
    @ValueClass
    interface ProcedureMethod {
        String name();

        Method method();
    }

    @ValueClass
    interface EstimatedProcedure {
        String name();

        MemoryEstimateResult estimation();
    }

    @JsonSerialize
    @JsonPropertyOrder(value = {
        "bytesMin", "bytesMax", "minMemory", "maxMemory", "procedure", "nodeCount",
        "relationshipCount", "labelCount", "relationshipTypeCount",
        "nodePropertyCount", "relationshipPropertyCount", "peakMemoryFactor",
        "bytesMinPeak", "minMemoryPeak", "bytesMaxPeak", "maxMemoryPeak"
    })
    @ValueClass
    interface JsonOutput {
        @JsonProperty("bytes_min_resident")
        long bytesMin();

        @JsonProperty("bytes_max_resident")
        long bytesMax();

        @JsonProperty("min_memory_resident")
        String minMemory();

        @JsonProperty("max_memory_resident")
        String maxMemory();

        String procedure();

        long nodeCount();

        long relationshipCount();

        int labelCount();

        int relationshipTypeCount();

        int nodePropertyCount();

        int relationshipPropertyCount();

        @JsonProperty("peak_memory_factor")
        default double peakMemoryFactor() {
            return procedure().startsWith("gds.graph.create") ?
                GRAPH_CREATE_PEAK_MEMORY_FACTOR :
                DEFAULT_PEAK_MEMORY_FACTOR;
        }

        @JsonProperty("bytes_min_peak")
        default long bytesMinPeak() {
            return (long) (bytesMin() * peakMemoryFactor());
        }

        @JsonProperty("min_memory_peak")
        default String minMemoryPeak() {
            return humanReadable(bytesMinPeak());
        }

        @JsonProperty("bytes_max_peak")
        default long bytesMaxPeak() {
            return (long) (bytesMax() * peakMemoryFactor());
        }

        @JsonProperty("max_memory_peak")
        default String maxMemoryPeak() {
            return humanReadable(bytesMaxPeak());
        }
    }
}
