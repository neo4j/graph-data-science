/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.ElementProjection;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import picocli.CommandLine;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
@CommandLine.Command(
    description = "Estimates the memory consumption of a GDS procedure.",
    name = "estimation-cli",
    mixinStandardHelpOptions = true,
    version = "estimation-cli 0.4.2"
)
public class EstimationCli implements Callable<Integer> {

    private static final List<String> PACKAGES_TO_SCAN = List.of(
        "org.neo4j.graphalgo",
        "org.neo4j.gds.embeddings"
    );

    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    private String procedureName;
    private Method procedure;

    @CommandLine.Parameters(
        index = "0",
        description = "The procedure to estimate, e.g. gds.pagerank.stream.",
        converter = ProcedureNameNormalizer.class
    )
    void setProcedure(String procedure) {
        this.procedureName = procedure;
        this.procedure = findProcedure(procedure);
    }

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
    @SuppressWarnings({"unused"})
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

    @CommandLine.Option(
        names = {"--tree"},
        description = "Print estimated memory as human readable tree view."
    )
    private boolean printTree = false;

    public static void main(String... args) {
        int exitCode = new CommandLine(new EstimationCli())
            .registerConverter(Number.class, new NumberParser())
            .execute(args);

        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        GdsEdition.instance().setToEnterpriseEdition();

        var actualConfig = updateConfig();
        var memoryEstimation = runProcedure(actualConfig);

        var output = printTree
            ? memoryEstimation.treeView
            : formatWithLocale("%d,%d", memoryEstimation.bytesMin, memoryEstimation.bytesMax);

        System.out.println(output);

        return 0;
    }

    @NotNull
    private Map<String, Object> updateConfig() {
        if (config == null) {
            config = new HashMap<>();
        }
        var actualConfig = new HashMap<String, Object>(config);
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
        return actualConfig;
    }

    private List<String> createEntries(int count, String prefix) {
        return IntStream.range(0, count)
            .mapToObj(i -> formatWithLocale("%s%d", prefix, i))
            .collect(Collectors.toList());
    }

    private MemoryEstimateResult runProcedure(Map<String, Object> config) throws Exception {
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
}
