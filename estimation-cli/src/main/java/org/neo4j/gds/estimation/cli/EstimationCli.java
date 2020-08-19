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

import org.neo4j.graphalgo.core.GdsEdition;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Procedure;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import picocli.CommandLine;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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

    @CommandLine.Parameters(index = "0", description = "The procedure to estimate, e.g. gds.pagerank.stream.")
    private String procedure;

    @CommandLine.Option(
        names = {"-nc", "--node-count"},
        description = "Number of nodes in the fictitious graph.",
        required = true
    )
    private long nodeCount;

    @CommandLine.Option(
        names = {"-rc", "--relationship-count"},
        description = "Number of relationships in the fictitious graph.",
        required = true
    )
    private long relationshipCount;

    @CommandLine.Option(
        names = {"-c", "--config"},
        description = "Numeric configuration options of the given procedure.",
        split = ","
    )
    private Map<String, Number> config;

    @CommandLine.Option(
        names = {"-t", "--tree"},
        description = "Print estimated memory as human readable tree view."
    )
    private boolean printTree = false;

    public static void main(String... args) {
        CommandLine app = new CommandLine(new EstimationCli());
        app.registerConverter(Number.class, number -> {
            try {
                return Integer.parseInt(number);
            } catch (NumberFormatException ignored) {
                try {
                    return Long.parseLong(number);
                } catch (NumberFormatException alsoIgnored) {
                    return Double.parseDouble(number);
                }
            }
        });
        int exitCode = app.execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        GdsEdition.instance().setToEnterpriseEdition();

        if (!procedure.endsWith(".estimate")) {
            procedure += ".estimate";
        }

        if (config == null) {
            config = new HashMap<>();
        }
        var actualConfig = new HashMap<String, Object>(config);

        actualConfig.put("nodeCount", nodeCount);
        actualConfig.put("relationshipCount", relationshipCount);
        actualConfig.put("nodeProjection", "*");
        actualConfig.put("relationshipProjection", "*");

        var procedureMethod = PACKAGES_TO_SCAN.stream()
            .map(pkg -> new Reflections(pkg, new MethodAnnotationsScanner()))
            .map(reflections -> findProcedure(reflections, procedure))
            .flatMap(Optional::stream)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale("Procedure not found: %s", procedure)));

        var memoryEstimation = runProcedure(procedureMethod, actualConfig);

        var output = printTree
            ? memoryEstimation.treeView
            : formatWithLocale("%d,%d", memoryEstimation.bytesMin, memoryEstimation.bytesMax);

        System.out.println(output);

        return 0;
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

    private MemoryEstimateResult runProcedure(Method procedureMethod, Map<String, Object> config) throws Exception {
        var procInstance = procedureMethod.getDeclaringClass().getConstructor().newInstance();
        var procResultStream = (Stream<?>) procedureMethod.invoke(procInstance, config, Map.of());
        return (MemoryEstimateResult) procResultStream.findFirst().orElseThrow();
    }
}
