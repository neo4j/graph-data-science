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
package org.neo4j.gds.catalog;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.graphstorecatalog.DatabaseExportResult;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.io.file.csv.estimation.CsvExportEstimation;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.procedures.GraphDataScienceProcedures;
import org.neo4j.gds.applications.graphstorecatalog.FileExportResult;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphStoreExportProc extends BaseProc {
    private static final String DESCRIPTION = "Exports a named graph to CSV files.";
    private static final String DESCRIPTION_ESTIMATE = "Estimate the required disk space for exporting a named graph to CSV files.";

    @Context
    public GraphDataScienceProcedures facade;

    @Procedure(name = "gds.graph.export", mode = READ)
    @Description("Exports a named graph into a new offline Neo4j database.")
    public Stream<DatabaseExportResult> database(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.graphCatalog().exportToDatabase(graphName, configuration);
    }

    @Internal
    @Procedure(name = "gds.beta.graph.export.csv", mode = READ, deprecatedBy = "gds.graph.export.csv")
    @Description(DESCRIPTION)
    @Deprecated(forRemoval = true)
    public Stream<FileExportResult> csvDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.graph.export.csv");
        facade.log().warn("Procedure `gds.beta.graph.export.csv` has been deprecated, please use `gds.graph.export.csv`.");

        return csv(graphName, configuration);
    }

    @Procedure(name = "gds.graph.export.csv", mode = READ)
    @Description(DESCRIPTION)
    public Stream<FileExportResult> csv(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return facade.graphCatalog().exportToCsv(graphName, configuration);
    }

    @Internal
    @Procedure(name = "gds.beta.graph.export.csv.estimate", mode = READ, deprecatedBy = "gds.graph.export.csv.estimate")
    @Description(DESCRIPTION_ESTIMATE)
    @Deprecated(forRemoval = true)
    public Stream<MemoryEstimateResult> csvEstimateDeprecated(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        facade.deprecatedProcedures().called("gds.beta.graph.export.csv.estimate");
        facade.log().warn("Procedure `gds.beta.graph.export.csv.estimate` has been deprecated, please use `gds.graph.export.csv.estimate`.");

        return csvEstimate(graphName, configuration);
    }

    @Procedure(name = "gds.graph.export.csv.estimate", mode = READ)
    @Description(DESCRIPTION_ESTIMATE)
    public Stream<MemoryEstimateResult> csvEstimate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var cypherConfig = CypherMapWrapper.create(configuration);
        var exportConfig = GraphStoreToCsvEstimationConfig.of(username(), cypherConfig);
        validateConfig(cypherConfig, exportConfig);

        var estimate = runWithExceptionLogging(
            "CSV export estimation failed",
            () -> {
                var graphStore = graphStoreFromCatalog(graphName, exportConfig).graphStore();


                var dimensions = GraphDimensions.of(graphStore.nodeCount(), graphStore.relationshipCount());
                var memoryTree = CsvExportEstimation
                    .estimate(graphStore, exportConfig.samplingFactor())
                    .estimate(dimensions, new Concurrency(1));
                return new MemoryTreeWithDimensions(memoryTree, dimensions);
            }
        );

        return Stream.of(new MemoryEstimateResult(estimate));
    }
}
