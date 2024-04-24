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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.beta.filter.NodesFilter;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Map;

public class WriteNodeLabelApplication {
    private final Log log;

    public WriteNodeLabelApplication(Log log) {
        this.log = log;
    }

    WriteLabelResult compute(
        NodeLabelExporterBuilder nodeLabelExporterBuilder,
        TerminationFlag terminationFlag,
        GraphStore graphStore,
        ResultStore resultStore,
        GraphName graphName,
        String nodeLabel,
        WriteLabelConfig configuration,
        Expression nodeFilter
    ) {
        var resultBuilder = WriteLabelResult
            .builder(graphName.getValue(), nodeLabel)
            .withConfig(configuration.toMap());

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            var filteredNodes = NodesFilter.filterNodes(
                graphStore,
                nodeFilter,
                configuration.concurrency(),
                Map.of(),
                DefaultPool.INSTANCE,
                ProgressTracker.NULL_TRACKER
            );

            var nodeLabelExporter = nodeLabelExporterBuilder
                .withIdMap(filteredNodes.idMap())
                .withTerminationFlag(terminationFlag)
                .withArrowConnectionInfo(
                    configuration.arrowConnectionInfo(),
                    graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
                )
                .withResultStore(configuration.resolveResultStore(resultStore))
                .parallel(DefaultPool.INSTANCE, configuration.concurrency())
                .build();

            try {
                nodeLabelExporter.write(nodeLabel);

                resultBuilder
                    .withNodeLabelsWritten(nodeLabelExporter.nodeLabelsWritten())
                    .withNodeCount(graphStore.nodeCount());
            } catch (RuntimeException e) {
                log.warn("Node label writing failed", e);
                throw e;
            }
        }
        return resultBuilder.build();
    }
}
