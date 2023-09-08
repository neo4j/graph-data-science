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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.filter.NodesFilter;
import org.neo4j.gds.beta.filter.expression.Expression;
import org.neo4j.gds.config.WriteLabelConfig;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.NodeLabelExporterBuilder;
import org.neo4j.gds.logging.Log;

import java.util.Map;

public class WriteNodeLabelApplication {
    private final Log log;
    private final NodeLabelExporterBuilder nodeLabelExporterBuilder;

    public WriteNodeLabelApplication(Log log, NodeLabelExporterBuilder nodeLabelExporterBuilder) {
        this.log = log;
        this.nodeLabelExporterBuilder = nodeLabelExporterBuilder;
    }

    WriteLabelResult compute(
        TerminationFlag terminationFlag,
        GraphStore graphStore,
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
                Pools.DEFAULT,
                ProgressTracker.NULL_TRACKER
            );

            var nodeLabelExporter = nodeLabelExporterBuilder
                .withIdMap(filteredNodes.idMap())
                .withTerminationFlag(terminationFlag)
                .withArrowConnectionInfo(configuration.arrowConnectionInfo(), graphStore.databaseId().databaseName())
                .parallel(Pools.DEFAULT, configuration.concurrency())
                .build();

            try {
                nodeLabelExporter.write(nodeLabel);

                resultBuilder
                    .withNodeLabelsWritten(nodeLabelExporter.nodeLabelsWritten())
                    .withNodeCount(graphStore.nodeCount());

                return resultBuilder.build();
            } catch (RuntimeException e) {
                log.warn("Node label writing failed", e);
                throw e;
            }
        }
    }
}
