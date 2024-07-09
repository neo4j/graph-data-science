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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.write.RelationshipPropertiesExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.values.storable.Values;

import java.util.List;

public class WriteRelationshipPropertiesApplication {
    private final Log log;

    public WriteRelationshipPropertiesApplication(Log log) {
        this.log = log;
    }

    WriteRelationshipPropertiesResult compute(
        RelationshipPropertiesExporterBuilder relationshipPropertiesExporterBuilder,
        TerminationFlag terminationFlag,
        GraphStore graphStore,
        ResultStore resultStore,
        GraphName graphName,
        String relationshipType,
        List<String> relationshipProperties,
        WriteRelationshipPropertiesConfig configuration
    ) {
        var relationshipCount = graphStore.relationshipCount(RelationshipType.of(relationshipType));

        var relationshipPropertiesExporter = relationshipPropertiesExporterBuilder
            .withGraphStore(graphStore)
            .withRelationPropertyTranslator(Values::doubleValue)
            .withTerminationFlag(terminationFlag)
            .withProgressTracker(ProgressTracker.NULL_TRACKER)
            .withArrowConnectionInfo(
                configuration.arrowConnectionInfo(),
                graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
            )
            .withResultStore(configuration.resolveResultStore(resultStore))
            .withRelationshipCount(relationshipCount)
            .withJobId(configuration.jobId())
            .build();

        var resultBuilder = new WriteRelationshipPropertiesResult.Builder(
            graphName.getValue(),
            relationshipType,
            relationshipProperties
        );

        try (var ignored = ProgressTimer.start(resultBuilder::withWriteMillis)) {
            try {
                relationshipPropertiesExporter.write(relationshipType, relationshipProperties);

                resultBuilder
                    .withRelationshipsWritten(relationshipCount)
                    .withConfiguration(configuration.toMap());
            } catch (RuntimeException e) {
                log.warn("Writing relationships failed", e);
                throw e;
            }
        }

        return resultBuilder.build();
    }
}
