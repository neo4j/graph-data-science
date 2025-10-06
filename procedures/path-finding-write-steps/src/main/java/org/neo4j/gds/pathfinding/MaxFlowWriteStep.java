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
package org.neo4j.gds.pathfinding;

import org.apache.commons.lang3.LongRange;
import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.values.storable.Values;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class MaxFlowWriteStep implements WriteStep<FlowResult, RelationshipsWritten> {
    private final WriteRelationshipService writeRelationshipService;
    private final String writeRelationshipType;
    private final String writeProperty;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final JobId jobId;

    public MaxFlowWriteStep(
        WriteRelationshipService writeRelationshipService,
        String writeRelationshipType,
        String writeProperty,
        Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        JobId jobId
    ) {
        this.writeRelationshipService = writeRelationshipService;
        this.writeRelationshipType = writeRelationshipType;
        this.writeProperty = writeProperty;
        this.resultStoreResolver = resultStoreResolver;
        this.jobId = jobId;
    }


    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        FlowResult result,
        JobId jobId
    ) {
        try (
            var relationshipStream = LongRange.of(0, result.flow().size()-1).toLongStream().mapToObj(relIdx -> {
                var rel = result.flow().get(relIdx);
                var relationshipValue = new org.neo4j.values.storable.DoubleValue[]{Values.doubleValue(rel.flow())};
                return new ExportedRelationship(rel.sourceId(), rel.targetId(), relationshipValue);
            })
        ) {

            // the final result is the side effect of writing to the database, plus this metadata
            return writeRelationshipService.writeFromRelationshipStream(
                writeRelationshipType,
                List.of(writeProperty),
                List.of(ValueType.DOUBLE),
                relationshipStream,
                graph,
                "Write maximum flow",
                resultStoreResolver.apply(resultStore),
                this.jobId
            );
        }
    }
}
