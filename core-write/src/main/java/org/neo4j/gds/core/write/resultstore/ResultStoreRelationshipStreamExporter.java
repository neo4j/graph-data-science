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
package org.neo4j.gds.core.write.resultstore;

import org.neo4j.gds.api.ExportedRelationship;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.ResultStoreEntry;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.write.RelationshipStreamExporter;

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public class ResultStoreRelationshipStreamExporter implements RelationshipStreamExporter {

    private final JobId jobId;
    private final ResultStore resultStore;
    private final Stream<ExportedRelationship> relationshipStream;
    private final LongUnaryOperator toOriginalId;

    ResultStoreRelationshipStreamExporter(
        JobId jobId,
        ResultStore resultStore,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    ) {
        this.jobId = jobId;
        this.resultStore = resultStore;
        this.relationshipStream = relationshipStream;
        this.toOriginalId = toOriginalId;
    }

    @Override
    public long write(String relationshipType, List<String> propertyKeys, List<ValueType> propertyTypes) {
        resultStore.addRelationshipStream(relationshipType, propertyKeys, propertyTypes, relationshipStream, toOriginalId);
        resultStore.add(jobId, new ResultStoreEntry.RelationshipStream(relationshipType, propertyKeys, propertyTypes, relationshipStream, toOriginalId));
        // TODO: return the number of relationships written
        return 0;
    }
}
