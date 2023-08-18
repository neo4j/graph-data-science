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

import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class StreamRelationshipPropertiesApplicationTest {
    @Test
    void shouldTrackProgress() {
        var application = new StreamRelationshipPropertiesApplication(null) {
            @Override
            <T> Stream<T> computeRelationshipPropertyStream(
                GraphStore graphStore,
                boolean usesPropertyNameColumn,
                GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller,
                List<Triple<RelationshipType, String, Graph>> relationshipPropertyKeysAndValues
            ) {
                return Stream.empty();
            }
        };

        var progressTracker = mock(ProgressTracker.class);
        application.computeWithProgressTracking(
            null,
            false,
            Optional.empty(),
            null,
            progressTracker,
            null
        ).close();

        verify(progressTracker).beginSubTask();
        verify(progressTracker).endSubTask();
    }

    @Test
    void shouldIssueDeprecationWarning() {
        var application = new StreamRelationshipPropertiesApplication(null) {
            @Override
            <T> Stream<T> computeRelationshipPropertyStream(
                GraphStore graphStore,
                boolean usesPropertyNameColumn,
                GraphStreamRelationshipPropertyOrPropertiesResultProducer<T> outputMarshaller,
                List<Triple<RelationshipType, String, Graph>> relationshipPropertyKeysAndValues
            ) {
                return Stream.empty();
            }
        };

        var progressTracker = mock(ProgressTracker.class);
        application.computeWithProgressTracking(
            null,
            false,
            Optional.of("willy nilly"),
            null,
            progressTracker,
            null
        ).close();

        verify(progressTracker).beginSubTask();
        verify(progressTracker).logWarning("willy nilly");
        verify(progressTracker).endSubTask();
    }
}
