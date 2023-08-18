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

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class StreamNodePropertiesApplicationTest {
    @Test
    void shouldTrackProgress() {
        var application = new StreamNodePropertiesApplication(null) {
            @Override
            <T> Stream<T> computeNodePropertyStream(
                GraphExportNodePropertiesConfig configuration,
                IdMap idMap,
                Collection<Pair<String, NodePropertyValues>> nodePropertyKeysAndValues,
                boolean usesPropertyNameColumn,
                ProgressTracker progressTracker,
                GraphStreamNodePropertyOrPropertiesResultProducer<T> producer
            ) {
                return Stream.empty();
            }
        };

        var progressTracker = mock(ProgressTracker.class);
        application.computeWithProgressTracking(
            null,
            null,
            null,
            false,
            Optional.empty(),
            progressTracker,
            null
        ).close();

        verify(progressTracker).beginSubTask();
        verify(progressTracker).endSubTask();
    }

    @Test
    void shouldIssueDeprecationWarning() {
        var application = new StreamNodePropertiesApplication(null) {
            @Override
            <T> Stream<T> computeNodePropertyStream(
                GraphExportNodePropertiesConfig configuration,
                IdMap idMap,
                Collection<Pair<String, NodePropertyValues>> nodePropertyKeysAndValues,
                boolean usesPropertyNameColumn,
                ProgressTracker progressTracker,
                GraphStreamNodePropertyOrPropertiesResultProducer<T> producer
            ) {
                return Stream.empty();
            }
        };

        var progressTracker = mock(ProgressTracker.class);
        application.computeWithProgressTracking(
            null,
            null,
            null,
            false,
            Optional.of("willy nilly"),
            progressTracker,
            null
        ).close();

        verify(progressTracker).beginSubTask();
        verify(progressTracker).logWarning("willy nilly");
        verify(progressTracker).endSubTask();
    }
}
