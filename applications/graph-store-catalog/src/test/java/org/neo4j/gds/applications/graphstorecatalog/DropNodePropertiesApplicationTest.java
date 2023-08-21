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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DropNodePropertiesApplicationTest {
    /**
     * Yep this is convoluted, but bear with: it is still better than starting a Neo4j to demonstrate it!
     * Matryoshka dolls much?
     * Well yeah, so an exercise for the reader: how might we change the design to make this testing easier?
     */
    @Test
    void shouldLogDeprecationWarnings() {
        var service = new DropNodePropertiesApplication(null) {
            @Override
            long computeWithErrorHandling(
                GraphStore graphStore,
                ProgressTracker progressTracker,
                List<String> nodeProperties
            ) {
                return 42; // short circuit the call stack
            }
        };

        var progressTrackerFactory = mock(ProgressTrackerFactory.class);
        var progressTracker = mock(ProgressTracker.class);
        when(progressTrackerFactory.create(any(Task.class))).thenReturn(progressTracker);
        var result = service.computeWithProgressTracking(
            null,
            Optional.of("deprecated!"),
            progressTrackerFactory, List.of("a", "b", "c")
        );

        assertEquals(42, result);
        verify(progressTracker).logWarning("deprecated!");
    }
}
