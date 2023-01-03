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
package org.neo4j.gds.core.io.file;

import org.neo4j.gds.core.io.GraphStoreInput;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;

import java.io.Flushable;
import java.io.IOException;

public final class ElementImportRunner<T extends InputEntityVisitor.Adapter & Flushable> implements Runnable {
    private final T visitor;
    private final InputIterator inputIterator;
    private final ProgressTracker progressTracker;

    ElementImportRunner(
        T visitor,
        InputIterator inputIterator,
        ProgressTracker progressTracker
    ) {
        this.visitor = visitor;
        this.inputIterator = inputIterator;
        this.progressTracker = progressTracker;
    }

    @Override
    public void run() {
        try (var chunk = inputIterator.newChunk()) {
            assert chunk instanceof GraphStoreInput.LastProgress : chunk.getClass();

            while (inputIterator.next(chunk)) {
                while (chunk.next(visitor)) {
                    long progress = ((GraphStoreInput.LastProgress) chunk).lastProgress();
                    progressTracker.logProgress(progress);
                }
                visitor.flush();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
