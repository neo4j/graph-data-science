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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.MemoryGuard;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.memory.tracking.MemoryGuardException;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * This interface is hell to try and stub with Mockito
 */
class MemoryGuardStub implements MemoryGuard {
    private final MemoryGuardException memoryGuardException;
    private final AtomicBoolean wasCalled;

    MemoryGuardStub(MemoryGuardException memoryGuardException, AtomicBoolean wasCalled) {
        this.memoryGuardException = memoryGuardException;
        this.wasCalled = wasCalled;
    }

    @Override
    public void assertAlgorithmCanRun(
        Graph graph,
        GraphStore graphStore,
        Collection<RelationshipType> relationshipTypesFilter,
        Concurrency concurrency,
        Supplier<MemoryEstimation> estimationFactory,
        Label label,
        DimensionTransformer dimensionTransformer,
        String username,
        JobId jobId,
        boolean bypassMemoryEstimation
    ) throws MemoryGuardException {
        if (memoryGuardException != null) throw memoryGuardException;

        wasCalled.set(true);
    }
}
