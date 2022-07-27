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
package org.neo4j.gds.beta.walking;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * A path template is a list of relationship types.
 *
 * In this implementation, the list of relationship types is encoded as a list of graphs with each one having only a single relationship type.
 */
public class CollapsePath extends Algorithm<Relationships> {
    private final List<Graph[]> pathTemplates;
    private final long nodeCount;
    private final boolean allowSelfLoops;
    private final int concurrency;
    private final ExecutorService executorService;

    public CollapsePath(
        List<Graph[]> pathTemplates,
        boolean allowSelfLoops,
        int concurrency,
        ExecutorService executorService
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.pathTemplates = pathTemplates;
        this.nodeCount = pathTemplates.get(0)[0].nodeCount();
        this.allowSelfLoops = allowSelfLoops;
        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    @Override
    public Relationships compute() {
        RelationshipsBuilder relImporter = GraphFactory.initRelationshipsBuilder()
            .nodes(pathTemplates.get(0)[0]) // just need any arbitrary graph
            .orientation(Orientation.NATURAL)
            .aggregation(Aggregation.NONE)
            .concurrency(concurrency)
            .executorService(executorService)
            .build();

        Supplier<Runnable> collapsePathTaskSupplier = CollapsePathTaskSupplier.create(
            relImporter,
            allowSelfLoops,
            pathTemplates,
            nodeCount
        );

        var tasks = ParallelUtil.tasks(concurrency, collapsePathTaskSupplier);

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        return relImporter.build();
    }

    @Override
    public void release() {

    }
}
