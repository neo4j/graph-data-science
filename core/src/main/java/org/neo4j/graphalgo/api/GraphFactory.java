/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.core.EmptyGraph;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.NodeImporter;
import org.neo4j.graphalgo.core.utils.ApproximatedImportProgress;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.neo4j.internal.kernel.api.Read.ANY_LABEL;

/**
 * The Abstract Factory defines the construction of the graph
 *
 * @author mknblch
 */
public abstract class GraphFactory {

    public static final String TASK_LOADING = "LOADING";

    protected final ExecutorService threadPool;
    protected final GraphDatabaseAPI api;
    protected final GraphSetup setup;
    protected final GraphDimensions dimensions;
    protected final ImportProgress progress;
    protected final Log log;
    protected final ProgressLogger progressLogger;

    public GraphFactory(GraphDatabaseAPI api, GraphSetup setup) {
        this.threadPool = setup.executor;
        this.api = api;
        this.setup = setup;
        this.log = setup.log;
        this.progressLogger = progressLogger(log, setup.logMillis);
        dimensions = new GraphDimensions(api, setup).call();
        progress = importProgress(progressLogger, dimensions, setup);
    }

    public Graph build() {
        if (!isValidPredicate()) {
            return new EmptyGraph();
        } else {
            return importGraph();
        }
    }

    protected abstract Graph importGraph();

    protected boolean isValidPredicate() {
        return isValidNodePredicate() && isValidRelationshipTypePredicate();
    }

    private boolean isValidNodePredicate() {
        return (setup.startLabel == null || setup.startLabel.isEmpty()) || dimensions.labelId() != ANY_LABEL;
    }

    private boolean isValidRelationshipTypePredicate() {
        return (setup.relationshipType == null || setup.relationshipType.isEmpty()) || dimensions.singleRelationshipTypeId() != ANY_LABEL;
    }

    protected ImportProgress importProgress(
            ProgressLogger progressLogger,
            GraphDimensions dimensions,
            GraphSetup setup) {
        long relOperations = 0L;
        if (setup.loadIncoming || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }
        if (setup.loadOutgoing || setup.loadAsUndirected) {
            relOperations += dimensions.maxRelCount();
        }
        return new ApproximatedImportProgress(
                progressLogger,
                setup.tracker,
                dimensions.hugeNodeCount(),
                relOperations
        );
    }

    protected IntIdMap loadIdMap() {
        final NodeImporter nodeImporter = new NodeImporter(
                api,
                setup.tracker,
                progress,
                dimensions.nodeCount(),
                dimensions.labelId());
        return nodeImporter.call();
    }

    private static ProgressLogger progressLogger(Log log, long time) {
        return ProgressLogger.wrap(log, TASK_LOADING, time, TimeUnit.MILLISECONDS);
    }
}
