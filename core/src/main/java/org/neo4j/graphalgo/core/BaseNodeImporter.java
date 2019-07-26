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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.StatementFunction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public abstract class BaseNodeImporter<T> extends StatementFunction<T> {

    private final ImportProgress progress;
    private final long nodeCount;
    private final int labelId;

    public BaseNodeImporter(GraphDatabaseAPI api, ImportProgress progress, long nodeCount, int labelId) {
        super(api);
        this.progress = progress;
        this.nodeCount = nodeCount;
        this.labelId = labelId;
    }

    @Override
    public final T apply(final KernelTransaction transaction) {
        final T mapping = newNodeMap(nodeCount);
        ReadHelper.readNodes(transaction.cursors(), transaction.dataRead(), labelId, (nodeId) -> {
            addNodeId(mapping, nodeId);
            progress.singleNodeImported();
        });
        finish(mapping);
        return mapping;
    }

    @Override
    public String threadName() {
        return "node-importer";
    }

    protected abstract T newNodeMap(long nodeCount);

    protected abstract void addNodeId(T map, long nodeId);

    protected abstract void finish(T map);
}
