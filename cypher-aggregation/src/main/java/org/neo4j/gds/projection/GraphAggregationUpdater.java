/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software; you can redistribute it and/or modify
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
package org.neo4j.gds.projection;

import org.jetbrains.annotations.Nullable;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.UserAggregationUpdater;
import org.neo4j.values.AnyValue;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.neo4j.gds.projection.CypherAggregation.FUNCTION_NAME;

/**
 * Per-kernel-updater wrapper around the shared {@link CypherAggregationUpdater}.
 * <p>
 * The Neo4j kernel creates one updater per aggregation unit: one for the whole
 * query under the interpreted and pipelined-serial runtimes, and one per worker
 * under the parallel runtime. Each wrapper owns one {@link org.neo4j.gds.projection.GraphImporter.ThreadLocalBatches},
 * so a pooled relationships-builder slot is claimed once per morsel (lazily on
 * the first update after {@link #applyUpdates}) instead of once per row.
 */
public class GraphAggregationUpdater implements UserAggregationUpdater {

    private final CypherAggregationUpdater aggregator;
    private final AtomicBoolean forceClosed;

    private volatile @Nullable GraphImporter.ThreadLocalBatches threadLocalBatches;

    GraphAggregationUpdater(CypherAggregationUpdater aggregator) {
        this.aggregator = aggregator;
        this.forceClosed = new AtomicBoolean(false);
    }

    @Override
    public void update(AnyValue[] input) throws ProcedureException {
        try {
            aggregator.updateRow(this, input);
        } catch (Throwable T) {
            throw ProcedureException.invocationFailed("function", FUNCTION_NAME.toString(), T);
        }
    }

    @Override
    public void applyUpdates() {
        // end of morsel: hand pooled slots back; the session stays usable
        // and re-acquires lazily on the next update
        var session = this.threadLocalBatches;
        if (session != null) {
            session.releaseBatches();
        }
    }

    GraphImporter.ThreadLocalBatches sessionFor(GraphImporter importer) {
        var session = this.threadLocalBatches;
        if (session == null) {
            session = this.threadLocalBatches = importer.newThreadLocalBatches();
        }
        return session;
    }

    // called from CypherAggregationUpdater.close(); possibly another thread.
    // Idempotent; safe to race with the owner's applyUpdates.
    void forceClose() {
        if (forceClosed.compareAndSet(false, true)) {
            var session = this.threadLocalBatches;   // volatile read
            if (session != null) {
                session.close();
            }
        }
    }
}
