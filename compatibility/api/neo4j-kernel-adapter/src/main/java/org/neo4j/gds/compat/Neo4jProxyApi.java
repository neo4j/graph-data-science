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
package org.neo4j.gds.compat;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import java.util.Optional;
import java.util.function.Function;

public interface Neo4jProxyApi {

    @CompatSince(Neo4jVersion.V_5_12)
    CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer);

    @CompatSince(Neo4jVersion.V_5_12)
    DependencyResolver emptyDependencyResolver();

    @CompatSince(Neo4jVersion.V_5_14)
    String neo4jArrowServerAddressHeader();

    @CompatSince(Neo4jVersion.V_5_14)
    <T> T nodeLabelTokenSet(
        NodeCursor nodeCursor,
        Function<int[], T> intsConstructor,
        Function<long[], T> longsConstructor
    );

    @CompatSince(Neo4jVersion.V_5_15)
    String metricsManagerClass();

    @CompatSince(value = Neo4jVersion.V_Dev, dev = "5.17")
    long estimateNodeCount(Read read, int label);

    @CompatSince(value = Neo4jVersion.V_Dev, dev = "5.17")
    long estimateRelationshipCount(Read read, int sourceLabel, int targetLabel, int type);
}
