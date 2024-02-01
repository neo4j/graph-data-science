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
package org.neo4j.gds.compat._513;

import org.neo4j.common.DependencyResolver;
import org.neo4j.gds.compat.CompatAccessModeImpl;
import org.neo4j.gds.compat.CustomAccessMode;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.ReadSecurityPropertyProvider;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

import java.util.Optional;
import java.util.function.Supplier;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    @Override
    public String metricsManagerClass() {
        return "com.neo4j.metrics.global.MetricsManager";
    }

    private static final DependencyResolver EMPTY_DEPENDENCY_RESOLVER = new DependencyResolver() {
        @Override
        public <T> T resolveDependency(Class<T> type, SelectionStrategy selector) {
            return null;
        }

        @Override
        public boolean containsDependency(Class<?> type) {
            return false;
        }
    };

    @Override
    public DependencyResolver emptyDependencyResolver() {
        return EMPTY_DEPENDENCY_RESOLVER;
    }

    @Override
    public CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer) {
        return pageCacheTracer.map(cacheTracer -> new CursorContextFactory(
            cacheTracer,
            FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER
        )).orElse(CursorContextFactory.NULL_CONTEXT_FACTORY);
    }

    @Override
    public AccessMode accessMode(CustomAccessMode customAccessMode) {
        return new CompatAccessModeImpl(customAccessMode) {
            @Override
            public boolean allowsReadNodeProperty(
                Supplier<TokenSet> labels,
                int propertyKey,
                ReadSecurityPropertyProvider propertyProvider
            ) {
                return custom.allowsReadNodeProperty(propertyKey);
            }
        };
    }

    @Override
    public String neo4jArrowServerAddressHeader() {
        throw new UnsupportedOperationException("Not implemented for Neo4j versions <5.14");
    }

    @Override
    public long estimateNodeCount(Read read, int label) {
        return read.countsForNodeWithoutTxState(label);
    }

    @Override
    public long estimateRelationshipCount(Read read, int sourceLabel, int targetLabel, int type) {
        return read.countsForRelationshipWithoutTxState(sourceLabel, type, targetLabel);
    }
}
