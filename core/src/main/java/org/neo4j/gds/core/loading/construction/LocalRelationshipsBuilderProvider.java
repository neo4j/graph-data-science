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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.function.Supplier;

sealed interface LocalRelationshipsBuilderProvider extends AutoCloseable {

    static LocalRelationshipsBuilderProvider threadLocal(Supplier<LocalRelationshipsBuilder> builderSupplier) {
        return new ThreadLocalProvider(builderSupplier);
    }

    LocalRelationshipsBuilderSlot acquire();

    interface LocalRelationshipsBuilderSlot {
        LocalRelationshipsBuilder get();

        void release();
    }

    final class ThreadLocalProvider implements LocalRelationshipsBuilderProvider {
        private final AutoCloseableThreadLocal<Slot> threadLocal;

        private ThreadLocalProvider(Supplier<LocalRelationshipsBuilder> builderSupplier) {
            this.threadLocal = AutoCloseableThreadLocal.withInitial(() -> new Slot(builderSupplier.get()));
        }

        @Override
        public LocalRelationshipsBuilderSlot acquire() {
            return threadLocal.get();
        }

        @Override
        public void close() throws Exception {
            threadLocal.close();
        }

        record Slot(LocalRelationshipsBuilder builder) implements LocalRelationshipsBuilderSlot, AutoCloseable {
            @Override
            public LocalRelationshipsBuilder get() {
                return builder;
            }

            @Override
            public void release() {

            }

            @Override
            public void close() throws Exception {
                builder.close();
            }
        }
    }
}
