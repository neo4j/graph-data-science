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

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;
import stormpot.Pool;
import stormpot.Poolable;
import stormpot.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

abstract class LocalRelationshipsBuilderProvider implements AutoCloseable {

    static LocalRelationshipsBuilderProvider threadLocal(Supplier<LocalRelationshipsBuilder> builderSupplier) {
        return new ThreadLocalProvider(builderSupplier);
    }

    static LocalRelationshipsBuilderProvider pooled(
        Supplier<LocalRelationshipsBuilder> builderSupplier,
        Concurrency concurrency
    ) {
        return PooledProvider.create(builderSupplier, concurrency);
    }

    abstract LocalRelationshipsBuilderSlot acquire();

    interface LocalRelationshipsBuilderSlot {
        LocalRelationshipsBuilder get();

        void release();
    }

    private static final class ThreadLocalProvider extends LocalRelationshipsBuilderProvider {
        private final AutoCloseableThreadLocal<Slot> threadLocal;

        private ThreadLocalProvider(Supplier<LocalRelationshipsBuilder> builderSupplier) {
            this.threadLocal = AutoCloseableThreadLocal.withInitial(() -> new Slot(builderSupplier.get()));
        }

        @Override
        LocalRelationshipsBuilderSlot acquire() {
            return threadLocal.get();
        }

        @Override
        public void close() {
            threadLocal.close();
        }

        private record Slot(LocalRelationshipsBuilder builder) implements LocalRelationshipsBuilderSlot, AutoCloseable {
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

    private static final class PooledProvider extends LocalRelationshipsBuilderProvider {
        private final Pool<Slot> pool;
        private final Timeout timeout = new Timeout(1, TimeUnit.HOURS);

        static LocalRelationshipsBuilderProvider create(
            Supplier<LocalRelationshipsBuilder> builderSupplier,
            Concurrency concurrency
        ) {
            var pool = Pool
                .fromInline(new Allocator(builderSupplier))
                .setSize(concurrency.value())
                .build();

            return new PooledProvider(pool);
        }

        private PooledProvider(Pool<Slot> pool) {
            this.pool = pool;
        }

        @Override
        LocalRelationshipsBuilderSlot acquire() {
            try {
                return pool.claim(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            pool.shutdown().await(timeout);
        }

        private record Slot(stormpot.Slot slot, LocalRelationshipsBuilder builder) implements Poolable, LocalRelationshipsBuilderSlot {
            @Override
            public LocalRelationshipsBuilder get() {
                return builder;
            }

            @Override
            public void release() {
                slot.release(this);
            }
        }

        private static final class Allocator implements stormpot.Allocator<Slot> {
            private final Supplier<LocalRelationshipsBuilder> builderSupplier;

            Allocator(Supplier<LocalRelationshipsBuilder> builderSupplier) {
                this.builderSupplier = builderSupplier;
            }

            @Override
            public Slot allocate(stormpot.Slot slot) {
                return new Slot(slot, builderSupplier.get());
            }

            @Override
            public void deallocate(Slot slot) throws Exception {
                slot.builder.close();
            }
        }
    }
}
