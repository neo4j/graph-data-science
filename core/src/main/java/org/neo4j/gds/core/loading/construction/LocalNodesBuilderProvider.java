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

abstract class LocalNodesBuilderProvider {

    static LocalNodesBuilderProvider threadLocal(Supplier<LocalNodesBuilder> builderSupplier) {
        return new ThreadLocalProvider(builderSupplier);
    }

    static LocalNodesBuilderProvider pooled(
        Supplier<LocalNodesBuilder> builderSupplier,
        Concurrency concurrency
    ) {
        return PooledProvider.create(builderSupplier, concurrency);
    }

    abstract LocalNodesBuilderSlot acquire();
    abstract void close();

    interface LocalNodesBuilderSlot {
        LocalNodesBuilder get();

        void release();
    }

    private static final class ThreadLocalProvider extends LocalNodesBuilderProvider {
        private final AutoCloseableThreadLocal<Slot> threadLocal;

        private ThreadLocalProvider(Supplier<LocalNodesBuilder> builderSupplier) {
            this.threadLocal = AutoCloseableThreadLocal.withInitial(() -> new Slot(builderSupplier.get()));
        }

        @Override
        LocalNodesBuilderSlot acquire() {
            return threadLocal.get();
        }

        @Override
        public void close() {
            threadLocal.close();
        }

        private record Slot(LocalNodesBuilder builder) implements LocalNodesBuilderSlot, AutoCloseable {
            @Override
            public LocalNodesBuilder get() {
                return builder;
            }

            @Override
            public void release() {

            }

            @Override
            public void close() {
                builder.close();
            }
        }
    }

    private static final class PooledProvider extends LocalNodesBuilderProvider {
        private final Pool<Slot> pool;
        private final Timeout timeout = new Timeout(1, TimeUnit.HOURS);

        static LocalNodesBuilderProvider create(
            Supplier<LocalNodesBuilder> builderSupplier,
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
        LocalNodesBuilderSlot acquire() {
            try {
                return pool.claim(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void close() {
            try {
                pool.shutdown().await(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private record Slot(stormpot.Slot slot, LocalNodesBuilder builder) implements Poolable, LocalNodesBuilderSlot {
            @Override
            public LocalNodesBuilder get() {
                return builder;
            }

            @Override
            public void release() {
                slot.release(this);
            }
        }

        private static final class Allocator implements stormpot.Allocator<Slot> {
            private final Supplier<LocalNodesBuilder> builderSupplier;

            Allocator(Supplier<LocalNodesBuilder> builderSupplier) {
                this.builderSupplier = builderSupplier;
            }

            @Override
            public Slot allocate(stormpot.Slot slot) {
                return new Slot(slot, builderSupplier.get());
            }

            @Override
            public void deallocate(Slot slot) {
                slot.builder.close();
            }
        }
    }
}
