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

import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides access to thread-exclusive {@link LocalNodesBuilder} instances.
 * The builders are not thread-safe; a builder acquired from a provider is
 * owned by the calling thread until it is released.
 * <p>
 * Two strategies are offered:
 * <ul>
 *     <li>{@code ThreadLocalProvider} (default): one builder per accessing
 *     thread, stored in a thread-local. Acquire and release are plain
 *     thread-local lookups, making this the fastest variant. The number of
 *     builder instances is unbounded (one per thread that ever calls
 *     {@code acquire()}), so use it only when the accessing threads are
 *     under our control and match the import concurrency, e.g. tasks
 *     running on a GDS executor with {@code concurrency} threads.</li>
 *     <li>{@code PooledProvider}: a fixed pool of {@code concurrency}
 *     builders shared by all accessing threads; acquire claims from the
 *     pool and may block until a builder is available. Use it when the
 *     accessing threads are not under our control, i.e. potentially many,
 *     varying, or short-lived, e.g. Neo4j parallel runtime workers driving
 *     a Cypher projection, or Arrow request handler threads. This bounds
 *     the number of builder instances, and with it the memory footprint
 *     and the amount of buffered-but-unflushed data, at the cost of slower
 *     access (pool claim/release per acquire).</li>
 * </ul>
 *
 * The access pattern is the same for both providers:
 *
 * <pre>
 * LocalNodesBuilderProvider provider = ...
 * var slot = provider.acquire();
 * try {
 *     var builder = slot.get();
 *     // use the builder
 * } finally {
 *     slot.release();
 * }
 * </pre>
 **/
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
                var slot = pool.claim(timeout);
                // Pairs with the releaseFence in Slot#release(); see
                // LocalRelationshipsBuilderProvider.PooledProvider#acquire()
                // for why these fences are required and when they can be removed.
                VarHandle.acquireFence();
                return slot;
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
                // Orders our builder writes before the setOpaque state store
                // inside stormpot's release; pairs with the acquireFence in
                // PooledProvider#acquire().
                VarHandle.releaseFence();
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
