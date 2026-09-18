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
import org.neo4j.gds.utils.GdsFeatureToggles;
import stormpot.Pool;
import stormpot.Poolable;
import stormpot.Timeout;

import java.lang.invoke.VarHandle;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides access to thread-exclusive {@link LocalRelationshipsBuilder} instances,
 * either via thread-locals (fast, one builder per accessing thread) or via a
 * fixed-size pool (bounded builder count, for callers whose threads we do not
 * control). See {@link LocalNodesBuilderProvider} for a discussion of when to
 * use which strategy; the same criteria apply here.
 **/
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

    /**
     * Tries to acquire a slot without blocking. Returns {@code null} if no
     * builder is readily available. Callers that would otherwise block while
     * holding other slots can use this to release their claims first, keeping
     * the invariant that a session only blocks while holding nothing.
     */
    abstract LocalRelationshipsBuilderSlot tryAcquire();

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
        LocalRelationshipsBuilderSlot tryAcquire() {
            // a thread-local builder is always readily available
            return acquire();
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
        // The timeout is captured at provider construction; tests that lower
        // GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS must do so before
        // creating the builder.
        private final Timeout timeout = new Timeout(
            GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.get(),
            TimeUnit.SECONDS
        );

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
                var slot = pool.claim(timeout);
                // This fence pairs with the releaseFence in Slot#release().
                // Together they restore the "release happens-before subsequent
                // claim" guarantee that stormpot documents but does not deliver:
                // Stormpot uses a fast-path for acquiring a slot: every thread
                // stores a reference to the last slot it used, i.e., the same
                // slot can be referenced by multiple threads. That fast-path
                // re-claims the slot via a CAS on the slot state, whose value
                // stormpot's release sets using setOpaque. Under the Java
                // Memory Model, an opaque write observed by an acquire-CAS
                // establishes no happens-before. This means that thread B can
                // see the state flipped to LIVING before A's writes to the
                // pooled builder are visible. On weakly-ordered CPUs like the
                // M-series, this leads to problems, while on x86 we have a
                // stronger memory model where this case cannot happen.
                //
                // For our specific case, this means that B can read a buffer
                // length before A has finished writing and therefore we lose
                // elements during import (PFORM-429).
                //
                // If stormpot ever publishes the state with release semantics
                // (setRelease), both fences can be removed.
                VarHandle.acquireFence();
                if (slot == null) {
                    throw new IllegalStateException(String.format(
                        Locale.US,
                        "Timed out after %d seconds waiting for a pooled relationships builder slot. " +
                            "This usually means a concurrent importer died while holding a slot.",
                        GdsFeatureToggles.POOLED_BUILDER_TIMEOUT_SECONDS.get()
                    ));
                }
                return slot;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        LocalRelationshipsBuilderSlot tryAcquire() {
            // claim with a zero timeout: never parks, returns null when the
            // pool is drained. The same visibility fence as acquire() applies.
            var slot = pool.tryClaim();
            VarHandle.acquireFence();
            return slot;
        }

        @Override
        public void close() throws Exception {
            if (!pool.shutdown().await(timeout)) {
                throw new IllegalStateException(
                    "Timed out waiting for pooled relationships builder slots to be released. " +
                        "An importer likely died while holding a slot."
                );
            }
        }

        private record Slot(stormpot.Slot slot, LocalRelationshipsBuilder builder) implements Poolable, LocalRelationshipsBuilderSlot {
            @Override
            public LocalRelationshipsBuilder get() {
                return builder;
            }

            @Override
            public void release() {
                // Orders our builder writes before the setOpaque state store
                // inside stormpot's release; pairs with the acquireFence in
                // PooledProvider#acquire(), see there for details.
                VarHandle.releaseFence();
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
