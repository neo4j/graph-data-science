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
package org.neo4j.gds.utils;

import org.immutables.builder.Builder;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class AutoCloseableThreadLocal<T extends AutoCloseable> extends CloseableThreadLocal<T> implements Supplier<T>, AutoCloseable {

    private final Consumer<? super T> destructor;

    public static <T extends AutoCloseable> AutoCloseableThreadLocal<T> withInitial(CheckedSupplier<T, ?> initial) {
        return new AutoCloseableThreadLocal<>(initial, Optional.empty());
    }

    @Builder.Constructor
    public AutoCloseableThreadLocal(
        @Builder.Parameter Supplier<T> constructor,
        Optional<Consumer<? super T>> destructor
    ) {
        super(constructor);
        this.destructor = destructor.orElse(doNothing -> {});
    }

    @Override
    public void close() {
        var error = new AtomicReference<RuntimeException>();
        for (T item : hardRefs.values()) {
            try (item) {
                destructor.accept(item);
            } catch (RuntimeException e) {
                error.set(ExceptionUtil.chain(error.get(), e));
            } catch (Exception e) {
                error.set(ExceptionUtil.chain(error.get(), new RuntimeException(e)));
            }
        }
        var errorWhileClosing = error.get();
        if (errorWhileClosing != null) {
            throw errorWhileClosing;
        }

        // Clear the hard refs; then, the only remaining refs to
        // all values we were storing are weak (unless somewhere
        // else is still using them) and so GC may reclaim them:
        hardRefs = null;
        // Take care of the current thread right now; others will be
        // taken care of via the WeakReferences.
        if (t != null) {
            t.remove();
        }
        t = null;
    }
}
