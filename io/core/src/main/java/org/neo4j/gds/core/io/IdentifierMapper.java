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
package org.neo4j.gds.core.io;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface IdentifierMapper<T> {
    default String identifierFor(T name) {
        throw new NoSuchElementException(formatWithLocale("no identifier for the name '%s' exists.", name));
    }

    default @NotNull T forIdentifier(String identifier) {
        throw new NoSuchElementException(formatWithLocale("no name for the identifier '%s' exists.", identifier));
    }

    default Collection<String> identifiers() {
        return Collections.emptyList();
    }

    default void forEach(BiConsumer<? super T, ? super String> action) {
    }

    static <T> Builder<T> builder(String prefix) {
        return new Builder<>(prefix);
    }

    static <T> IdentifierMapper<T> empty() {
        //noinspection unchecked
        return (IdentifierMapper<T>) EmptyMapper.INSTANCE;
    }

    static IdentifierMapper<String> identity() {
        return biject(Function.identity(), Function.identity());
    }

    static <T> IdentifierMapper<T> biject(Function<T, String> inject, Function<String, T> surject) {
        return new BijectionMapper<>(inject, surject);
    }

    final class Builder<T> {

        private final String prefix;
        private final Map<T, String> identifierMapping = new ConcurrentHashMap<>();
        // TODO this might not be needed
        private final Map<String, T> reverseMapping = new ConcurrentHashMap<>();
        private final AtomicInteger count = new AtomicInteger(1);

        private Builder(String prefix) {
            this.prefix = prefix;
        }

        public String getOrCreateIdentifierFor(T name) {
            var identifier = this.identifierMapping.computeIfAbsent(
                name,
                __ -> this.prefix + this.count.getAndIncrement()
            );
            this.addReverseMapping(name, identifier);
            return identifier;
        }

        public Builder<T> setIdentifierMapping(T name, String identifier) {
            String actualMapping = this.identifierMapping.putIfAbsent(name, identifier);
            if (actualMapping != null && !Objects.equals(actualMapping, identifier)) {
                throw new IllegalStateException(
                    formatWithLocale(
                        "Encountered multiple different identifiers for the same name: {Name=%s Identifier1=%s Identifier2=%s}",
                        name,
                        actualMapping,
                        identifier
                    ));
            }

            this.addReverseMapping(name, identifier);

            return this;
        }

        public IdentifierMapper<T> build() {
            return new RealMapper<>(this.identifierMapping, this.reverseMapping);
        }

        private void addReverseMapping(T name, String identifier) {
            var reverseName = this.reverseMapping.putIfAbsent(identifier, name);
            if (reverseName != null && !Objects.equals(reverseName, name)) {
                throw new IllegalStateException(
                    formatWithLocale(
                        "Generated the same identifier for multiple different names: {Identifier=%s Name1=%s Name2=%s}",
                        identifier,
                        name,
                        reverseName
                    ));
            }
        }
    }

}

enum EmptyMapper implements IdentifierMapper<Object> {
    INSTANCE
}

final class BijectionMapper<T> implements IdentifierMapper<T> {
    private final Function<T, String> inject;
    private final Function<String, T> surject;

    BijectionMapper(Function<T, String> inject, Function<String, T> surject) {
        this.inject = inject;
        this.surject = surject;
    }


    @Override
    public String identifierFor(T name) {
        return this.inject.apply(name);
    }

    @Override
    public @NotNull T forIdentifier(String identifier) {
        return this.surject.apply(identifier);
    }
}

final class RealMapper<T> implements IdentifierMapper<T> {

    private final Map<T, String> identifierMapping;
    private final Map<String, T> reverseMapping;

    RealMapper(Map<T, String> identifierMapping, Map<String, T> reverseMapping) {
        this.identifierMapping = identifierMapping;
        this.reverseMapping = reverseMapping;
    }

    @Override
    public String identifierFor(T name) {
        return Objects.requireNonNullElseGet(
            this.identifierMapping.get(name),
            () -> IdentifierMapper.super.identifierFor(name)
        );
    }

    @Override
    public @NotNull T forIdentifier(String identifier) {
        return Objects.requireNonNullElseGet(
            this.reverseMapping.get(identifier),
            () -> IdentifierMapper.super.forIdentifier(identifier)
        );
    }

    @Override
    public Collection<String> identifiers() {
        return Collections.unmodifiableCollection(this.identifierMapping.values());
    }

    @Override
    public void forEach(BiConsumer<? super T, ? super String> action) {
        this.identifierMapping.forEach(action);
    }
}
