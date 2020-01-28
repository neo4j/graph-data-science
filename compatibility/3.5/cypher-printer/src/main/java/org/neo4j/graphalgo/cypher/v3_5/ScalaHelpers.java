/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.cypher.v3_5;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import scala.Tuple2;
import scala.collection.immutable.List;
import scala.collection.mutable.ListBuffer;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static java.util.Collections.emptySet;

@SuppressWarnings("TypeMayBeWeakened")
@Value.Style(
    builderVisibility = Value.Style.BuilderVisibility.PUBLIC,
    deferCollectionAllocation = true,
    depluralize = true,
    add = "add",
    addAll = "addAll"
)
final class ScalaHelpers {

    @Builder.Factory
    static <T> List<T> scalaList(java.util.List<T> items) {
        return items.stream().collect(toScala());
    }

    static <T, U> Tuple2<T, U> pair(T left, @Nullable U right) {
        return Tuple2.apply(left, right);
    }

    private static <T> Collector<T, ?, List<T>> toScala() {
        return new ScalaSeqCollector<>(ListBuffer::toList);
    }

    private static final class ScalaSeqCollector<T, R> implements
        Collector<T, ListBuffer<T>, R>,
        Supplier<ListBuffer<T>>,
        BiConsumer<ListBuffer<T>, T>,
        BinaryOperator<ListBuffer<T>> {
        private final Function<ListBuffer<T>, R> finisher;

        private ScalaSeqCollector(Function<ListBuffer<T>, R> finisher) {
            this.finisher = finisher;
        }

        @Override
        public Supplier<ListBuffer<T>> supplier() {
            return this;
        }

        @Override
        public ListBuffer<T> get() {
            return new ListBuffer<>();
        }

        @Override
        public BiConsumer<ListBuffer<T>, T> accumulator() {
            return this;
        }

        @Override
        public void accept(ListBuffer<T> buffer, T t) {
            buffer.$plus$eq(t);
        }

        @Override
        public BinaryOperator<ListBuffer<T>> combiner() {
            return this;
        }

        @Override
        public ListBuffer<T> apply(ListBuffer<T> left, ListBuffer<T> right) {
            left.$plus$plus$eq(right);
            return left;
        }

        @Override
        public Function<ListBuffer<T>, R> finisher() {
            return finisher;
        }

        @Override
        public java.util.Set<Characteristics> characteristics() {
            return emptySet();
        }
    }

    private ScalaHelpers() {}
}
