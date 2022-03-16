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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.graphdb.Label;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class NodeLabelTokens {

    public static @NotNull NodeLabelToken of(@Nullable Object nodeLabels) {
        return Objects.requireNonNullElseGet(ofNullable(nodeLabels), () -> {
            throw new IllegalArgumentException(formatWithLocale(
                "Could not represent a value of type[%s] as nodeLabels: %s",
                nodeLabels.getClass(),
                nodeLabels
            ));
        });
    }

    public static @Nullable NodeLabelToken ofNullable(@Nullable Object nodeLabels) {
        if (nodeLabels == null) {
            return empty();
        }

        if (nodeLabels instanceof NodeLabel[]) {
            return ofNodeLabels((NodeLabel[]) nodeLabels);
        }

        if (nodeLabels instanceof String[]) {
            return new Array<>((String[]) nodeLabels, NodeLabelTokens::labelOf);
        }

        if (nodeLabels instanceof Label[]) {
            return new Array<>((Label[]) nodeLabels, NodeLabelTokens::labelOf);
        }

        if (nodeLabels instanceof List) {
            List<?> labels = (List<?>) nodeLabels;
            return ofList(labels);
        }

        return ofSingleton(nodeLabels);
    }

    public static @NotNull NodeLabelToken empty() {
        return Null.INSTANCE;
    }

    public static @NotNull NodeLabelToken ofNodeLabels(NodeLabel... nodeLabels) {
        return new Array<>(nodeLabels, Function.identity());
    }

    private static @Nullable NodeLabelToken ofList(List<?> nodeLabels) {
        if (nodeLabels.isEmpty()) {
            return empty();
        }

        Object label = nodeLabels.get(0);

        if (nodeLabels.size() == 1) {
            return ofSingleton(label);
        }

        if (label instanceof NodeLabel) {
            //noinspection unchecked
            return new JavaList<>((List<NodeLabel>) nodeLabels, Function.identity());
        }
        if (label instanceof String) {
            //noinspection unchecked
            return new JavaList<>((List<String>) nodeLabels, NodeLabelTokens::labelOf);
        }
        if (label instanceof Label) {
            //noinspection unchecked
            return new JavaList<>((List<Label>) nodeLabels, NodeLabelTokens::labelOf);
        }

        return null;
    }

    private static @Nullable NodeLabelToken ofSingleton(Object nodeLabel) {
        if (nodeLabel instanceof NodeLabel) {
            return new Singleton<>((NodeLabel) nodeLabel, Function.identity());
        }

        if (nodeLabel instanceof String) {
            return new Singleton<>((String) nodeLabel, NodeLabelTokens::labelOf);
        }

        if (nodeLabel instanceof Label) {
            return new Singleton<>((Label) nodeLabel, NodeLabelTokens::labelOf);
        }

        return null;
    }

    private static NodeLabel labelOf(String label) {
        return new NodeLabel(label);
    }

    private static NodeLabel labelOf(Label label) {
        return new NodeLabel(label.name());
    }

    private enum Null implements NodeLabelToken {
        INSTANCE;

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public @NotNull NodeLabel get(int index) {
            throw new NoSuchElementException();
        }
    }

    private static final class Array<T> implements NodeLabelToken {
        private final T[] nodeLabels;

        private final Function<T, NodeLabel> toNodeLabel;

        private Array(T[] nodeLabels, Function<T, NodeLabel> toNodeLabel) {
            this.nodeLabels = nodeLabels;
            this.toNodeLabel = toNodeLabel;
        }

        @Override
        public boolean isEmpty() {
            return nodeLabels.length == 0;
        }

        @Override
        public int size() {
            return nodeLabels.length;
        }

        @Override
        public @NotNull NodeLabel get(int index) {
            return toNodeLabel.apply(nodeLabels[index]);
        }
    }

    private static final class JavaList<T> implements NodeLabelToken {
        private final List<T> nodeLabels;

        private final Function<T, NodeLabel> toNodeLabel;

        private JavaList(List<T> nodeLabels, Function<T, NodeLabel> toNodeLabel) {
            this.nodeLabels = nodeLabels;
            this.toNodeLabel = toNodeLabel;
        }

        @Override
        public boolean isEmpty() {
            return nodeLabels == null || nodeLabels.isEmpty();
        }

        @Override
        public int size() {
            return nodeLabels.size();
        }

        @Override
        public @NotNull NodeLabel get(int index) {
            return toNodeLabel.apply(nodeLabels.get(index));
        }
    }

    private static final class Singleton<T> implements NodeLabelToken {

        private final NodeLabel item;

        private Singleton(T item, Function<T, NodeLabel> toNodeLabel) {
            this.item = toNodeLabel.apply(item);
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public @NotNull NodeLabel get(int index) {
            assert index == 0;
            return item;
        }
    }

    private NodeLabelTokens() {}
}
