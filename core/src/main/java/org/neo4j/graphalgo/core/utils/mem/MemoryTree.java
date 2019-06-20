package org.neo4j.graphalgo.core.utils.mem;

import java.util.Collection;
import java.util.Collections;

/**
 * A tree shaped description of an object that has resources residing in memory.
 */
public interface MemoryTree {

    /**
     * @return a textual description for this component.
     */
    String description();

    /**
     * @return The resident memory of this component.
     */
    MemoryRange memoryUsage();

    /**
     * @return nested resources of this component.
     */
    default Collection<MemoryTree> components() {
        return Collections.emptyList();
    }

    /**
     * Renders the memory requirements into a human readable representation.
     */
    default String render() {
        StringBuilder sb = new StringBuilder();
        render(sb, this, 0);
        return sb.toString();
    }

    static void render(
            final StringBuilder sb,
            final MemoryTree estimation,
            final int depth) {
        for (int i = 1; i < depth; i++) {
            sb.append("    ");
        }

        if (depth > 0) {
            sb.append("|-- ");
        }

        sb.append(estimation.description());
        sb.append(": ");
        sb.append(estimation.memoryUsage());
        sb.append(System.lineSeparator());

        for (final MemoryTree component : estimation.components()) {
            render(sb, component, depth + 1);
        }
    }

    static MemoryTree empty() {
        return NULL_TREE;
    }

    MemoryTree NULL_TREE = new MemoryTree() {
        @Override
        public String description() {
            return "";
        }

        @Override
        public MemoryRange memoryUsage() {
            return MemoryRange.empty();
        }
    };
}
