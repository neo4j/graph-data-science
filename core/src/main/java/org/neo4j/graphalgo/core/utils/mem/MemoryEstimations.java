package org.neo4j.graphalgo.core.utils.mem;

import org.neo4j.graphalgo.core.GraphDimensions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntToLongFunction;
import java.util.function.LongFunction;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

public final class MemoryEstimations {

    @FunctionalInterface
    public interface MemoryRangeModifier {
        MemoryRange apply(MemoryRange range, GraphDimensions dimensions, int concurrency);
    }

    public static MemoryEstimation empty() {
        return NULL_ESTIMATION;
    }

    public static MemoryEstimation of(final Class<?> instanceType) {
        return new LeafEstimation("instance", MemoryResidents.fixed(MemoryRange.of(sizeOfInstance(instanceType))));
    }

    public static MemoryEstimation of(final String description, final MemoryResident resident) {
        return new LeafEstimation(description, resident);
    }

    private static MemoryEstimation of(final String description, final Collection<MemoryEstimation> components) {
        return new CompositeEstimation(description, components);
    }

    public static Builder builder(final String description) {
        return new Builder(description);
    }

    public static Builder builder(final Class<?> type) {
        return new Builder(type.getSimpleName(), type);
    }

    public static Builder builder(final String description, final Class<?> type) {
        return new Builder(description, type);
    }

    public static final class Builder {

        private final String description;
        private final Collection<MemoryEstimation> components = new ArrayList<>();
        private Builder parent = null;

        private Builder(final String description) {
            this.description = description;
        }

        private Builder(final String description, final Class<?> type) {
            this(description);
            field("this.instance", type);
        }

        private Builder(final String description, final Class<?> type, final Builder parent) {
            this(description, type);
            this.parent = parent;
        }

        public Builder startField(final String name, final Class<?> type) {
            return new Builder(name, type, this);
        }

        public Builder endField() {
            if (parent == null) {
                throw new IllegalArgumentException("Cannot end field from root builder");
            }
            parent.components.add(build());
            return parent;
        }

        public Builder add(final MemoryEstimation estimation) {
            components.add(estimation);
            return this;
        }

        public Builder add(final String name, final MemoryEstimation estimation) {
            components.add(new DelegateEstimation(estimation, name));
            return this;
        }

        public Builder field(final String name, final Class<?> type) {
            components.add(new LeafEstimation(name, MemoryResidents.fixed(MemoryRange.of(sizeOfInstance(type)))));
            return this;
        }

        public Builder fixed(final String description, final long bytes) {
            components.add(new LeafEstimation(description, MemoryResidents.fixed(MemoryRange.of(bytes))));
            return this;
        }

        public Builder fixed(final String description, final MemoryRange range) {
            components.add(new LeafEstimation(description, MemoryResidents.fixed(range)));
            return this;
        }

        public Builder perNode(final String description, final LongUnaryOperator fn) {
            components.add(new LeafEstimation(description, MemoryResidents.perNode(fn)));
            return this;
        }

        public Builder perNode(final String description, final MemoryEstimation subComponent) {
            components.add(new AndThenEstimation(description, subComponent, (mem, dim, concurrency) -> mem.times(dim.hugeNodeCount())));
            return this;
        }

        public Builder perGraphDimension(final String description, final ToLongFunction<GraphDimensions> fn) {
            components.add(new LeafEstimation(description, MemoryResidents.perDim(fn)));
            return this;
        }

        public Builder rangePerNode(final String description, final LongFunction<MemoryRange> fn) {
            components.add(new LeafEstimation(description, MemoryResidents.perNode(fn)));
            return this;
        }

        public Builder perThread(final String description, final IntToLongFunction fn) {
            components.add(new LeafEstimation(description, MemoryResidents.perThread(fn)));
            return this;
        }

        public Builder perThread(final String description, final MemoryEstimation subComponent) {
            components.add(new AndThenEstimation(description, subComponent, (mem, dim, concurrency) -> mem.times(concurrency)));
            return this;
        }

        public MemoryEstimation build() {
            return of(description, components);
        }
    }

    private static final MemoryEstimation NULL_ESTIMATION = new MemoryEstimation() {
        @Override
        public String description() {
            return "";
        }

        @Override
        public MemoryResident resident() {
            return MemoryResidents.empty();
        }

        @Override
        public MemoryTree apply(final GraphDimensions dimensions, final int concurrency) {
            return MemoryTree.empty();
        }
    };

    private MemoryEstimations() {
        throw new UnsupportedOperationException("No instances");
    }
}

abstract class BaseEstimation implements MemoryEstimation {
    private final String description;

    BaseEstimation(final String description) {
        this.description = description;
    }

    @Override
    public final String description() {
        return description;
    }

    @Override
    public final String toString() {
        return description();
    }
}

final class LeafEstimation extends BaseEstimation {
    private final MemoryResident resident;

    LeafEstimation(final String description, final MemoryResident resident) {
        super(description);
        this.resident = resident;
    }

    @Override
    public MemoryTree apply(final GraphDimensions dimensions, final int concurrency) {
        MemoryRange memoryRange = resident.estimateMemoryUsage(dimensions, concurrency);
        return new LeafTree(description(), memoryRange);
    }
}

final class AndThenEstimation extends BaseEstimation {
    private final MemoryEstimation delegate;
    private final MemoryEstimations.MemoryRangeModifier andThen;

    AndThenEstimation(
            final String description,
            final MemoryEstimation delegate,
            final MemoryEstimations.MemoryRangeModifier andThen) {
        super(description);
        this.delegate = delegate;
        this.andThen = andThen;
    }

    @Override
    public Collection<MemoryEstimation> components() {
        return delegate.components();
    }

    @Override
    public MemoryResident resident() {
        return (dimensions, concurrency) -> {
            MemoryResident resident = delegate.resident();
            MemoryRange memoryRange = resident.estimateMemoryUsage(dimensions, concurrency);
            return andThen.apply(memoryRange, dimensions, concurrency);
        };
    }

    @Override
    public MemoryTree apply(final GraphDimensions dimensions, final int concurrency) {
        MemoryTree memoryTree = delegate.apply(dimensions, concurrency);
        return new AndThenTree(description(), memoryTree, range -> andThen.apply(range, dimensions, concurrency));
    }
}

final class CompositeEstimation extends BaseEstimation {
    private final Collection<MemoryEstimation> components;

    CompositeEstimation(
            final String description,
            final Collection<MemoryEstimation> components) {
        super(description);
        this.components = components;
    }

    @Override
    public Collection<MemoryEstimation> components() {
        return components;
    }

    @Override
    public MemoryResident resident() {
        return components.stream()
                .map(MemoryEstimation::resident)
                .collect(Collectors.collectingAndThen(
                        Collectors.toList(),
                        MemoryResidents::composite
                ));
    }

    @Override
    public MemoryTree apply(final GraphDimensions dimensions, final int concurrency) {
        List<MemoryTree> newComponent = components.stream()
                .map(e -> e.apply(dimensions, concurrency))
                .collect(Collectors.toList());
        return new CompositeTree(description(), newComponent);
    }
}

final class DelegateEstimation extends BaseEstimation {
    private final MemoryEstimation delegate;

    DelegateEstimation(final MemoryEstimation delegate, final String description) {
        super(description);
        this.delegate = delegate;
    }

    @Override
    public Collection<MemoryEstimation> components() {
        return delegate.components();
    }

    @Override
    public MemoryResident resident() {
        return delegate.resident();
    }

    @Override
    public MemoryTree apply(final GraphDimensions dimensions, final int concurrency) {
        return new DelegateTree(delegate.apply(dimensions, concurrency), description());
    }
}

abstract class BaseTree implements MemoryTree {
    private final String description;

    BaseTree(final String description) {
        this.description = description;
    }

    @Override
    public final String description() {
        return description;
    }

    @Override
    public final String toString() {
        return description();
    }
}

final class LeafTree extends BaseTree {
    private final MemoryRange range;

    LeafTree(final String description, final MemoryRange range) {
        super(description);
        this.range = range;
    }

    @Override
    public MemoryRange memoryUsage() {
        return range;
    }
}

final class AndThenTree extends BaseTree {
    private final MemoryTree delegate;
    private final UnaryOperator<MemoryRange> andThen;

    AndThenTree(
            final String description,
            final MemoryTree delegate,
            final UnaryOperator<MemoryRange> andThen) {
        super(description);
        this.delegate = delegate;
        this.andThen = andThen;
    }

    @Override
    public MemoryRange memoryUsage() {
        return andThen.apply(delegate.memoryUsage());
    }

    @Override
    public Collection<MemoryTree> components() {
        return delegate.components();
    }
}


final class CompositeTree extends BaseTree {
    private final Collection<MemoryTree> components;

    CompositeTree(
            final String description,
            final Collection<MemoryTree> components) {
        super(description);
        this.components = components;
    }

    @Override
    public Collection<MemoryTree> components() {
        return components;
    }

    @Override
    public MemoryRange memoryUsage() {
        return components.stream()
                .map(MemoryTree::memoryUsage)
                .reduce(MemoryRange.empty(), MemoryRange::add);
    }
}

final class DelegateTree extends BaseTree {
    private final MemoryTree delegate;

    DelegateTree(final MemoryTree delegate, final String description) {
        super(description);
        this.delegate = delegate;
    }

    @Override
    public Collection<MemoryTree> components() {
        return delegate.components();
    }

    @Override
    public MemoryRange memoryUsage() {
        return delegate.memoryUsage();
    }
}
