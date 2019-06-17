/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core;

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.GraphSetup;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressLoggerAdapter;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The GraphLoader provides a fluent interface and default values to configure
 * the {@link Graph} before loading it.
 * <p>
 * By default, the complete graph is loaded – no restriction based on
 * node label or relationship type is made.
 * Weights are also not loaded by default.
 *
 * @author mknobloch
 */
public class GraphLoader {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    private static final MethodType CTOR_METHOD = MethodType.methodType(
            void.class,
            GraphDatabaseAPI.class,
            GraphSetup.class);

    private String name = null;
    private String label = null;
    private String relation = null;
    private String relWeightProp = null;
    private String nodeWeightProp = null;
    private String nodeProp = null;
    private Direction direction = Direction.BOTH;

    private final GraphDatabaseAPI api;
    private ExecutorService executorService;
    private double relWeightDefault = 0.0;
    private double nodeWeightDefault = 0.0;
    private final Map<String, Object> params = new HashMap<>();
    private double nodePropDefault = 0.0;
    private int batchSize = ParallelUtil.DEFAULT_BATCH_SIZE;
    private int concurrency;

    private DuplicateRelationshipsStrategy duplicateRelationshipsStrategy = DuplicateRelationshipsStrategy.NONE;

    private Log log = NullLog.getInstance();
    private long logMillis = -1;
    private AllocationTracker tracker = AllocationTracker.EMPTY;
    private boolean sort = false;
    private boolean loadAsUndirected = false;
    private PropertyMapping[] nodePropertyMappings = new PropertyMapping[0];

    /**
     * Creates a new serial GraphLoader.
     */
    public GraphLoader(GraphDatabaseAPI api) {
        this.api = Objects.requireNonNull(api);
        this.executorService = null;
        this.concurrency = Pools.DEFAULT_CONCURRENCY;
    }

    /**
     * Creates a new parallel GraphLoader.
     * What exactly parallel means depends on the {@link GraphFactory}
     * implementation provided in {@link #load(Class)}.
     */
    public GraphLoader(GraphDatabaseAPI api, ExecutorService executorService) {
        this.api = Objects.requireNonNull(api);
        this.executorService = Objects.requireNonNull(executorService);
        this.concurrency = Pools.DEFAULT_CONCURRENCY;
    }

    /**
     * Use the given {@link Log}instance to log the progress during loading.
     */
    public GraphLoader withLog(Log log) {
        this.log = log;
        return this;
    }

    /**
     * Log progress every {@code interval} time units.
     * At most 1 message will be logged within this interval, but it is not
     * guaranteed that a message will be logged at all.
     *
     * @see #withDefaultLogInterval()
     */
    public GraphLoader withLogInterval(long value, TimeUnit unit) {
        this.logMillis = unit.toMillis(value);
        return this;
    }

    /**
     * Log progress in the default interval specified by {@link ProgressLoggerAdapter}.
     *
     * @see #withLogInterval(long, TimeUnit)
     */
    public GraphLoader withDefaultLogInterval() {
        this.logMillis = -1;
        return this;
    }

    public GraphLoader withSort(boolean sort) {
        this.sort = sort;
        return this;
    }

    public GraphLoader asUndirected(boolean loadAsUndirected) {
        this.loadAsUndirected = loadAsUndirected;
        return this;
    }

    /**
     * Use the given {@link AllocationTracker} to track memory allocations during loading.
     * Can be null, in which case no tracking happens. The same effect can be
     * achieved by using {@link AllocationTracker#EMPTY}.
     */
    public GraphLoader withAllocationTracker(AllocationTracker tracker) {
        this.tracker = tracker;
        return this;
    }

    /**
     * set an executor service
     *
     * @param executorService the executor service
     * @return itself to enable fluent interface
     */
    public GraphLoader withExecutorService(ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService);
        return this;
    }

    /**
     * disable use of executor service
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutExecutorService() {
        this.executorService = null;
        return this;
    }

    /**
     * change the concurrency level. Negative and zero values are not supported.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withConcurrency(int newConcurrency) {
        if (newConcurrency <= 0) {
            throw new IllegalArgumentException("concurrency: " + newConcurrency);
        }
        this.concurrency = Pools.allowedConcurrency(newConcurrency);
        return this;
    }

    /**
     * change the concurrency level to the default concurrency, which is based
     * on the numbers of detected processors.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultConcurrency() {
        this.concurrency = Pools.DEFAULT_CONCURRENCY;
        return this;
    }

    public GraphLoader withName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withLabel(String label) {
        this.label = Objects.requireNonNull(label);
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given label name.
     *
     * @param label May be null
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalLabel(String label) {
        this.label = label;
        return this;
    }

    /**
     * Instructs the loader to load only nodes with the given {@link Label}.
     *
     * @param label May not be null; to remove a label filter, use {@link #withAnyLabel()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withLabel(Label label) {
        this.label = Objects.requireNonNull(label).name();
        return this;
    }

    /**
     * Instructs the loader to load any node with no restriction to any label.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withAnyLabel() {
        this.label = null;
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     *
     * @param relation May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipType(String relation) {
        this.relation = Objects.requireNonNull(relation);
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given type name.
     * If the argument is null, every relationship will be considered.
     *
     * @param relation May be null
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalRelationshipType(String relation) {
        this.relation = relation;
        return this;
    }

    /**
     * Instructs the loader to load only relationships with the given {@link RelationshipType}.
     *
     * @param relation May not be null; to remove a type filter, use {@link #withAnyRelationshipType()} instead.
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipType(RelationshipType relation) {
        this.relation = Objects.requireNonNull(relation).name();
        return this;
    }

    /**
     * Instructs the loader to load any relationship with no restriction to any type.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withAnyRelationshipType() {
        this.relation = null;
        return this;
    }

    /**
     * Instructs the loader to load only relationship of the given direction.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    /**
     * Instructs the loader to load relationship weights by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property             May not be null; to remove a weight property, use {@link #withoutRelationshipWeights()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipWeightsFromProperty(String property, double propertyDefaultValue) {
        this.relWeightProp = Objects.requireNonNull(property);
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load relationship weights by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property             May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalRelationshipWeightsFromProperty(String property, double propertyDefaultValue) {
        this.relWeightProp = property;
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node weights by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property             May not be null; to remove a weight property, use {@link #withoutNodeWeights()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withNodeWeightsFromProperty(String property, double propertyDefaultValue) {
        this.nodeWeightProp = Objects.requireNonNull(property);
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node weights by reading the given property.
     * If the property is not set at the relationship, the propertyDefaultValue is used instead.
     *
     * @param property             May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalNodeWeightsFromProperty(String property, double propertyDefaultValue) {
        this.nodeWeightProp = property;
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node values by reading the given property.
     * If the property is not set, the propertyDefaultValue is used instead.
     *
     * @param property             May not be null; to remove a node property, use {@link #withoutNodeProperties()} instead.
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withNodeProperty(String property, double propertyDefaultValue) {
        this.nodeProp = Objects.requireNonNull(property);
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to load node values by reading the given property.
     * If the property is not set at the node, the propertyDefaultValue is used instead.
     *
     * @param property             May be null
     * @param propertyDefaultValue the default value to use if property is not set
     * @return itself to enable fluent interface
     */
    public GraphLoader withOptionalNodeProperty(String property, double propertyDefaultValue) {
        this.nodeProp = property;
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any relationship weights. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultRelationshipWeight(double propertyDefaultValue) {
        this.relWeightProp = null;
        this.relWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any node weights. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultNodeWeight(double propertyDefaultValue) {
        this.nodeWeightProp = null;
        this.nodeWeightDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any node properties. Instead each weight is set
     * to propertyDefaultValue.
     *
     * @param propertyDefaultValue the default value.
     * @return itself to enable fluent interface
     */
    public GraphLoader withDefaultNodeProperties(double propertyDefaultValue) {
        this.nodeProp = null;
        this.nodePropDefault = propertyDefaultValue;
        return this;
    }

    /**
     * Instructs the loader to not load any weights. The behavior of using weighted graph-functions
     * on a graph without weights is not specified.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutRelationshipWeights() {
        this.relWeightProp = null;
        this.relWeightDefault = 0.0;
        return this;
    }

    /**
     * Instructs the loader to not load any node weights.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutNodeWeights() {
        this.nodeWeightProp = null;
        this.nodeWeightDefault = 0.0;
        return this;
    }

    /**
     * Instructs the loader to not load any node properties.
     *
     * @return itself to enable fluent interface
     */
    public GraphLoader withoutNodeProperties() {
        this.nodeProp = null;
        this.nodePropDefault = 0.0;
        return this;
    }

    public GraphLoader withParams(Map<String, Object> params) {
        this.params.putAll(params);
        return this;
    }

    /**
     * Returns an instance of the factory that can be used to load the graph.
     */
    public final <T extends GraphFactory> T build(final Class<T> factoryType) {
        final MethodHandle constructor = findConstructor(factoryType);
        return factoryType.cast(invokeConstructor(constructor));
    }

    /**
     * Loads the graph using the provided GraphFactory, passing the built
     * configuration as parameters.
     * <p>
     * The chosen implementation determines the performance characteristics
     * during load and usage of the Graph.
     *
     * @return the freshly loaded graph
     */
    public Graph load(Class<? extends GraphFactory> factoryType) {
        return build(factoryType).build();
    }

    private MethodHandle findConstructor(Class<?> factoryType) {
        try {
            return LOOKUP.findConstructor(factoryType, CTOR_METHOD);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphFactory invokeConstructor(MethodHandle constructor) {

        final GraphSetup setup = toSetup();

        try {
            return (GraphFactory) constructor.invoke(api, setup);
        } catch (Throwable throwable) {
            throw Exceptions.launderedException(
                    throwable.getMessage(),
                    throwable);
        }
    }

    public GraphSetup toSetup() {
        return new GraphSetup(
                    label,
                    null,
                    relation,
                    direction,
                    relWeightProp,
                    relWeightDefault,
                    nodeWeightProp,
                    nodeWeightDefault,
                    nodeProp,
                    nodePropDefault,
                    params,
                    executorService,
                    concurrency,
                    batchSize,
                    duplicateRelationshipsStrategy,
                    log,
                    logMillis,
                    sort,
                    loadAsUndirected,
                    tracker,
                    name,
                    nodePropertyMappings);
    }

    /**
     * provide statement to load nodes, has to return "id" and optionally "weight" or "value"
     *
     * @param nodeStatement
     * @return itself to enable fluent interface
     */
    public GraphLoader withNodeStatement(String nodeStatement) {
        this.label = nodeStatement;
        return this;
    }

    /**
     * provide statement to load unique relationships, has to return ids of start "source" and end-node "target" and optionally "weight"
     *
     * @param relationshipStatement
     * @return itself to enable fluent interface
     */
    public GraphLoader withRelationshipStatement(String relationshipStatement) {
        this.relation = relationshipStatement;
        return this;
    }

    /**
     * provide batch size for parallel loading
     *
     * @param batchSize
     * @return itself to enable fluent interface
     */
    public GraphLoader withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public GraphLoader init(Log log, String label, String relationship, ProcedureConfiguration config) {
        return withLog(log)
                .withName(config.getGraphName(null))
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withConcurrency(config.getReadConcurrency())
                .withBatchSize(config.getBatchSize())
                .withDuplicateRelationshipsStrategy(config.getDuplicateRelationshipsStrategy())
                .withParams(config.getParams());
    }

    /**
     * @param duplicateRelationshipsStrategy strategy for handling duplicate relationships
     * @return itself to enable fluent interface
     */
    public GraphLoader withDuplicateRelationshipsStrategy(DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        this.duplicateRelationshipsStrategy = duplicateRelationshipsStrategy;
        return this;
    }

    public GraphLoader withOptionalNodeProperties(PropertyMapping... nodePropertyMappings) {
        this.nodePropertyMappings = nodePropertyMappings;
        return this;
    }

    public GraphLoader direction(Direction direction) {
        return direction == Direction.BOTH ? asUndirected(true).withDirection(direction) : withDirection(direction);
    }
}
