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
package org.neo4j.gds.graphsampling.samplers;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.beta.filter.NodesFilter;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.GraphStoreBuilder;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.values.storable.NumberType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.api.AdjacencyCursor.NOT_FOUND;

public class RandomWalkWithRestarts {
    private final RandomWalkWithRestartsConfig config;
    private final GraphStore inputGraphStore;

    public RandomWalkWithRestarts(RandomWalkWithRestartsConfig config, GraphStore inputGraphStore) {
        this.config = config;
        this.inputGraphStore = inputGraphStore;
    }

    public GraphStore sample() {
        var inputGraph = inputGraphStore.getGraph(
            config.nodeLabelIdentifiers(inputGraphStore),
            config.internalRelationshipTypes(inputGraphStore),
            Optional.empty()
        );
        var rng = new Random();
        config.randomSeed().ifPresent(rng::setSeed);

        IdMap sampledNodes = sampleNodes(inputGraph, rng);

        var nodePropertyStore = NodesFilter.filterNodeProperties(inputGraphStore, sampledNodes, config.concurrency(), ProgressTracker.NULL_TRACKER);

        Map<RelationshipType, Relationships.Topology> topologies = new HashMap<>();
        Map<RelationshipType, RelationshipPropertyStore> relPropertyStores = new HashMap<>();
        filterRelationshipsAndProperties(sampledNodes, topologies, relPropertyStores);

        var filteredSchema = filterSchema(sampledNodes, topologies);

        return new GraphStoreBuilder()
            .databaseId(inputGraphStore.databaseId())
            .capabilities(inputGraphStore.capabilities())
            .schema(filteredSchema)
            .nodes(sampledNodes)
            .nodePropertyStore(nodePropertyStore)
            .relationships(topologies)
            .relationshipPropertyStores(relPropertyStores)
            .concurrency(config.concurrency())
            .build();

    }

    private IdMap sampleNodes(Graph inputGraph, Random rng) {
        boolean hasLabelInformation = !inputGraphStore.nodeLabels().isEmpty();
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .concurrency(config.concurrency())
            .maxOriginalId(inputGraph.highestNeoId())
            .hasProperties(false)
            .hasLabelInformation(hasLabelInformation)
            .deduplicateIds(false)
            .build();

        long expectedNodes = Math.round(inputGraph.nodeCount() * config.samplingRatio());
        final long startNode = config.startNode().map(inputGraph::toMappedNodeId).orElse(0L);
        var currentNode = new MutableLong(startNode);
        // must keep track of this because nodesBuilder may not have flushed its buffer, so importedNodes cannot be used atm
        var seen = HugeAtomicBitSet.create(inputGraph.nodeCount());
        while (seen.cardinality() < expectedNodes) {
            if (!seen.get(currentNode.getValue())) {
                long originalId = inputGraph.toOriginalNodeId(currentNode.getValue());
                if (hasLabelInformation) {
                    var nodeLabelList = inputGraph.nodeLabels(currentNode.getValue());
                    var nodeLabels = new NodeLabel[nodeLabelList.size()];
                    nodeLabelList.toArray(nodeLabels);
                    nodesBuilder.addNode(originalId, nodeLabels);
                } else {
                    nodesBuilder.addNode(originalId);
                }
                seen.set(currentNode.getValue());
            }
            currentNode.setValue(walkStep(currentNode, startNode, inputGraph, rng));
        }
        var idMapAndProperties = nodesBuilder.build();
        return idMapAndProperties.idMap();
    }

    private void filterRelationshipsAndProperties(
        IdMap sampledNodes,
        Map<RelationshipType, Relationships.Topology> topologies,
        Map<RelationshipType, RelationshipPropertyStore> relPropertyStores
    ) {
        for (RelationshipType relType : config.internalRelationshipTypes(inputGraphStore)) {
            var relPropertyKeys = new ArrayList<>(inputGraphStore.relationshipPropertyKeys(relType));
            var relationships = filterRelationships(sampledNodes, relType, relPropertyKeys);
            var topology = relationships.get(0).topology();
            if (topology.elementCount() > 0) {
                topologies.put(relType, topology);
                relPropertyStores.put(relType, relationshipPropertyStore(relPropertyKeys, relationships));
            }
        }
    }

    private List<Relationships> filterRelationships(
        IdMap sampledNodes,
        RelationshipType relType,
        ArrayList<String> relPropertyKeys
    ) {
        IdMap inputNodes = inputGraphStore.nodes();
        var propertyConfigs = relPropertyKeys
            .stream()
            .map(key -> GraphFactory.PropertyConfig.of(
                Aggregation.NONE,
                inputGraphStore.relationshipPropertyValues(relType, key).defaultValue()
            ))
            .collect(Collectors.toList());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(sampledNodes)
            .concurrency(config.concurrency())
            .addAllPropertyConfigs(propertyConfigs)
            .build();

        var compositeIterator = inputGraphStore.getCompositeRelationshipIterator(relType, relPropertyKeys);

        for (long nodeId = 0; nodeId < sampledNodes.nodeCount(); nodeId++) {

            compositeIterator.forEachRelationship(
                inputNodes.toMappedNodeId(sampledNodes.toOriginalNodeId(nodeId)),
                (source, target, properties) -> {
                    var neoTarget = inputNodes.toOriginalNodeId(target);
                    var mappedTarget = sampledNodes.toMappedNodeId(neoTarget);
                    var neoSource = inputNodes.toOriginalNodeId(source);

                    if (mappedTarget != NOT_FOUND) {

                        if (properties.length == 0) {
                            relationshipsBuilder.add(neoSource, neoTarget);
                        } else if (properties.length == 1) {
                            relationshipsBuilder.add(neoSource, neoTarget, properties[0]);
                        } else {
                            relationshipsBuilder.add(neoSource, neoTarget, properties);
                        }
                    }
                    return true;
                }
            );
        }

        return relationshipsBuilder.buildAll();
    }


    private RelationshipPropertyStore relationshipPropertyStore(
        ArrayList<String> relPropertyKeys,
        List<Relationships> relationships
    ) {
        var properties = IntStream.range(0, relPropertyKeys.size())
            .boxed()
            .collect(Collectors.toMap(
                relPropertyKeys::get,
                idx -> relationships.get(idx).properties().orElseThrow(IllegalStateException::new)
            ));

        var propertyStoreBuilder = RelationshipPropertyStore.builder();
        properties.forEach((propertyKey, propertiesForKey) -> propertyStoreBuilder.putIfAbsent(
            propertyKey,
            RelationshipProperty.of(
                propertyKey,
                NumberType.FLOATING_POINT,
                PropertyState.PERSISTENT,
                propertiesForKey,
                DefaultValue.of(propertiesForKey.defaultPropertyValue()),
                Aggregation.NONE
            )
        ));

        return propertyStoreBuilder.build();
    }

    private GraphSchema filterSchema(IdMap sampledNodes, Map<RelationshipType, Relationships.Topology> topologies) {
        var nodeSchema = inputGraphStore.schema().nodeSchema().filter(sampledNodes.availableNodeLabels());
        var relationshipSchema = inputGraphStore.schema()
            .relationshipSchema()
            .filter(topologies.keySet());

        return GraphSchema.of(nodeSchema, relationshipSchema, inputGraphStore.schema().graphProperties());
    }

    private long walkStep(MutableLong currentNode, long startNode, Graph inputGraph, Random rng) {
        int degree = inputGraph.degree(currentNode.getValue());
        if (degree == 0 || rng.nextDouble() < config.restartProbability()) {
            return startNode;
        }
        int targetOffset = rng.nextInt(degree);
        return getTarget(inputGraph, currentNode.getValue(), targetOffset);
    }

    private long getTarget(RelationshipIterator inputGraph, long sourceNode, int targetOffset) {
        var targetsRemaining = new MutableInt(targetOffset);
        var target = new MutableLong();
        inputGraph.forEachRelationship(sourceNode, (src, trg) -> {
            if (targetsRemaining.getValue() == 0) {
                target.setValue(trg);
                return false;
            }
            targetsRemaining.decrement();
            return true;
        });
        return target.getValue();
    }
}
