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
package org.neo4j.gds;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.Aggregation.DEFAULT;
import static org.neo4j.gds.core.Aggregation.NONE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringJoining.join;

public final class TestCypherGraphLoader implements TestGraphLoader {

    private final GraphDatabaseService db;

    private final Set<String> nodeLabels;
    private final Set<String> relTypes;

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private PropertyMappings relProperties = PropertyMappings.of();

    private Optional<Aggregation> maybeAggregation = Optional.empty();
    private Optional<Log> maybeLog = Optional.empty();

    TestCypherGraphLoader(GraphDatabaseService db) {
        this.db = db;
        this.nodeLabels = new HashSet<>();
        this.relTypes = new HashSet<>();
    }

    public TestCypherGraphLoader withLabels(String... labels) {
        nodeLabels.addAll(Arrays.asList(labels));
        return this;
    }

    public TestCypherGraphLoader withRelationshipTypes(String... types) {
        relTypes.addAll(Arrays.asList(types));
        return this;
    }

    public TestCypherGraphLoader withNodeProperties(PropertyMappings nodeProperties) {
        this.nodeProperties = nodeProperties;
        return this;
    }

    public TestCypherGraphLoader withRelationshipProperties(PropertyMapping... relProperties) {
        return withRelationshipProperties(PropertyMappings.of(relProperties));
    }

    public TestCypherGraphLoader withRelationshipProperties(PropertyMappings relProperties) {
        return withRelationshipProperties(relProperties, true);
    }

    public TestCypherGraphLoader withRelationshipProperties(PropertyMappings relProperties, boolean addToLoader) {
        this.relProperties = relProperties;
        return this;
    }

    public TestCypherGraphLoader withDefaultAggregation(Aggregation aggregation) {
        this.maybeAggregation = Optional.of(aggregation);
        return this;
    }

    public TestCypherGraphLoader withLog(Log log) {
        this.maybeLog = Optional.of(log);
        return this;
    }

    public Graph graph() {
        return graphStore().getUnion();
    }

    public GraphStore graphStore() {
        try (Transaction ignored = db.beginTx()) {
            return cypherLoader().graphStore();
        }
    }

    private GraphLoader cypherLoader() {
        CypherLoaderBuilder cypherLoaderBuilder = new CypherLoaderBuilder().databaseService(db);

        String nodeQueryTemplate = "MATCH (n) %s RETURN id(n) AS id%s%s";

        String labelString = nodeLabels.isEmpty()
            ? ""
            : nodeLabels
                .stream()
                .map(l -> "n:" + l)
                .collect(Collectors.joining(" OR ", "WHERE ", ""));

        String nodePropertiesString = getNodePropertiesString(nodeProperties, "n");

        cypherLoaderBuilder.nodeQuery(formatWithLocale(nodeQueryTemplate,
            labelString,
            nodeLabels.isEmpty() ? "" : ", labels(n) AS labels",
            nodePropertiesString));

        String relationshipQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN ";
        if (!Arrays.asList(DEFAULT, NONE).contains(maybeAggregation.orElse(NONE))) {
            relationshipQueryTemplate += "DISTINCT ";
        }

        relationshipQueryTemplate += relTypes.isEmpty()
            ? " id(n) AS source, id(m) AS target%s"
            : " type(r) AS type, id(n) AS source, id(m) AS target%s";

        String relTypeString = relTypes.isEmpty()
            ? ""
            : join(relTypes, "|", ":", "");

        relProperties = getUniquePropertyMappings(relProperties);
        String relPropertiesString = getRelationshipPropertiesString(relProperties, "r");

        cypherLoaderBuilder.relationshipQuery(formatWithLocale(
            relationshipQueryTemplate,
            relTypeString,
            relPropertiesString
        ));

        cypherLoaderBuilder.validateRelationships(false);

        cypherLoaderBuilder.log(maybeLog);

        return cypherLoaderBuilder.build();
    }

    private PropertyMappings getUniquePropertyMappings(PropertyMappings propertyMappings) {
        MutableInt mutableInt = new MutableInt(0);
        return PropertyMappings.of(propertyMappings.stream()
            .map(mapping -> PropertyMapping.of(
                mapping.propertyKey(),
                addSuffix(mapping.neoPropertyKey(), mutableInt.getAndIncrement()),
                mapping.defaultValue(),
                mapping.aggregation() == DEFAULT ? maybeAggregation.orElse(NONE) : mapping.aggregation()
            ))
            .toArray(PropertyMapping[]::new)
        );
    }

    private String getNodePropertiesString(PropertyMappings propertyMappings, String entityVar) {
        return propertyMappings.hasMappings()
            ? propertyMappings
                .stream()
                .map(mapping -> formatWithLocale(
                    "COALESCE(%s.%s, %s) AS %s",
                    entityVar,
                    mapping.neoPropertyKey(),
                    mapping.defaultValue().getObject(),
                    mapping.propertyKey()
                ))
                .collect(Collectors.joining(", ", ", ", ""))
            : "";
    }

    private String getRelationshipPropertiesString(PropertyMappings propertyMappings, String entityVar) {
        return propertyMappings.hasMappings()
            ? propertyMappings
            .stream()
            .map(mapping -> formatWithLocale(
                "%s AS %s",
                TestSupport.getCypherAggregation(
                    mapping.aggregation().name(),
                    formatWithLocale(
                        "COALESCE(%s.%s, %f)",
                        entityVar,
                        removeSuffix(mapping.neoPropertyKey()),
                        mapping.defaultValue().getObject()
                    )
                ),
                mapping.propertyKey()
            ))
            .collect(Collectors.joining(", ", ", ", ""))
            : "";
    }

    private static final String SUFFIX = "___";

    private static String addSuffix(String propertyKey, int id) {
        return formatWithLocale("%s%s%d", propertyKey, SUFFIX, id);
    }

    private String removeSuffix(String propertyKey) {
        return propertyKey.substring(0, propertyKey.indexOf(SUFFIX));
    }
}
