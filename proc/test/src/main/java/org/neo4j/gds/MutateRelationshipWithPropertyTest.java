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

import org.apache.commons.lang3.mutable.MutableLong;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.gds.core.loading.GraphStoreCatalog;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.QueryRunner.runQueryWithRowConsumer;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface MutateRelationshipWithPropertyTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends MutateConfig & AlgoBaseConfig, RESULT>
    extends MutatePropertyProcTest<ALGORITHM, CONFIG, RESULT> {

    String mutateRelationshipType();

    String mutateProperty();

    ValueType mutatePropertyType();

    @Test
    default void testWriteBackGraphMutationOnFilteredGraph() {
        emptyDb();
        GraphStoreCatalog.removeAllLoadedGraphs();

        if (mutateRelationshipType().equals("REL")) {
            throw new IllegalArgumentException("mutateRelationshipType must not be `REL`");
        }

        runQuery(graphDb(), "CREATE (:B), (a1: A), (a2: A), (a3: A), (a1)-[:REL]->(a3), (a2)-[:REL]->(a3)");
        String graphName = "myGraph";

        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder()
            .api(graphDb())
            .graphName(graphName)
            .addNodeLabels("A", "B");

        relationshipProjections().projections().forEach((relationshipType, projection)->
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(relationshipType.name(), projection));

        CypherMapWrapper filterConfig = CypherMapWrapper.empty().withEntry(
            "nodeLabels",
            Collections.singletonList("A")
        );
        Map<String, Object> config = createMinimalConfig(filterConfig).toMap();

        setupStoreLoader(storeLoaderBuilder, config);

        GraphLoader loader = storeLoaderBuilder.build();
        GraphStoreCatalog.set(loader.projectConfig(), loader.graphStore());

        applyOnProcedure(procedure ->
            getProcedureMethods(procedure)
                .filter(procedureMethod -> getProcedureMethodName(procedureMethod).endsWith(".mutate"))
                .forEach(mutateMethod -> {
                    try {
                        mutateMethod.invoke(procedure, graphName, config);
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        fail(e);
                    }
                })
        );

        String graphWriteQuery =
            "CALL gds.graph.writeRelationship(" +
            "   $graph, " +
            "   $relationshipType, " +
            "   $relationshipProperty " +
            ") YIELD writeMillis, graphName, relationshipsWritten, propertiesWritten";

        runQuery(graphDb(), graphWriteQuery, Map.of(
            "graph", graphName,
            "relationshipType", mutateRelationshipType(),
            "relationshipProperty", mutateProperty())
        );

        String checkNeo4jGraphNegativeQuery = formatWithLocale(
            "MATCH ()-[r:REL]->() RETURN r.%s AS property",
            mutateProperty()
        );

        runQueryWithRowConsumer(
            graphDb(),
            checkNeo4jGraphNegativeQuery,
            Map.of(),
            ((transaction, resultRow) -> assertThat(resultRow.get("property"))
                .as("negative check on existing relationship")
                .isNull())
        );

        String checkNeo4jGraphPositiveQuery = formatWithLocale(
            "MATCH (a1)-[r:%s]->(a2) RETURN labels(a1)[0] AS label1, labels(a2)[0] AS label2, r.%s AS property",
            mutateRelationshipType(),
            mutateProperty()
        );

        SoftAssertions.assertSoftly(softly -> {
            var numberOfRows = new MutableLong();
            runQueryWithRowConsumer(
                graphDb(),
                checkNeo4jGraphPositiveQuery,
                Map.of(),
                (transaction, resultRow) -> {
                    numberOfRows.increment();
                    softly.assertThat(resultRow.get("property")).as("positive check on created relationship").isNotNull();
                    softly.assertThat(resultRow.getString("label1")).as("label on created relationship").isEqualTo("A");
                    softly.assertThat(resultRow.getString("label2")).as("label on created relationship").isEqualTo("A");
                }
            );
            softly.assertThat(numberOfRows.longValue()).isGreaterThan(0L);
        });
    }

    @Override
    default String failOnExistingTokenMessage() {
        return formatWithLocale(
            "Relationship type `%s` already exists in the in-memory graph.",
            mutateRelationshipType()
        );
    }

    default void setupStoreLoader(StoreLoaderBuilder storeLoaderBuilder, Map<String, Object> config) {}
}
