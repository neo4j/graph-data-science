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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateConfig;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.TestSupport.fromGdl;

public interface MutateRelationshipsTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends MutateConfig & AlgoBaseConfig, RESULT>
    extends MutateProcTest<ALGORITHM, CONFIG, RESULT> {

    String mutateRelationshipType();

    @Override
    default CypherMapWrapper createMinimalConfig(CypherMapWrapper mapWrapper) {
        if (!mapWrapper.containsKey("mutateRelationshipType")) {
            mapWrapper = mapWrapper.withString("mutateRelationshipType", mutateRelationshipType());
        }
        return mapWrapper;
    }

    @Override
    @Test
    default void testGraphMutation() {
        var graphStore = runMutation();
        TestSupport.assertGraphEquals(fromGdl(expectedMutatedGraph()), graphStore.getUnion());
        assertTrue(graphStore.hasRelationshipType(RelationshipType.of(mutateRelationshipType())));
    }

    @Override
    default String failOnExistingTokenMessage() {
        return formatWithLocale(
            "Relationship type `%s` already exists in the in-memory graph.",
            mutateRelationshipType()
        );
    }
}
