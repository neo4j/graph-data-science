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

import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.config.NodeConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.internal.recordstorage.RecordStorageEngine;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.QueryRunner.runQuery;

public interface NodeConfigTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends NodeConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    default void testNodeValidation(String configKey, String... expectedMessageSubstrings) {
        runQuery(graphDb(), "CREATE (:A)");

        var graphProjectConfig = ImmutableGraphProjectFromStoreConfig.of(
            "",
            "loadedGraph",
            NodeProjections.all(),
            RelationshipProjections.all()
        );

        long nodeId = TestSupport.fullAccessTransaction(graphDb()).apply((tx, ktx) -> {
            var nodeStore = GraphDatabaseApiProxy
                .resolveDependency(graphDb(), RecordStorageEngine.class)
                .testAccessNeoStores()
                .getNodeStore();
            return Neo4jProxy.getHighestPossibleIdInUse(nodeStore, ktx);
        });

        GraphStoreCatalog.set(graphProjectConfig, graphLoader(graphProjectConfig).graphStore());

        var config = createMinimalConfig(CypherMapWrapper.empty())
            .withNumber(configKey, nodeId + 42L)
            .toMap();

        applyOnProcedure(proc -> {
            assertThatThrownBy(() -> proc.compute("loadedGraph", config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContainingAll(expectedMessageSubstrings);
        });
    }
}
