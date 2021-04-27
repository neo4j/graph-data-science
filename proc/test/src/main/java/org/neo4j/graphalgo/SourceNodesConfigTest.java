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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.MapUtil;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.SourceNodesConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.kernel.impl.core.NodeEntity;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphalgo.QueryRunner.runQuery;

public interface SourceNodesConfigTest<ALGORITHM extends Algorithm<ALGORITHM, RESULT>, CONFIG extends SourceNodesConfig & AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void testFailOnMissingSourceNodes() {
        var graphName = "loadedGraph";
        runQuery(graphDb(), "CREATE (), (), ()");

        var sourceNodes = List.of(
            new NodeEntity(null, 42),
            new NodeEntity(null, 1337)
        );

        var graphCreateConfig = withNameAndNodeProjections(
            "",
            graphName,
            NodeProjections.ALL
        );

        var graphStore = graphLoader(graphCreateConfig).graphStore();
        GraphStoreCatalog.set(graphCreateConfig, graphStore);

        var mapWrapper = CypherMapWrapper.create(MapUtil.map("sourceNodes", sourceNodes));
        var config = createMinimalConfig(mapWrapper).toMap();

        applyOnProcedure(proc ->
            assertThatThrownBy(() -> proc.compute(graphName, config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Source nodes do not exist in the in-memory graph")
                .hasMessageContaining("['1337', '42']"));
    }
}
