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
package org.neo4j.gds.ml.remote;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class RemoteMLProcTest {

    @GdlGraph
    private static final String X =
        "CREATE" +
            "  (a:Node1 {f: [0.4, 1.3, 1.4]})" +
            ", (b:Node1 {f: [2.1, 0.5, 1.8]})" +
            ", (c:Node2 {f: [-0.3, 0.8, 2.8]})" +
            ", (d:Isolated {f: [2.5, 8.1, 1.3], hejJacob: 0.1})" +
            ", (e:Isolated {f: [0.6, 0.5, 5.2], hejJacob: 41.2})" +
            ", (a)-[:REL {weight: 2.0}]->(b)" +
            ", (b)-[:REL {weight: 1.0}]->(a)" +
            ", (a)-[:REL {weight: 1.0}]->(c)" +
            ", (c)-[:REL {weight: 1.0}]->(a)" +
            ", (b)-[:REL {weight: 1.0}]->(c)" +
            ", (c)-[:REL {weight: 1.0}]->(b)";

    @Inject
    private GraphStore graphStore;

    @Test
    void asdasd() throws JsonProcessingException {
        var config = RemoteMLProcConfigImpl.builder()
            .modelName("asdadasaaaa")
            .mlTrainingConfig("{\"name\":\"mkyong\", \"age\":\"37\"}")
            .nodeProperties(List.of(
                "a",
                "b"
            ))
            .build();
        Map<String, Object> mlTrainingConfigAsMap = new ObjectMapper().readValue(config.mlTrainingConfig(), Map.class);
        var mlConfigAsJsonString = new ObjectMapper().writeValueAsString(Map.of(
            "nodeProperties", config.nodeProperties(),
            "mlTrainingConfig", mlTrainingConfigAsMap
        ));
        System.out.println("mlConfigAsJsonString = " + mlConfigAsJsonString);
    }
}
