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

import org.neo4j.configuration.Config;
import org.neo4j.gds.BaseProc;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphStoreExportSettings;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import static org.neo4j.procedure.Mode.READ;

public class RemoteMLProc extends BaseProc {

    private static final String DESCRIPTION = "Experimental endpoint for remote ml";

    @Internal
    @Procedure(name = "gds.upload.graph", mode = READ)
    @Description(DESCRIPTION)
    public void upload(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) throws IOException, InterruptedException {
        String[] parts = UUID.randomUUID().toString().split("-");
        var crName = parts[parts.length - 1];

        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        var remoteMLProcConfig = new RemoteMLProcConfigImpl(cypherConfig);

        Config config = GraphDatabaseApiProxy.resolveDependency(databaseService, Config.class);
        String bucketBasePath = config.get(GraphStoreExportSettings.storage_bucket_base_path);

        String outputPath = bucketBasePath + "/" + username() + "/models/" + remoteMLProcConfig.modelName();

        log.info("Creating yaml file");
        var yamlFile = writeCRDtoFile(outputPath, crName, remoteMLProcConfig);

        log.info("Applying CRD");
        callShellWithErrorLogging(String.format(
            "/plugins/kubectl apply -f %s",
            yamlFile.toFile().getAbsolutePath()
        ));

    }

    private static Path writeCRDtoFile(String outputPath, String name, RemoteMLProcConfig config) throws IOException {
        var yamlBuilder = new Yaml();
        var yaml = yamlBuilder.dump(
            Map.of(
                "apiVersion", "gds.neo4j.io/v1",
                "kind", "MachineLearning",
                "metadata", Map.of(
                    "name", name
                ),
                "spec", Map.of(
                    "token", config.token(),
                    "arrowEndpoint", config.arrowEndpoint(),
                    "trainConfig", config.mlTrainingConfig(),
                    "nodeLabel", config.nodeLabels(),
                    "outputPath", outputPath,
                    "k8sServiceAccountName", "beefbeef-database"
                )
            )
        );
        var yamlFile = Files.createTempFile("ml-crd-", ".yaml");
        BufferedWriter writer = new BufferedWriter(new FileWriter(yamlFile.toString(), StandardCharsets.UTF_8));
        writer.write(yaml);
        writer.close();

        return yamlFile;
    }

    private static void callShellWithErrorLogging(String cmd) throws IOException, InterruptedException {
        Process process = Runtime.getRuntime().exec(cmd);
        if (process.waitFor() != 0) {
            logShellError(cmd, process);
        }
    }

    private static void logShellError(String cmd, Process process) throws IOException {
        String line;
        StringBuilder stdErr = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
            process.getErrorStream(),
            StandardCharsets.UTF_8
        ));
        while ((line = bufferedReader.readLine()) != null) {
            stdErr.append(line);
        }
        bufferedReader.close();
        StringBuilder stdOut = new StringBuilder();
        bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        while ((line = bufferedReader.readLine()) != null) {
            stdOut.append(line);
        }
        bufferedReader.close();
        throw new RuntimeException(String.format(
            "Failed to run command '%s' with error code %d, stdout: '%s', stderr: '%s'",
            cmd,
            process.exitValue(),
            stdOut,
            stdErr
        ));
    }

    @SuppressWarnings("unused")
    public static class UploadResult {
    }
}
