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
package org.neo4j.gds.applications.graphstorecatalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.processing.Generated;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.BaseConfig;
import org.neo4j.gds.core.CypherMapAccess;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.progress.JobId;

@Generated("org.neo4j.gds.proc.ConfigurationProcessor")
public final class TestConfigImpl implements MemoryUsageValidatorTest.TestConfig {
    private List<String> relationshipTypes;

    private List<String> nodeLabels;

    private Optional<String> usernameOverride;

    private boolean sudo;

    private boolean logProgress;

    private int concurrency;

    private JobId jobId;

    public TestConfigImpl(@NotNull CypherMapAccess config) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.relationshipTypes = CypherMapAccess.failOnNull("relationshipTypes", config.getChecked("relationshipTypes", MemoryUsageValidatorTest.TestConfig.super.relationshipTypes(), List.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.nodeLabels = CypherMapAccess.failOnNull("nodeLabels", config.getChecked("nodeLabels", MemoryUsageValidatorTest.TestConfig.super.nodeLabels(), List.class));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.usernameOverride = CypherMapAccess.failOnNull("username", config.getOptional("username", String.class).map(BaseConfig::trim));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.sudo = config.getBool("sudo", MemoryUsageValidatorTest.TestConfig.super.sudo());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.logProgress = config.getBool("logProgress", MemoryUsageValidatorTest.TestConfig.super.logProgress());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.concurrency = config.getInt("concurrency", MemoryUsageValidatorTest.TestConfig.super.typedConcurrency().value());
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.jobId = CypherMapAccess.failOnNull("jobId", JobId.parse(config.getChecked("jobId", MemoryUsageValidatorTest.TestConfig.super.jobId(), Object.class)));
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            validateConcurrency();
        } catch (IllegalArgumentException e) {
            errors.add(e);
        } catch (NullPointerException e) {
        }
        if(!errors.isEmpty()) {
            if(errors.size() == 1) {
                throw errors.get(0);
            } else {
                String combinedErrorMsg = errors.stream().map(IllegalArgumentException::getMessage).collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t", "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t", ""));
                IllegalArgumentException combinedError = new IllegalArgumentException(combinedErrorMsg);
                errors.forEach(error -> combinedError.addSuppressed(error));
                throw combinedError;
            }
        }
    }

    @Override
    public List<String> relationshipTypes() {
        return this.relationshipTypes;
    }

    @Override
    public List<String> nodeLabels() {
        return this.nodeLabels;
    }

    @Override
    public void graphStoreValidation(GraphStore graphStore, Collection<NodeLabel> selectedLabels,
            Collection<RelationshipType> selectedRelationshipTypes) {
        ArrayList<IllegalArgumentException> errors_ = new ArrayList<>();
        try {
            validateNodeLabels(graphStore, selectedLabels, selectedRelationshipTypes);
        } catch (IllegalArgumentException e) {
            errors_.add(e);
        }
        try {
            validateRelationshipTypes(graphStore, selectedLabels, selectedRelationshipTypes);
        } catch (IllegalArgumentException e) {
            errors_.add(e);
        }
        if(!errors_.isEmpty()) {
            if(errors_.size() == 1) {
                throw errors_.get(0);
            } else {
                String combinedErrorMsg_ = errors_.stream().map(IllegalArgumentException::getMessage).collect(Collectors.joining(System.lineSeparator() + "\t\t\t\t", "Multiple errors in configuration arguments:" + System.lineSeparator() + "\t\t\t\t", ""));
                IllegalArgumentException combinedError_ = new IllegalArgumentException(combinedErrorMsg_);
                errors_.forEach(error_ -> combinedError_.addSuppressed(error_));
                throw combinedError_;
            }
        }
    }

    @Override
    public Optional<String> usernameOverride() {
        return this.usernameOverride;
    }

    @Override
    public boolean sudo() {
        return this.sudo;
    }

    @Override
    public boolean logProgress() {
        return this.logProgress;
    }

    @Override
    public Collection<String> configKeys() {
        return Arrays.asList("relationshipTypes", "nodeLabels", "username", "sudo", "logProgress", "concurrency", "jobId");
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("relationshipTypes", relationshipTypes());
        map.put("nodeLabels", nodeLabels());
        usernameOverride().ifPresent(username -> map.put("username", username));
        map.put("sudo", sudo());
        map.put("logProgress", logProgress());
        map.put("concurrency", typedConcurrency().value());
        map.put("jobId", org.neo4j.gds.core.utils.progress.JobId.asString(jobId()));
        return map;
    }

    @Override
    public int concurrency() {
        return this.concurrency;
    }

    @Override
    public JobId jobId() {
        return this.jobId;
    }

    public static TestConfigImpl.Builder builder() {
        return new TestConfigImpl.Builder();
    }

    public static final class Builder {
        private final Map<String, Object> config;

        public Builder() {
            this.config = new HashMap<>();
        }

        public static TestConfigImpl.Builder from(MemoryUsageValidatorTest.TestConfig baseConfig) {
            var builder = new TestConfigImpl.Builder();
            builder.relationshipTypes(baseConfig.relationshipTypes());
            builder.nodeLabels(baseConfig.nodeLabels());
            builder.usernameOverride(baseConfig.usernameOverride());
            builder.sudo(baseConfig.sudo());
            builder.logProgress(baseConfig.logProgress());
            builder.concurrency(baseConfig.typedConcurrency().value());
            builder.jobId(baseConfig.jobId());
            return builder;
        }

        public TestConfigImpl.Builder relationshipTypes(List<String> relationshipTypes) {
            this.config.put("relationshipTypes", relationshipTypes);
            return this;
        }

        public TestConfigImpl.Builder nodeLabels(List<String> nodeLabels) {
            this.config.put("nodeLabels", nodeLabels);
            return this;
        }

        public TestConfigImpl.Builder usernameOverride(String usernameOverride) {
            this.config.put("username", usernameOverride);
            return this;
        }

        public TestConfigImpl.Builder usernameOverride(Optional<String> usernameOverride) {
            usernameOverride.ifPresent(actualusernameOverride -> this.config.put("username", actualusernameOverride));
            return this;
        }

        public TestConfigImpl.Builder sudo(boolean sudo) {
            this.config.put("sudo", sudo);
            return this;
        }

        public TestConfigImpl.Builder logProgress(boolean logProgress) {
            this.config.put("logProgress", logProgress);
            return this;
        }

        public TestConfigImpl.Builder concurrency(int concurrency) {
            this.config.put("concurrency", concurrency);
            return this;
        }

        public TestConfigImpl.Builder jobId(Object jobId) {
            this.config.put("jobId", jobId);
            return this;
        }

        public MemoryUsageValidatorTest.TestConfig build() {
            CypherMapWrapper config = CypherMapWrapper.create(this.config);
            return new TestConfigImpl(config);
        }
    }
}
