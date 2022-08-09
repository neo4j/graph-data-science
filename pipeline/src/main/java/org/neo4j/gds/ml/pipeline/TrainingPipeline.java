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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.core.utils.TimeUtil;
import org.neo4j.gds.ml.models.TrainerConfig;
import org.neo4j.gds.ml.models.TrainingMethod;
import org.neo4j.gds.ml.models.automl.TunableTrainerConfig;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.config.MutatePropertyConfig.MUTATE_PROPERTY_KEY;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class TrainingPipeline<FEATURE_STEP extends FeatureStep> implements Pipeline<FEATURE_STEP> {

    protected final List<ExecutableNodePropertyStep> nodePropertySteps;
    protected final List<FEATURE_STEP> featureSteps;
    private final ZonedDateTime creationTime;

    protected Map<TrainingMethod, List<TunableTrainerConfig>> trainingParameterSpace;
    protected AutoTuningConfig autoTuningConfig;

    public static Map<String, List<Map<String, Object>>> toMapParameterSpace(Map<TrainingMethod, List<TunableTrainerConfig>> parameterSpace) {
        return parameterSpace.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> entry.getKey().toString(),
                entry -> entry.getValue().stream().map(TunableTrainerConfig::toMap).collect(Collectors.toList())
            ));
    }

    protected TrainingPipeline(TrainingType trainingType) {
        this.nodePropertySteps = new ArrayList<>();
        this.featureSteps = new ArrayList<>();
        this.creationTime = TimeUtil.now();

        this.trainingParameterSpace = new EnumMap<>(TrainingMethod.class);
        this.autoTuningConfig = AutoTuningConfig.DEFAULT_CONFIG;

        trainingType.supportedMethods().forEach(method -> trainingParameterSpace.put(method, new ArrayList<>()));
    }

    @Override
    public Map<String, Object> toMap() {
        // The pipeline's type and creation is not part of the map.
        Map<String, Object> map = new HashMap<>();
        map.put("featurePipeline", featurePipelineDescription());
        map.put(
            "trainingParameterSpace",
            toMapParameterSpace(trainingParameterSpace)
        );
        map.put("autoTuningConfig", autoTuningConfig().toMap());
        map.putAll(additionalEntries());
        return map;
    }

    public abstract String type();

    protected abstract Map<String, List<Map<String, Object>>> featurePipelineDescription();

    protected abstract Map<String, Object> additionalEntries();

    private int numberOfTrainerConfigs() {
        return this.trainingParameterSpace()
            .values()
            .stream()
            .mapToInt(List::size)
            .sum();
    }

    public void addNodePropertyStep(NodePropertyStep step) {
        validateUniqueMutateProperty(step);
        this.nodePropertySteps.add(step);
    }

    public void addFeatureStep(FEATURE_STEP featureStep) {
        this.featureSteps.add(featureStep);
    }

    @Override
    public List<ExecutableNodePropertyStep> nodePropertySteps() {
        return this.nodePropertySteps;
    }

    @Override
    public List<FEATURE_STEP> featureSteps() {
        return this.featureSteps;
    }

    public Map<TrainingMethod, List<TunableTrainerConfig>> trainingParameterSpace() {
        return trainingParameterSpace;
    }

    private int concreteTrainerConfigsCount() {
        return (int) trainingParameterSpace()
            .values()
            .stream()
            .flatMap(List::stream)
            .filter(TunableTrainerConfig::isConcrete)
            .count();
    }

    public int numberOfModelSelectionTrials() {
        int concreteTrainerConfigsCount = concreteTrainerConfigsCount();

        return concreteTrainerConfigsCount == numberOfTrainerConfigs()
            ? numberOfTrainerConfigs()
            : autoTuningConfig().maxTrials() + concreteTrainerConfigsCount;
    }

    public void addTrainerConfig(TunableTrainerConfig trainingConfig) {
        this.trainingParameterSpace.get(trainingConfig.trainingMethod()).add(trainingConfig);
    }

    public void addTrainerConfig(TrainerConfig trainingConfig) {
        this.trainingParameterSpace.get(trainingConfig.method()).add(trainingConfig.toTunableConfig());
    }

    public AutoTuningConfig autoTuningConfig() {
        return autoTuningConfig;
    }

    public void setAutoTuningConfig(AutoTuningConfig autoTuningConfig) {
        this.autoTuningConfig = autoTuningConfig;
    }

    private void validateUniqueMutateProperty(NodePropertyStep step) {
        this.nodePropertySteps.forEach(nodePropertyStep -> {
            var newMutatePropertyName = step.config().get(MUTATE_PROPERTY_KEY);
            var existingMutatePropertyName = nodePropertyStep.config().get(MUTATE_PROPERTY_KEY);
            if (newMutatePropertyName.equals(existingMutatePropertyName)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "The value of `%s` is expected to be unique, but %s was already specified in the %s procedure.",
                    MUTATE_PROPERTY_KEY,
                    newMutatePropertyName,
                    nodePropertyStep.procName()
                ));
            }
        });
    }

    public ZonedDateTime creationTime() {
        return creationTime;
    }

    protected enum TrainingType {
        CLASSIFICATION {
            @Override
            List<TrainingMethod> supportedMethods() {
                return List.of(TrainingMethod.LogisticRegression, TrainingMethod.RandomForestClassification);
            }
        },
        REGRESSION {
            @Override
            List<TrainingMethod> supportedMethods() {
                return List.of(TrainingMethod.LinearRegression, TrainingMethod.RandomForestRegression);
            }
        };

        abstract List<TrainingMethod> supportedMethods();
    }
}
