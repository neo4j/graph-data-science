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
package org.neo4j.gds.core.model;

/*
 * ModelType (sometimes called algo type) describes the type of a model.
 * It is used to determine for example whether and how to serialize/deserialize a model.
 * We use a record and define a few instances to provide some usage clarity and type safety.
 * We do not use an ENUM because at the moment the API is rather porous and we don't want to
 * make it super strict all at once. Let anyone who has a String create their own model type--just
 * don't give me your String and tell me it is a model type.
 */
public record ModelType(String value) {
    public static final ModelType GraphSage =  new ModelType("graphSage");
    public static final ModelType NodeClassification =  new ModelType("NodeClassification");
    public static final ModelType LinkPrediction = new ModelType("LinkPrediction");
    public static final ModelType None = new ModelType("");
    /*
     * We cannot use the type quite everywhere. When serializing a model we write a String, and when deserializing
     * we therefore need to map back to ModelType from String.
     */
    public static ModelType lookUpFromString(String value) {
        if (value.equals("graphSage")) return GraphSage;
        if (value.equals("NodeClassification")) return NodeClassification;
        if (value.equals("LinkPrediction")) return LinkPrediction;
        throw new IllegalArgumentException("Unknown model type: " + value);
    }
}
