/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package positive;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.ArrayList;

import javax.annotation.processing.Generated;

@Generated("org.neo4j.graphalgo.proc.ConfigurationProcessor")
public final class NamingConflictConfig implements NamingConflict {

    private int config;

    private int anotherConfig;

    private int config_;

    public NamingConflictConfig(int config_, @NotNull CypherMapWrapper config__) {
        ArrayList<IllegalArgumentException> errors = new ArrayList<>();
        try {
            this.config = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.anotherConfig = config__.requireInt("config");
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        try {
            this.config_ = config_;
        } catch (IllegalArgumentException e) {
            errors.add(e);
        }
        if (!errors.isEmpty()) {
            if(errors.size() == 1) {
                throw errors.get(0);
            } else {
                IllegalArgumentException combinedErrors = new IllegalArgumentException(
                    "Multiple errors in configuration arguments");
                errors.forEach(combinedErrors::addSuppressed);
                throw combinedErrors;
            }
        }
    }

    @Override
    public int config() {
        return this.config;
    }

    @Override
    public int anotherConfig() {
        return this.anotherConfig;
    }

    @Override
    public int config_() {
        return this.config_;
    }
}
