/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file contains proprietary code that is only available via a commercial license from Neo4j.
 * For more information, see https://neo4j.com/contact-us/
 */
package org.neo4j.internal.recordstorage;

import org.neo4j.storageengine.api.AllRelationshipsScan;

abstract class AbstractInMemoryAllRelationshipScan extends BaseRecordScan<AbstractInMemoryRelationshipScanCursor> implements AllRelationshipsScan {

}
