/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import org.apache.bookkeeper.mledger.proto.MLDataFormats;

public abstract class LightProtoHelper {

    public static MLDataFormats.ManagedLedgerInfo.LedgerInfo createLedgerInfo() {
        MLDataFormats.ManagedLedgerInfo.LedgerInfo li =  new MLDataFormats.ManagedLedgerInfo.LedgerInfo();

        // light proto doesn't return MLDataFormats.OffloadContext.getDefaultInstance()
        // if it wasn't explicitly set and a lot of code expects that behavior
        li.setOffloadContext()
                .setBookkeeperDeleted(false)
                .setComplete(false)
                .setTimestamp(-1L); // like protobuf default instance
        li.setOffloadContext()
                .setDriverMetadata()
                .setName(""); // like protobuf default instance

// stuff like this seems to be a bad idea,
// unlike protobuf that just creates empty collection,
// these will add empty object to that collection.
// so this is something we can't mimic from protobuf
//   li.setOffloadContext().setDriverMetadata().addProperty();
//   li.setOffloadContext().addOffloadSegment();
        return li;
    }
}
