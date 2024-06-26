/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.rest.message.statement;

import org.apache.flink.runtime.rest.messages.MessagePathParameter;

/** {@link MessagePathParameter} that parses the token string. */
public class FetchResultsTokenPathParameter extends MessagePathParameter<Long> {

    public static final String KEY = "token";

    public FetchResultsTokenPathParameter() {
        super(KEY);
    }

    @Override
    protected Long convertFromString(String token) {
        return Long.valueOf(token);
    }

    @Override
    protected String convertToString(Long token) {
        return String.valueOf(token);
    }

    @Override
    public String getDescription() {
        return "The token that identifies which batch of data to fetch.";
    }
}
