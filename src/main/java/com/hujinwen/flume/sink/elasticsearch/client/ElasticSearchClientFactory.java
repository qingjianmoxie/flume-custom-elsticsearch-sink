/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hujinwen.flume.sink.elasticsearch.client;


/**
 * Internal ElasticSearch client factory. Responsible for creating instance
 * of ElasticSearch clients.
 */
public class ElasticSearchClientFactory {
    public static final String TransportClient = "transport";
    public static final String RestClient = "rest";

    /**
     * @param clientType  String representation of client type
     * @param hostNames   Array of strings that represents hostnames with ports (hostname:port)
     * @return
     */
    public ElasticSearchClient getClient(String clientType, String[] hostNames) throws NoSuchClientTypeException {
        if (clientType.equalsIgnoreCase(TransportClient)) {
//            return new ElasticSearchTransportClient(hostNames, clusterName, serializer);
        } else if (clientType.equalsIgnoreCase(RestClient)) {
            return new ElasticSearchRestClient(hostNames);
        }
        throw new NoSuchClientTypeException();
    }

    /**
     * Used for tests only. Creates local elasticsearch instance client.
     *
     * @param clientType Name of client to use
     * @return Local elastic search instance client
     */
    public ElasticSearchClient getLocalClient(String clientType)
            throws NoSuchClientTypeException {
        if (clientType.equalsIgnoreCase(TransportClient)) {
//            return new ElasticSearchTransportClient(serializer);
        } else if (clientType.equalsIgnoreCase(RestClient)) {
        }
        throw new NoSuchClientTypeException();
    }
}
