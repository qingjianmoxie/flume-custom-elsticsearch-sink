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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hujinwen.flume.sink.elasticsearch.ElasticSearchSinkConstants;
import com.hujinwen.flume.sink.elasticsearch.IndexNameBuilder;
import com.hujinwen.utils.DateUtils;
import com.hujinwen.utils.JsonUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static com.hujinwen.flume.sink.elasticsearch.ElasticSearchSinkConstants.CONTENT_FLAG;
import static com.hujinwen.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_AMOUNT;

/**
 * Rest ElasticSearch client which is responsible for sending bulks of events to
 * ElasticSearch using ElasticSearch HTTP API. This is configurable, so any
 * config params required should be taken through this.
 */
public class ElasticSearchRestClient implements ElasticSearchClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    private final Set<IndexRequest> bulkCache = new HashSet<>();

    private static String contentFlag = "";

    private static Integer indexAmount = 3;

    private static String indexPrefix = "flume";

    private RestHighLevelClient client;

    /**
     * 每日接收总数统计
     */
    private long takeSize = 0;

    /**
     * 当前的index名称
     */
    private String currentIndexName = null;

    /**
     * 每日入库总数统计
     */
    private long saveSize = 0;


    public ElasticSearchRestClient(String[] hostNames) {
        final HttpHost[] hosts = new HttpHost[hostNames.length];
        for (int i = 0; i < hostNames.length; i++) {
            String hostName = hostNames[i];
            if (hostName.contains("http://") || hostName.contains("https://")) {
                hostName = hostName.substring(hostName.indexOf("//") + 2);
            }
            final String[] hostArg = hostName.split(":");
            hosts[i] = new HttpHost(hostArg[0], Integer.parseInt(hostArg[1]));
        }

        client = new RestHighLevelClient(RestClient.builder(hosts));
    }


    @Override
    public void configure(Context context) {
        contentFlag = context.getString(CONTENT_FLAG);
        indexAmount = context.getInteger(INDEX_AMOUNT, indexAmount);
        indexPrefix = context.getString(ElasticSearchSinkConstants.INDEX_NAME, indexPrefix);
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void addEvent(Event event, IndexNameBuilder indexNameBuilder, String indexType,
                         long ttlMs) throws Exception {
        // 统计
        takeSize++;

        final String content = new String(event.getBody(), StandardCharsets.UTF_8);
        // 这里的过滤暂时放到这里，日志内容必须包含配置的 contentFlag 字符串
        if (!content.contains(contentFlag)) {
            return;
        }

        final String indexName = indexNameBuilder.getIndexName(event);

        if (!indexName.equals(currentIndexName)) {
            if (currentIndexName != null) {
                takeSize = 0;
                saveSize = 0;
            }
            currentIndexName = indexName;
            synchronized (this) {
                indexClear();
            }
        }

        final IndexRequest indexRequest = new IndexRequest(indexName);

        final ObjectNode contentNode = JsonUtils.newObjectNode();
        contentNode.put("content", content);

        indexRequest.source(JsonUtils.toString(contentNode), XContentType.JSON);

        synchronized (bulkCache) {
            bulkCache.add(indexRequest);
        }
    }

    @Override
    public void execute(IndexNameBuilder indexNameBuilder) throws Exception {

        synchronized (bulkCache) {
            if (bulkCache.isEmpty()) {
                return;
            }

            final BulkRequest bulkRequest = new BulkRequest();

            IndexRequest[] requestArray = new IndexRequest[bulkCache.size()];
            bulkCache.toArray(requestArray);

            bulkRequest.add(requestArray);

            client.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    final int length = bulkItemResponses.getItems().length;
                    saveSize += length;
                    logger.info("Successfully! Inserted size -> {}, index -> {}, take size -> {}, save size -> {}", length, currentIndexName, takeSize, saveSize);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("+++++>> Failed!!!", e);
                    // 不成功可以抛出如下异常
//                    throw new EventDeliveryException(EntityUtils.toString(response.getEntity(), "UTF-8"));

                }
            });

            bulkCache.clear();
        }
    }

    /**
     * 根据配置删除多少天前的索引
     */
    private void indexClear() throws IOException {
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -indexAmount);
        final Date time = calendar.getTime();

        final GetIndexRequest getIndexRequest = new GetIndexRequest(indexPrefix + "*");
        final GetIndexResponse response = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);

        for (String indexName : response.getIndices()) {
            final Date date = DateUtils.extractDate(indexName);

            if (date == null) {
                continue;
            }
            if (time.after(date)) {
                final DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
                client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            }
        }
    }


}
