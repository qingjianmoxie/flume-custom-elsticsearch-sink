package com.hujinwen.flume.sink.elasticsearch.client;

import com.hujinwen.utils.DateUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;


public class ElasticSearchRestClientTest {

    @Test
    public void test() {
        final Set<String> set = new HashSet<String>() {{
            add("a");
            add("b");
            add("c");
        }};
        String[] strings = new String[set.size()];
        set.toArray(strings);
        System.out.println();
    }

    @Test
    public void test2() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.0.252.174", 9200)));

        final long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            try {
                final GetIndexRequest getIndexRequest = new GetIndexRequest("test1");
                final boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
                if (exists) {
                    final DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest();
                    deleteIndexRequest.indices("test1");
                    final AcknowledgedResponse resp = client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                }
            } catch (Exception _) {
                System.out.println();
            }
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    @Test
    public void test3() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("10.0.100.61", 9200)));
        final GetIndexRequest getIndexRequest = new GetIndexRequest("flume-*");
        final GetIndexResponse response = client.indices().get(getIndexRequest, RequestOptions.DEFAULT);
        System.out.println();
    }

    @Test
    public void test4() {
        final Date date = DateUtils.extractDate("flume-2020-04-01");
        final Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -3);
        final Date time = calendar.getTime();
        final boolean before = date.before(time);
        System.out.println();
    }

}