package com.demo.gmallrealpublisher.service.impl;

import com.demo.gmallrealpublisher.service.PublisherService;
import constant.GmallConstant;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublicsherServiceImpl implements PublisherService {

    //Autowired 会根据你的配置文件而自动组装生成es的实现，然后注入进来
    @Autowired
    JestClient jestClient;
    /**
     * 从es中查询当日日活总数
     * @param date
     * @return
     */
    @Override
    public Integer getDauTotal(String date) {
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \"2019-05-23\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        Search search = new Search.Builder(query).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DEFAULT).build();
        Integer total = 0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    /**
     * 分时日活统计
     * @param date
     * @return
     */
    @Override
    public Map getDauHour(String date) {

        Map<String, Long> logHourmap = new HashMap<>();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //按日期进行过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //按小时进行分组
        TermsBuilder termsAgg = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(termsAgg);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_TYPE_DEFAULT).build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> logHourBuckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : logHourBuckets){
                logHourmap.put(bucket.getKey(),bucket.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return logHourmap;
    }
}
