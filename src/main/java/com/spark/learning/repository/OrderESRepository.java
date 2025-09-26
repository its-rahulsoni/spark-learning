package com.spark.learning.repository;

import com.spark.learning.models.es.OrderESDoc;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderESRepository extends ElasticsearchRepository<OrderESDoc, Integer> {
    // you can add custom query methods if needed
}

