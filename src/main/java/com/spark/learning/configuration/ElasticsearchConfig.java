package com.spark.learning.configuration;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;

import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.security.KeyStore;

@Configuration
public class ElasticsearchConfig {

    @Bean
    public ElasticsearchTemplate elasticsearchTemplate() throws Exception {

        // 1. Load PKCS12 truststore
        KeyStore truststore = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("/Users/rahulsoni/elasticsearch-9.1.3/elastic-ca/ca/elastic-truststore.p12")) {
            truststore.load(fis, "mypassword".toCharArray());
        }

        // 2. Build SSLContext with truststore
        SSLContext sslContext = SSLContexts.custom()
                .loadTrustMaterial(truststore, null)
                .build();

        // 3. Setup basic auth
        BasicCredentialsProvider creds = new BasicCredentialsProvider();
        creds.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials("elastic", "PEk9XgEl8Pwp-hijYzMU"));

        // 4. Build RestClient
        RestClientBuilder builder = RestClient.builder(
                        new HttpHost("localhost", 9200, "https"))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(creds)
                        .setSSLHostnameVerifier((hostname, session) -> true)); // 👈 disables hostname verification for local dev

        RestClient restClient = builder.build();

        // 5. Build transport & ElasticsearchTemplate
        // Build transport
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

// Wrap transport into ElasticsearchClient
        ElasticsearchClient client = new ElasticsearchClient(transport);

// Pass client to ElasticsearchTemplate
        return new ElasticsearchTemplate(client);

    }


    /*
     * NOT NEEDED.
     * Refer to ConfigDetails.md file for further details ....
     **/
    /*
    @Bean
    public RestClient elasticsearchRestClient() {
        return RestClient.builder(HttpHost.create("localhost:9200"))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    httpClientBuilder.addInterceptorLast((HttpResponseInterceptor) (response, context) ->
                            response.addHeader("X-Elastic-Product", "Elasticsearch"));
                    return httpClientBuilder;
                })
                .build();
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(RestClient restClient) {
        return ElasticsearchClients.createImperative(restClient);
    }
    */

}


