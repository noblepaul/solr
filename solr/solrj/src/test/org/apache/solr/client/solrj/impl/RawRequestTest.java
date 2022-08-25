/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.api.RawRequest;
import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.util.Utils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.solr.cluster.api.RawRequest.JAVABIN_PARSER;
import static org.apache.solr.cluster.api.RawRequest.JSON_PARSER;

public class RawRequestTest extends SolrCloudTestCase {

    public void testAllMethodsHttp2() throws Exception {
        System.setProperty("metricsEnabled", "true");
        MiniSolrCloudCluster c = configureCluster(1)
                .addConfig(
                        "conf",
                        getFile("solrj")
                                .toPath()
                                .resolve("solr")
                                .resolve("configsets")
                                .resolve("streaming")
                                .resolve("conf"))
                .configure();

        try {
            CollectionAdminRequest.createCollection("coll1", "conf", 1, 1)
                    .setPerReplicaState(Boolean.TRUE)
                    .process(cluster.getSolrClient());
            cluster.waitForActiveCollection("coll1", 1, 1);
            JettySolrRunner j = c.getJettySolrRunner(0);
            try (CloudSolrClient client = new CloudHttp2SolrClient.Builder(List.of(j.getBaseUrl().toString())).build()) {
                verifyGet(j, client, JSON_PARSER);
                verifyGet(j, client, JAVABIN_PARSER);
            }
        } finally {
            c.shutdown();
        }
    }

    private void verifyGet(JettySolrRunner j, CloudSolrClient client, RawRequest.Parser<NavigableObject> parser ) {
        NavigableObject res = client.<NavigableObject>createRawRequest()
                .withNode(j.getNodeName())
                .withPath("/node/properties")
                .withParser(parser)
                .GET();
        assertEquals("0", res._getStr("responseHeader/status", null));
        assertEquals("true", res._getStr("system.properties/metricsEnabled", null));
    }
}
