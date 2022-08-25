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

package org.apache.solr.cluster.api;

import org.apache.solr.common.NavigableObject;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;

/**
 * A plain interface that captures all inputs for a Solr request. As the name suggests, it lets
 * users interact with Solr using raw bytes and params instead of concrete objects.
 *
 * <p>*
 *
 * <pre>{@code
 * public class C {
 *     //fields go here
 * }
 *  public class D {
 *       // class for payload
 *  }
 *
 * void serializeJackson(OutputStream os, Object o) {
 *    // code to serialize an object using jackson (or you may use your favorite library)
 *   }
 * <T> T deserializeJackson(InputStream in, Class<T> c) {
 *       //code to deserialize an object from jackson (or you may use your favorite library)
 *   }
 *  CloudolrClient client = null; // do initialization here
 *  C c = client.<C>createRequest()
 *                  .withNode("nodeName")
 *                  .withPath("/admin/metrics")
 *                  .withParser(in -> deserializeJackson(in.stream(), C.class))
 *                  .withParams(p -> p.
 *                          .add(CommonParams.OMIT_HEADER, CommonParams.TRUE)
 *                          .add("key", "solr.jvm:system.properties:java.version"))
 *                          .add("wt", "json"))
 *             .GET();
 *
 *
 *
 *
 * }</pre>
 *
 * @param <T> The concrete return type object
 */
public interface RawRequest<T> {

    /** Use /solr or /api end points default is /api (a.k.a V2 API*/
    RawRequest<T> withApiType(ApiType apiType);

    /**
     * Make a request to a specific Solr node
     *
     * @param node node name
     */
    RawRequest<T> withNode(String node);

    RawRequest<T> withHeader(String name, String val);

    /**
     * The path to which the request needs to be made eg:  /admin/metrics  etc.
     *
     * @param path The path
     */
    RawRequest<T> withPath(String path);

    /** The request parameters */
    RawRequest<T> withParams(Consumer<Params> params);

    /** If there is a payload, write it directly to the server */
    RawRequest<T> withPayload(Payload<T> os);

    /**
     * Parse and deserialize a concrete object . If this is not supplied, the response is just eaten
     * up and thrown away
     *
     * @param p This impl should consume an input stream and return an object
     */
    RawRequest<T> withParser(Parser<T> p);

    /**
     * do an HTTP GET operation
     *
     * @return the parsed object as returned by the parser. A null is returned if there is no parser
     *     set.
     */
    T GET();

    /**
     * do an HTTP POST operation
     *
     * @return the parsed object as returned by the parser. A null is returned if there is no parser
     *     set.
     */
    T POST();

    /**
     * do an HTTP DELETE operation
     *
     * @return the parsed object as returned by the parser. A null is returned if there is no parser
     *     set.
     */
    T DELETE();

    /**
     * do an HTTP PUT operation
     *
     * @return the parsed object as returned by the parser. A null is returned if there is no parser
     *     set.
     */
    T PUT();

    interface Params {
        Params add(String key, String... val);

        Params add(String key, Iterable<String> vals);

    }

    interface Payload<T> {
        default void init(RawRequest<T> r){}

        void accept(OutputStream os) throws IOException;
    }
    interface Parser<T> {
        default void init(RawRequest<T> r){}
        /**
         * HTTP status code
         */
        default void status(int status) {}
        default boolean listenHeaders() { return false;}

       default boolean header(String key, String val) {return false;}
        /**
         * Consume the stream sent by server
         */
        T accept(InputStream  is) throws IOException;
    }
    Parser<NavigableObject> JAVABIN_PARSER = new Parser<>() {
        @Override
        public NavigableObject accept(InputStream is) throws IOException {
            return (NavigableObject) Utils.JAVABINCONSUMER.accept(is);
        }

        @Override
        public void init(RawRequest<NavigableObject> r) {
            r.withParams(p -> p.add(CommonParams.WT, CommonParams.JAVABIN));
        }
    };
    Parser<NavigableObject> JSON_PARSER = new Parser<>() {
        @Override
        public void init(RawRequest<NavigableObject> r) {
            r.withParams(p -> p.add(CommonParams.WT, CommonParams.JSON));
        }

        @Override
        public NavigableObject accept(InputStream is) throws IOException {
            return (NavigableObject) Utils.fromJSON(is, Utils.MAPWRITEROBJBUILDER);
        }
    };
}
