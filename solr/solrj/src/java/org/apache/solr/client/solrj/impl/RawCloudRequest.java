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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cluster.api.ApiType;
import org.apache.solr.cluster.api.RawRequest;
import org.apache.solr.common.util.NamedList;

import java.net.URLEncoder;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RawCloudRequest<T> implements RawRequest<T> {

  ApiType apiType = ApiType.V2;
  String node;
  String path = "";
  ResponseListener<T> parser;
  Payload<T> payload;
  ParamsImpl params;

  SolrRequest.METHOD method = SolrRequest.METHOD.GET;
  private final Function<RawCloudRequest<T>, T> fun;

  NamedList<String> headers;

  RawCloudRequest(Function<RawCloudRequest<T>, T> fun) {
    this.fun = fun;
  }

  @Override
  public RawRequest<T> withApiType(ApiType apiType) {
    this.apiType = apiType;
    return this;
  }

  @Override
  public RawRequest<T> withNode(String node) {
    this.node = node;
    return this;
  }

  @Override
  public RawRequest<T> withHeader(String name, String val) {
    if(headers == null) headers = new NamedList<>();
    headers.add(name, val);
    return this;
  }

  @Override
  public RawRequest<T> withPath(String path) {
    this.path = path;
    return this;
  }

  @Override
  public RawRequest<T> withParams(Consumer<Params> p) {
    if(params == null) params = new ParamsImpl();
    p.accept(params);
    return this;
  }

  @Override
  public T GET() {
    this.method =  SolrRequest.METHOD.GET;
    return fun.apply(this);

  }

  @Override
  public T POST() {
    this.method = SolrRequest.METHOD.POST;
    return fun.apply(this);
  }

  @Override
  public T DELETE() {
    this.method = SolrRequest.METHOD.DELETE;
    return fun.apply(this);

  }

  @Override
  public T PUT() {
    this.method = SolrRequest.METHOD.PUT;
    return fun.apply(this);

  }

  @Override
  public RawRequest<T> withParser(ResponseListener<T> parser) {
    parser.init(this);
    this.parser = parser;
    return this;
  }

  @Override
  public RawRequest<T> withPayload(Payload<T> os) {
    os.init(this);
    this.payload = os;
    return this;
  }

  static class ParamsImpl implements Params {
    StringBuilder sb = new StringBuilder();
    @Override
    public Params add(String key, String... vals) {
      if(vals != null ) {
        key = URLEncoder.encode(key, UTF_8);
        for (String val : vals) {
          if(val == null) continue;
          if(sb.length() > 0) sb.append('&');
          sb.append(key)
                  .append('=')
                  .append(URLEncoder.encode(val, UTF_8));

        }
      }
      return this;
    }

    @Override
    public Params add(String key, Iterable<String> vals) {
      if(vals != null) {
        key = URLEncoder.encode(key, UTF_8);
        Iterator<String> it = vals.iterator();
        while (it.hasNext()) {
          String val = it.next();
          if(val == null) continue;
          sb.append(key)
                  .append('&')
                  .append(URLEncoder.encode(val, UTF_8));

        }

      }
      return this;
    }
  }
}
