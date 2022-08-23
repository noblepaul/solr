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

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cluster.api.ApiType;
import org.apache.solr.cluster.api.RawRequest;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.cloud.Replica;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RawCloudRequest<T> implements RawRequest<T> {

  ApiType apiType;
  String node;
  String collection;
  Locator locator;
  String path;
  Function<Response, T> parser;
  Payload<T> payload;
  ParamsImpl params;

  SolrRequest.METHOD method;
  final Supplier<T> fun;

  RawCloudRequest(Supplier<T> fun) {
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
  public RawRequest<T> withCollection(String collection) {
    this.collection = collection;
    return this;
  }

  @Override
  public RawRequest<T> withReplica(Consumer<ReplicaLocator> replicaLocator) {
    locator = new Locator();
    replicaLocator.accept(locator);
    return this;
  }

  @Override
  public RawRequest<T> withHeader(String name, String val) {
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
    return fun.get();
  }

  @Override
  public T POST() {
    this.method = SolrRequest.METHOD.POST;
    return fun.get();
  }

  @Override
  public T DELETE() {
    this.method = SolrRequest.METHOD.DELETE;
    return fun.get();
  }

  @Override
  public T PUT() {
    this.method = SolrRequest.METHOD.PUT;
    return fun.get();
  }

  @Override
  public RawRequest<T> withParser(Function<Response, T> parser) {
    this.parser = parser;
    return this;
  }

  @Override
  public RawRequest<T> withPayload(Payload<T> os) {
    return null;
  }

  static class ParamsImpl implements Params {
    StringBuilder sb = new StringBuilder();
    @Override
    public Params add(String key, String... vals) {
      if(vals != null ) {
        key = URLEncoder.encode(key, UTF_8);
        for (String val : vals) {
          if(val == null) continue;
          sb.append(key)
                  .append('&')
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

    @Override
    public Params add(MapWriter mw) {
      try {
        mw.writeMap(new MapWriter.EntryWriter() {
          @Override
          public MapWriter.EntryWriter put(CharSequence k, Object v) {
          if(k == null) return this;
          if (v instanceof CharSequence) {
              add(k.toString(), v.toString());
            } else if(v instanceof Iterable) {
             Iterable<?> i = (Iterable<?>) v;
              for (Object o : i) {
                if (o != null) {
                  add(k.toString(), (o).toString());
                }
              }
            } else {
              throw new IllegalArgumentException("unknown type : "+v);
            }

          return this;
          }
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }
  }
  static class Locator implements ReplicaLocator {
    String replica;
    String shardKey;
    String shardName;
    boolean onlyLeader = false;
    boolean onlyFollower = false;
    Replica.Type replicaType;

    @Override
    public ReplicaLocator replicaName(String replicaName) {
      this.replica = replicaName;
      return this;
    }

    @Override
    public ReplicaLocator shardKey(String key) {
      this.shardKey = key;
      return  this;
    }

    @Override
    public ReplicaLocator shardName(String shardName) {
      this.shardName = shardName;
      return this;
    }

    @Override
    public ReplicaLocator onlyLeader() {
      this.onlyLeader = true;
      return this;
    }

    @Override
    public ReplicaLocator onlyFollower() {
      this.onlyFollower = true;
      return this;
    }

    @Override
    public ReplicaLocator replicaType(Replica.Type type) {
      this.replicaType = type;
      return this;
    }
  }
}
