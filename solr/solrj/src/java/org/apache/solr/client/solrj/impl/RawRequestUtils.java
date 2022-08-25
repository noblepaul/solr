package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.cluster.api.ApiType;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.util.InputStreamResponseListener;
import org.eclipse.jetty.client.util.OutputStreamContentProvider;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

public class RawRequestUtils {

    public static  <T> T executeJettyClientRequest(CloudHttp2SolrClient http2Client,
                                                   HttpClient client,
                                                   RawCloudRequest<T> req) throws IOException{
        String node = req.node;
        if (node == null) {
            node =  http2Client.getClusterState().getLiveNodes().iterator().next();
        }
        Request r = null;
        if (SolrRequest.METHOD.GET == req.method) {
            if (req.payload != null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "GET can't send streams!");
            }
            r = createReq( client, req, node, HttpMethod.GET);
            return sendReq(req, r);
        } else if(SolrRequest.METHOD.POST == req.method) {
            r = createReq( client, req, node, HttpMethod.POST);
            addPayload(req, r);
            return sendReq(req, r);
        } else if(SolrRequest.METHOD.PUT == req.method) {
            r = createReq( client, req, node, HttpMethod.PUT);
            addPayload(req, r);
            return sendReq(req, r);
        } else if(SolrRequest.METHOD.DELETE == req.method) {
            if (req.payload != null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "DELETE can't send streams!");
            }
            r = createReq( client, req, node, HttpMethod.DELETE);
            return sendReq(req, r);
        }
        return null;
    }

    private static <T> void addPayload(RawCloudRequest<T> req, Request r) throws IOException {
        if (req.payload != null) {
            OutputStreamContentProvider content = new OutputStreamContentProvider();
            r.content(content);
            req.payload.accept(content.getOutputStream());
        }
    }

    private static <T> Request createReq(HttpClient client, RawCloudRequest<T> req, String node, HttpMethod get) {
        Request r = client
                .newRequest(getUrlWithParams(req, node))
                .method(get);
        if (req.headers != null) {
            for (int i = 0; i < req.headers.size(); i++) {
                r.getHeaders().add(req.headers.getName(i), req.headers.getVal(i));
            }
        }
        return r;
    }

    private static <T> T sendReq(RawCloudRequest<T> req, Request r) throws IOException {
        InputStreamResponseListener respListener = new InputStreamResponseListener();
        r.send(respListener);
        Response response = null;
        try {
            response = respListener.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(req.parser != null) {
            req.parser.status(response.getStatus());
            if(req.parser.listenHeaders()) {
                HttpFields flds = response.getHeaders();
                Enumeration<String> keys = flds.getFieldNames();
                while (keys.hasMoreElements()) {
                    String k = keys.nextElement();
                    if(!req.parser.header(k, flds.get(k))) break;
                }
            }
        }
        if (response.getStatus() == 200) {
            // Obtain the input stream on the response content
            try (InputStream input = respListener.getInputStream())
            {
                if(req.parser != null){
                    T t = req.parser.accept(input);
                    return t;
                }
                // Read the response content
            }
        }
        return null;
    }

    private static <T> String getUrlWithParams(RawCloudRequest<T> req, String node) {
        StringBuilder url = new StringBuilder(Utils.getBaseUrlForNodeName(node, "http", req.apiType == ApiType.V2));
        url.append(req.path);
        if (req.params != null && req.params.sb.length() > 0) {
            url.append("?").append(req.params.sb);
        }
        return url.toString();
    }
}
