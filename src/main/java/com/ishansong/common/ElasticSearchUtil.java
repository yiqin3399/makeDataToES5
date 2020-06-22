package com.ishansong.common;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author mr
 * @date 2019/07/16
 */
public class ElasticSearchUtil {

    public static TransportClient getTransportClient(String hostPorts) throws UnknownHostException {
        Settings settings = Settings.builder()
                .put("client.transport.ignore_cluster_name", true)
                .put("client.transport.sniff", true).build();

        PreBuiltTransportClient preBuiltTransportClient = new PreBuiltTransportClient(settings);

        String[] hostAndPorts = hostPorts.split(",", -1);
        for (String hostAndPort : hostAndPorts) {
            String[] split = hostAndPort.split(":", -1);
            preBuiltTransportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(split[0]), Integer.parseInt(split[1])));
        }

        return preBuiltTransportClient;
    }

    public static void close(TransportClient transportClient) {
        if (null != transportClient) {
            transportClient.close();
        }
    }
}
