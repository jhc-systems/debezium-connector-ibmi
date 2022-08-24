package com.fnz.kafka.connector.health;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fnz.kafka.connector.health.beans.ConnectorStatus;
import com.fnz.kafka.connector.health.beans.State;
import com.fnz.kafka.connector.health.beans.Task;


public class CheckHealth {

    public static void main(String[] args) throws Exception {
        String uri = "http://127.0.0.1:8083";
        if (args.length > 0 ) {
            uri = args[0];
        }
        try {

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(uri + "/connectors/")).GET().build();

            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> responses = client.send(request, HttpResponse.BodyHandlers.ofString());
            
            ObjectMapper mapper = new ObjectMapper();
            String[] connectors = mapper.readValue(responses.body(), String[].class);
            boolean ok = true;
            for (String connector: connectors) {
                ok &= checkStatus(uri, connector);
            }
            if (ok) {
                System.exit(0);
            } else {
                System.exit(1);            
            }
        } catch (IOException e) {
            throw new IOException("Failed with url "+ uri, e);
        }
    }
    
    public static final Set<State> Acceptable = Set.of(State.RUNNING, State.UNASSIGNED);
    public static boolean checkStatus(String url, String connector) throws Exception {
        
        String uri = String.format("%s/connectors/%s/status", url, connector);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(uri)).GET().build();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> responses = client.send(request, HttpResponse.BodyHandlers.ofString());
        ObjectMapper mapper = new ObjectMapper();
        String body = responses.body();
        boolean ok = true;
        if (responses.statusCode() == 200) {
            ConnectorStatus cs = mapper.readValue(body, ConnectorStatus.class);
            
            if (Acceptable.contains(cs.connector().state())) {
                for (Task t: cs.tasks()) {
                    if (Acceptable.contains(t.state())) {
                        System.out.println(String.format("%s task %s %s OK", cs.name(), t.id(), t.state()));
                    } else {
                        System.out.println(String.format("%s task %s %s NOT OK - trace: %s", cs.name(), t.id(), t.state(), t.trace()));
                        ok = false;
                    }
                }
            } else {
                System.out.println(String.format("%s connector state %s NOT OK", cs.name(), cs.connector().state()));
                ok = false;
            }
        } else {
            System.out.println(String.format("check failed for %s status was %s", uri, responses.statusCode()));
        }
        return ok;
    }
}
