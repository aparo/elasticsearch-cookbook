package org.elasticsearch.action.simple;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalGenericClient;

public class SimpleAction extends Action<SimpleRequest, SimpleResponse, SimpleRequestBuilder> {

    public static final SimpleAction INSTANCE = new SimpleAction();
    public static final String NAME = "indices/simple";

    private SimpleAction() {
        super(NAME);
    }

    @Override
    public SimpleResponse newResponse() {
        return new SimpleResponse();
    }

@Override
public SimpleRequestBuilder newRequestBuilder(Client client) {
    return new SimpleRequestBuilder((InternalGenericClient)client);
}
}
