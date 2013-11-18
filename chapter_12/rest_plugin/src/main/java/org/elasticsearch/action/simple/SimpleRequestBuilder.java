package org.elasticsearch.action.simple;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalGenericClient;

/**
 * A request to get simples of one or more indices.
 */
public class SimpleRequestBuilder extends BroadcastOperationRequestBuilder<SimpleRequest, SimpleResponse, SimpleRequestBuilder> {

    public SimpleRequestBuilder(InternalGenericClient client) {
        super(client, new SimpleRequest());
    }

    @Override
    protected void doExecute(ActionListener<SimpleResponse> listener) {
        ((Client)client).execute(SimpleAction.INSTANCE, request, listener);
    }
}
