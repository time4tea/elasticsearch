package org.elasticsearch.river.couchdb.async;

import org.hamcrest.SelfDescribing;

public interface Probe extends SelfDescribing {
    void probe();

    boolean isOk();
}
