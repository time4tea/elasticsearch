package org.elasticsearch.river.couchdb;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.xcontent.TermQueryBuilder;
import org.elasticsearch.index.query.xcontent.XContentQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.river.couchdb.async.Timeout;
import org.elasticsearch.river.couchdb.http.Http;
import org.elasticsearch.river.couchdb.run.Couch;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import static org.elasticsearch.client.Requests.*;
import static org.elasticsearch.common.io.FileSystemUtils.*;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import static org.elasticsearch.common.xcontent.XContentFactory.*;
import static org.elasticsearch.index.query.xcontent.QueryBuilders.*;
import static org.elasticsearch.node.NodeBuilder.*;
import static org.elasticsearch.river.couchdb.SearchResponseMatchers.*;
import static org.elasticsearch.river.couchdb.async.TimeoutTimer.*;
import static org.elasticsearch.search.builder.SearchSourceBuilder.*;
import static org.hamcrest.Matchers.*;

public class CouchDBRiverTests {
    private final Http http = new Http();

    private Couch couch;
    private CouchDatabase database;
    private Node node;

    @BeforeMethod
    public void setUp() throws Exception {

        removeExistingTestIndexes();

        couch = new Couch(http);
        couch.start();

        database = couch.createDatabase("database");
        node = startLocalNode();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if ( node != null ) node.stop();
        if ( couch != null ) couch.stop();
    }

    private void removeExistingTestIndexes() {
        deleteRecursively(new File("data"));
    }

    @Test
    public void riverFlowsWhenCouchIsRunning() throws Exception {

        startRiver(node, couchDbRiverFor(this.couch, this.database));

        database.createDocument("id", "{ \"test\" : \"value\" } ");

        Timeout timeout = Timeout.seconds(5);

        within(timeout).assertThat(
                "Should find a document", query(termQuery("test", "value")), hasHits(hasTotalHits(equalTo(1l)))
        );

        within(timeout).assertThat(
                "Document is the expected one", query(termQuery("test", "value")), hasHits(hasHitAtPosition(0, hasId(equalTo("id"))))
        );
    }

    private Callable<SearchResponse> query(final TermQueryBuilder query) {
        return new Callable<SearchResponse>() {
            @Override public SearchResponse call() throws Exception {
                return queryNodeFor(node, query);
            }
        };
    }

    private Node startLocalNode() {
        Settings settings = settingsBuilder()
                .put("gateway.type", "local")
                .build();

        return nodeBuilder().settings(settings).node();
    }

    private SearchResponse queryNodeFor(Node node, XContentQueryBuilder query) {
        return node.client().search(searchRequest()
                .source(searchSource().query(query))).actionGet();
    }

    private XContentBuilder couchDbRiverFor(Couch couch, CouchDatabase database) throws IOException {
        return jsonBuilder()
                .startObject()
                .field("type", "couchdb")
                .field("couchdb" )
                .startObject()
                .field("host", couch.uri().getHost())
                .field("port", couch.uri().getPort())
                .field("db", database.name())
                .endObject()
                .endObject();
    }

    private void startRiver(Node node, XContentBuilder startRiverRequest) {
        node.client().prepareIndex("_river", "db", "_meta").setSource(startRiverRequest).execute().actionGet();
    }
}
