from flask import g, current_app
from opensearchpy import OpenSearch
from week1.index_products import get_opensearch as opensearch

# Create an OpenSearch client instance and put it into Flask shared space for use by the application
def get_opensearch():
    if 'opensearch' not in g:
        #### Step 4.a:
        # Implement a client connection to OpenSearch so that the rest of the application can communicate with OpenSearch
        g.opensearch = opensearch()

    return g.opensearch
