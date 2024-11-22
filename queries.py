from embedding_util import generate_embeddings
import weaviate
from weaviate.embedded import EmbeddedOptions
from weaviate.classes.query import MetadataQuery


def run_queries(client):
    collection = client.collections.get("Package")
    queries = [
        "give me firebase related packages",
        "how can i minify AngularJS code",
        "give me an npm package to interact with lighthouse",
        "list me python packages to interact with SVG icons",
        "list me go packages to work with ipfs"
    ]
    for query in queries:
        print("**** Query is ****", query)
        query_vector = generate_embeddings(query)

        response = collection.query.near_vector(
            query_vector, limit=5, return_metadata=MetadataQuery(distance=True), distance=0.1)

        for o in response.objects:
            print(o.properties)
            print(o.metadata.distance)


def perform_queries():
    client = weaviate.WeaviateClient(
        embedded_options=EmbeddedOptions(
            persistence_data_path="./weaviate_data"
        ),
    )
    with client:
        client.connect()
        run_queries(client)


if __name__ == '__main__':
    perform_queries()
