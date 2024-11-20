import weaviate
from weaviate.embedded import EmbeddedOptions
from weaviate.classes.config import Property, DataType
from weaviate.classes.query import MetadataQuery


packages = [
    {"name": "invokehttp", "type": "pypi", "status": "malicious", "description": "A malicious package that sends data to a remote server."},
    {"name": "numpy", "type": "pypi", "status": "good", "description": "A powerful package for numerical computing."},
    {"name": "pandas", "type": "pypi", "status": "good", "description": "A versatile package for data manipulation and analysis."},
    {"name": "react", "type": "npm", "status": "good", "description": "A popular library for building user interfaces."},
    {"name": "deprecated-package", "type": "npm", "status": "deprecated", "description": "A package that is no longer maintained."},
    {"name": "archived-package", "type": "npm", "status": "archived", "description": "A package that has been archived."},
]


def setup_schema(client):
    if client.collections.exists("Package"):
        client.collections.delete("Package")
    client.collections.create(
        "Package",
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="type", data_type=DataType.TEXT),
            Property(name="status", data_type=DataType.TEXT),
            Property(name="description", data_type=DataType.TEXT),
        ]
    )


def add_data(client):
    collection = client.collections.get("Package")
    with collection.batch.dynamic() as batch:
        for package in packages:
            # search string is package name + description
            batch.add_object(properties=package)


def perform_search(client):
    package_col = client.collections.get("Package")
    response = package_col.query.bm25(query="longer", limit=5, return_metadata=MetadataQuery(distance=True))
    for o in response.objects:
        print(o.properties)
        print(o.metadata.distance)


def test_weaviate():
    client = weaviate.WeaviateClient(
        embedded_options=EmbeddedOptions(
            additional_env_vars={
                "ENABLE_MODULES": "backup-filesystem,text2vec-openai,text2vec-cohere,text2vec-huggingface,ref2vec-centroid,generative-openai,qna-openai",
                "BACKUP_FILESYSTEM_PATH": "/tmp/backups"
            },
            persistence_data_path="./weaviate_data"

        ),
    )
    with client:
        client.connect()
        print('is_ready:', client.is_ready())

        setup_schema(client)
        add_data(client)
        perform_search(client)


if __name__ == '__main__':
    test_weaviate()
