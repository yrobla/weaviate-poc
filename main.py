from datetime import datetime
import json
import weaviate
from weaviate.embedded import EmbeddedOptions
from weaviate.classes.config import Property, DataType


json_files = [
    'data/archived.jsonl',
    'data/deprecated.jsonl',
    'data/malicious.jsonl',
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

    for json_file in json_files:
        with open(json_file, 'r') as f:
            print("Adding data from", json_file)
            with collection.batch.dynamic() as batch:
                for line in f:
                    package = json.loads(line)

                    # now add the status column
                    if 'archived' in json_file:
                        package['status'] = 'archived'
                    elif 'deprecated' in json_file:
                        package['status'] = 'deprecated'
                    elif 'malicious' in json_file:
                        package['status'] = 'malicious'
                    else:
                        package['status'] = 'unknown'

                    batch.add_object(properties=package)


def perform_backup(client):
    backup_name = "backup-"+datetime.now().strftime("%Y%m%d-%H%M%S")
    result = client.backup.create(
        backup_id=backup_name,
        backend="filesystem",
        include_collections=["Package"],
        exclude_collections=None,
        wait_for_completion=True,
    )
    print("result is")
    print(result)


def test_weaviate():
    client = weaviate.WeaviateClient(
        embedded_options=EmbeddedOptions(
            additional_env_vars={
                "ENABLE_MODULES": "backup-filesystem",
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
        try:
            perform_backup(client)
        except Exception as e:
            print("Error during backup")
            print(e)


if __name__ == '__main__':
    test_weaviate()
