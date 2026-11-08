# Development

## Make commands

Shortcuts for the Docker broker, defined in the [`Makefile`](../Makefile). Run `make` to list them.

| Command | What it does |
|---|---|
| `make up` | Start the broker and wait until it is ready |
| `make topics` | Create the `orders` and `trips` topics (3 partitions) |
| `make list` | List topics |
| `make produce N=10` | Send N fake orders |
| `make consume` | Read orders as group `order-tracker` (Ctrl-C to stop) |
| `make group GROUP=order-tracker` | Show partitions, offsets and lag for a consumer group |
| `make quorum` | Show the KRaft controller quorum |
| `make logs` | Follow the broker logs |
| `make stop` | Stop the broker (keeps data) |
| `make down` | Remove the container (keeps data in the volume) |
| `make clean` | Remove the container **and delete its data** |

Add `BROKER=confluent` to any command to use the Confluent broker instead, for example `make up BROKER=confluent`.

## Adding a Python library

Dependencies are managed by [uv](https://docs.astral.sh/uv/). Add a library with:

    uv add confluent-kafka

This installs it into `.venv`, adds it to `dependencies` in `pyproject.toml`, and pins the exact version in `uv.lock`. Commit both files so everyone gets the same version.

- Version rules: `uv add "confluent-kafka>=2.6"`. Pin an exact one with `==`, as `pyspark` is (it must match the Spark Kafka connector in `stream/consume.py`).
- Tools only you need, like a linter: `uv add --dev ruff`. These go in a separate dev group.
- Remove: `uv remove confluent-kafka`.
- After pulling someone else's change to `pyproject.toml` or `uv.lock`: `uv sync`.

Do not use `pip install`: it skips `pyproject.toml` and `uv.lock`, so the next `uv sync` removes the library again.
