# file: run-node.sh
#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
java -jar "$SCRIPT_DIR/target/maelstrom.datomic/maelstrom.datomic-0.1.0-SNAPSHOT.jar" "$@"
