# maelstrom/maelstrom expects a binary and gets confused with
# clojure -M -m maelstrom.datomic.maelstrom.datomic
# hence two scripts, one to invoke maelstrom's machinery and another to exec the uber jar

echo "building the uber jar file.."
clj -T:build uber && echo "target built! running maelstrom..." && \
../maelstrom/maelstrom test \
  -w echo \
  --bin "./run-node.sh" \
  --node-count 1 \
  --time-limit 10
