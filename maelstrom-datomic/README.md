# maelstrom.datomic

## Usage

Run the project directly, via `:main-opts` (`-m maelstrom.datomic.maelstrom.datomic`):
    $ clojure -M:run-m
    Hello, World!

this will loop forever until a JSON message is passed to STDIN assuming maelstrom is in `maelstrom/maelstrom`,
build an uber .jar artefact of the project:

```
clojure -T:build uber
```

and pass the binary to maelstrom with the helper bash script:
```
./run.sh
```

Run the project's tests (they'll fail until you edit them):

    $ clojure -T:build test

Run the project's CI pipeline and build an uberjar (this will fail until you edit the tests to pass):

    $ clojure -T:build ci

Run that uberjar:

    $ java -jar target/maelstrom.datomic/maelstrom.datomic-0.1.0-SNAPSHOT.jar
