# Convenience script to run binaries against maelstrom and view STDOUT
# long live STDOUT debugging :)
# I might as well have written logical correctness tests in a test.go but this is easier
go install . && ../maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 3 --rate 1 --node-count 3 --availability total --nemesis partition
