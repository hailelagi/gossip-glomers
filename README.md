# gossip-glomers

<https://fly.io/dist-sys/>

## Installation

There should be a `run.sh` script which expects a maelstrom binary to be downloaded in root at `maelstrom/maelstrom`.

```zsh
run.sh
```

don't forget to ask nicely, the script doesn't do anything interesting:

```zsh
chmod +x ./path/run.sh
```

## Note to self

You can't do print statement debugging against maelstrom..(but duh!):

````zsh
WARN [2023-08-31 07:34:14,717] n1 stdout - maelstrom.process Error!
clojure.lang.ExceptionInfo: Node n1 printed a line to STDOUT which was not well-formed JSON:
map[msg_id:1 type:generate]
Did you mean to encode this line as JSON? Or was this line intended for STDERR? See doc/protocol.md for more guidance.
        at slingshot.support$stack_trace.invoke(support.clj:201)
````
