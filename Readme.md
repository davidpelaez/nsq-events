Nsq Event Router
=====

Trigger event handlers when a message is sent to a nsq topic.

This tiny util is a modification of nqs_tail. It adds and extra commad line flag `--handlers-dir` where you should put executables, either scripts or binaries, that are called when a message is sent to the indicated topic. The first word before a space in the received message contents is used as the target event handler to execute and the rest of the message is passed as an argument. 

The idea is to use this tool as a minimal equivalent of [event handler in a Serf cluster](http://www.serfdom.io/docs/recipes/event-handler-router.html), e.g. in a nsq+consul cluster.

There are two sample handlers, one only shows how the log prints error messages if the handler failed. The other, simply appends the messages contents to a text file inside the handlers directory.

## Examples


Listen for events in the `events` topic:
`nsq_event_router --topic events  --lookupd-http-address localhost:4161 --handlers-dir sample-handlers`

Trigger the error event, that will always fail:
`curl -d 'error ----' 'http://127.0.0.1:4151/put?topic=events'`

Trigger append event (creates/appends the body, hello world, to `samples-handlers/out.tmp`):
`curl -d 'append hello world' 'http://127.0.0.1:4151/put?topic=events'`

Trigger the event with the cli binary (defaulting to localhost's nsqd):
`nsq_trigger --topic events append "hello world!"`

Trigger the event with the cli binary (remote nsqd):
`nsq_trigger --topic events --nsqd-http-address elsewhere:4151 append "hello world!"`

Development instructions
====

Make sure you have GOPATH defined and [godep](https://github.com/tools/godep)

