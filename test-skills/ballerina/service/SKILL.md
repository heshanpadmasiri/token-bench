---
name: service
description: Use this when user asks you to create a HTTP service or REST end point.
---

First identify the payload shape for each endpoint based user input. If you expect addition fields use a open record else use close record. Default to open records
Similarly try to define response shape. Default to close records

```ballerina
type CloseRecordPayload readonly & record {|
  $body
|}

type OpenRecordPayload readonly & record {
  $body
}
```
Then define service end points as fallows

```ballerina
import ballerina/http;
service $base-path on new http:Listener($port) {
  // $rest-operation on $base-path/$rest-operation
  resource function $rest-operation(get|post|put) $endpoint($payloadtype request) $responsetype|error {
    $body
  }

  // you can also get path parameters
  resource function $rest-operation(get|post|put) foo/bar/[ty param]($payloadtype request) $responsetype|error {
    $body
  }
}
```

IMPORTANT: when you define a `$payloadtype` like this runtime will validate payload no need to do it ourself.

If you want to allow concurrent invocations for resource function you must declare them as isolated and parameters to be immutable. Also all the functions you call must be isolated as well (same restrictions)
