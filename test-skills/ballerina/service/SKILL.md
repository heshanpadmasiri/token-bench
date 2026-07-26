---
name: service
description: Use this when user asks you to create a service.
---

- IMPORTANT: don't use http:Request directly anywhere in the source code
- IMPORTANT: don't use `json` or `byte[]` as the payload type in service. Always explicitly define the record type for the payload
- IMPORTANT: never try to manually parse the payload or validate it. Always explicitly define the shape of the input and let runtime validate it

First identify the shape of data you should receive from outside. Also determine the expected shape of the result. For inputs define `readonly` open records and for result define closed records 
  - Open records allow addition fields than what you describe
  ```ballerina
     anydata a = a.["fieldName"]; // By default rest type is anydata
     if a == () {
        // no a
     } if a is int {
        int x = a; // NOTE: a is narrowed to int type. 
     }
     boolean hasField = a.hasField("fieldName"); // When you just need to check if a has a field
      
  ```


```ballerina

import ballerina/http;
type Data readonly & record {
  UserId string
  Name string
};

type Response record {|
  DataofBirth string
|};

service /dob on new http:Listener(8080) {
  // $rest-operation on $base-path/$rest-operation
  isolated resource function post find(request Data) Response|error {
    // do something
    // var userId = request.UserId
    // var someotherField = request["someotherField"]
    Response r = {...};
    return r;
  }
}
```

Ballerina runtime will validate payload can match the given shape you don't need to manually validate that.

### Path parameters
```ballerina
import ballerina/http;
type Response record {|
  DataofBirth string
|};

service /dob on new http:Listener(8080) {
  // $rest-operation on $base-path/$rest-operation
  isolated resource function get /[string UserId]/[string name]() Response|error {
    // do something
    string dob = findDob(userId, name)
    Response r = {...};
    return r;
  }
}
```
Similar to payload runtime will validate you can bind the path parameter to the given type.

### Header parameters

If you need to access a specific http header parameter use `@http:header` annotation

```ballerina
import ballerina/http;
import ballerina/mime;

type Album readonly & record {|
    string title;
    string artist;
|};

table<Album> key(title) albums = table [
    {title: "Blue Train", artist: "John Coltrane"},
    {title: "Jeru", artist: "Gerry Mulligan"}
];

service / on new http:Listener(9090) {

    // The `accept` argument with `@http:Header` annotation takes the value of the `Accept` request header.
    resource function get albums(@http:Header string accept) returns Album[]|http:NotAcceptable {
        if !string:equalsIgnoreCaseAscii(accept, mime:APPLICATION_JSON) {
            return http:NOT_ACCEPTABLE;
        }
        return albums.toArray();
    }
}
```
