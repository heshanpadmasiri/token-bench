---
name: http-client
description: Use this when you need to make a request to a rest endpoint.
---
- First identify the response shape and define a record for that. Default to open records
- Then perform the request and bind it to the expected output shape. If not matching runtime will return an error

```ballerin
import ballerina/http;
type Album readonly & record {
    string title;
    string artist;
};

public function main() returns error? {
    // Creates a new client with the Basic REST service URL.
    http:Client albumClient = check new ("localhost:9090");

    // Sends a `GET` request to the "/albums" resource.
    // The verb is not mandatory as it is default to "GET".
    Album[] albums = check albumClient->/albums;
    io:println("GET request:" + albums.toJsonString());

    // Sends a `POST` request to the "/albums" resource.
    Album album = check albumClient->/albums.post({
        title: "Sarah Vaughan and Clifford Brown",
        artist: "Sarah Vaughan"
    });
    io:println("\nPOST request:" + album.toJsonString());
}
```
