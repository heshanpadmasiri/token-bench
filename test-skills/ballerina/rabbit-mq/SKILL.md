---
name: rabbit-mq
description: Use this when user asks create a listener or a publisher for rabit mq.
---

## listener

```ballerina

import ballerinax/rabbitmq;

// Message body
public type Order record {
    int orderId;
    string productName;
    decimal price;
    boolean isValid;
};

// The consumer service listens to the "OrderQueue" queue.
service "OrderQueue" on new rabbitmq:Listener(rabbitmq:DEFAULT_HOST, rabbitmq:DEFAULT_PORT) {

    remote function onMessage(Order 'order) returns error? {
        if 'order.isValid {
            log:printInfo(string `Received valid order for ${'order.productName}`);
        }
    }
}
```

## publisher

```ballerina
function publish(Order order) returns error? {
    rabbitmq:Client orderClient = check new (rabbitmq:DEFAULT_HOST, rabbitmq:DEFAULT_PORT);
    check orderClient->publishMessage({
                content: newOrder,
                routingKey: "OrderQueue"
            });
}
```

