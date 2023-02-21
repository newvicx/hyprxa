<p align="center">
	<img src="https://github.com/newvicx/hyprxa/blob/master/docs/img/logo.png?raw=true)" width="465" height="345" alt='Hyprxa'>
</p>

<p align="center"><strong>Hyprxa</strong> <em> (hyper-ex-ay)</em></p>

<p align="center"><em>An asynchronous data integration framework and event hub built on
    </em><a href="https://fastapi.tiangolo.com/"> FastAPI</a>
</p>

## Installation

Install hyprxa using pip:

`pip install hyprxa`

You will also need [MongoDB](https://www.mongodb.com/), [RabbitMQ](https://www.rabbitmq.com/), and [Memcached](https://memcached.org/). To get going quickly, clone the repository and run `docker compose up`

## Getting Started

### Event Hub

Hyprxa ships with a ready to use event hub service that can be added to any project. The event hub allows publishing events with a defined schema to the service. Those events are routed to subscribers listening for a specific topic or sub-topic in real time. Published events are also stored in the database.

**main.py**

```python
from fastapi import FastAPI
from hyprxa.auth import BaseUser
from hyprxa.auth.debug import DebugAuthenticationMiddleware
from hyprxa.exception_handlers import (
	handle_CacheError,
    handle_DatabaseUnavailable,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError,
)
from hyprxa.base import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching import CacheError
from hyprxa.exceptions import NotConfiguredError
from hyprxa.routes import (
    admin_router,
    events_router,
    topics_router,
    users_router
)
from hyprxa.settings import HYPRXA_SETTINGS, LOGGING_SETTINGS
from pymongo.errors import PyMongoError
from starlette.authentication import AuthenticationBackend


# Create an admin user for debug so you dont need to write an authentication backend
ADMIN_USER = BaseUser(
    username="admin",
    first_name="John",
    last_name="Smith",
    email="johnsmith@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            HYPRXA_SETTINGS.admin_scopes,
            HYPRXA_SETTINGS.write_scopes,
            HYPRXA_SETTINGS.read_scopes
        )
    )
)

# Configure logging for this application
LOGGING_SETTINGS.configure_logging()

app = FastAPI(
    debug=True,
    title="hyprxa-events-demo",
    description="Hyprxa event hub demo."
)

DebugAuthenticationMiddleware.set_user(ADMIN_USER)
app.add_middleware(DebugAuthenticationMiddleware, backend=AuthenticationBackend())

# Add routes for event hub service
app.include_router(admin_router)
app.include_router(events_router)
app.include_router(topics_router)
app.include_router(users_router)

# Add exception handlers
app.add_exception_handler(NotConfiguredError, handle_NotConfiguredError)
app.add_exception_handler(PyMongoError, handle_PyMongoError)
app.add_exception_handler(DatabaseUnavailable, handle_DatabaseUnavailable)
app.add_exception_handler(ManagerClosed, handle_ManagerClosed)
app.add_exception_handler(SubscriptionTimeout, handle_retryable_SubscriptionError)
app.add_exception_handler(SubscriptionLimitError, handle_retryable_SubscriptionError)
app.add_exception_handler(CacheError, handle_CacheError)
```

Save the file to a folder, navigate to the folder from the command prompt and run...

`uvicorn main:app`

Open http://localhost:8000/docs in a browser

![events-demo-docs-view.JPG](https://github.com/newvicx/hyprxa/blob/master/docs/img/events-demo-docs-view.JPG?raw=true)

With the event hub running we can define a topic and publish some events...

**sample_events.py**

```python
import concurrent.futures
import json
import random
import time
from datetime import datetime

from hyprxa.client import HyprxaClient
from hyprxa.event import Event
from hyprxa.topics import Topic
from hyprxa.util import StatusOptions
from pydantic import BaseModel, Field



class SampleEvent(BaseModel):
    sample_val: float
    timestamp: datetime = Field(default_factory=datetime.now)
    
topic = Topic(topic="sample", jschema=json.dumps(SampleEvent.schema()))

def create_topic(client: HyprxaClient) -> None:
    status = client.create_topic(topic=topic)
    assert status.status is StatusOptions.OK
    
def publish_events(client: HyprxaClient) -> None:
    for i in range(10):
        event = Event(
            topic="sample",
            payload=SampleEvent(sample_val=random.randint(1,10)).dict()
        )
        time.sleep(0.5)
        
def listen_for_events(client: HyprxaClient) -> None:
    for event in client.stream_events(topic="samples"):
        assert event.topic == "samples"
        print(event.payload)

        
with HyprxaClient("http://localhost:8000") as client:
    create_topic(client)
    with concurrent.futures.ThreadPoolExecutor as executor:
        listener = executor.submit(listen_for_events, client)
        # Give a little time for connection to be made
        time.sleep(1)
        # Publish the events, we should see the event payload printed
        publish_events(client)
        listener.cancel()
        # The listener will actually stop when the client closes after we exit the
        # context block
```

The event hub supports sub-topic routing using the same semantics as RabbitMQ [topic](https://www.rabbitmq.com/tutorials/tutorial-five-python.html) exchanges. Lets modify `publish_events` and `listen_for_events` from above...

```python
def publish_events(client: HyprxaClient) -> None:
    for i in range(10):
        event = Event(
            topic="sample",
            routing_key="mytestsamples" # Full routing key is "sample.mytestsamples"
            payload=SampleEvent(sample_val=random.randint(1,10)).dict()
        )
        time.sleep(0.5)
        
def listen_for_events(client: HyprxaClient) -> None:
    for event in client.stream_events(topic="samples", routing_key="mytestsamples"):
        assert event.topic == "samples"
        print(event.payload)
```

Now the subscriber will only receive events with the sub-topic "mytestsamples".

### Data Integration

Hyprxa is also a framework for real-time data integrations. The purpose of the data integration framework is to provide the tools for developers to create a unified stream of timeseries data across multiple sources.

To enable the timeseries API, lets modify **main.py**

**main.py**

```python
from fastapi import FastAPI
from hyprxa.auth import BaseUser
from hyprxa.auth.debug import DebugAuthenticationMiddleware
from hyprxa.exception_handlers import (
	handle_CacheError,
    handle_ClientSubscriptionError, # Import handler for ClientSubscriptionError
    handle_DatabaseUnavailable,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError,
    handle_ClientSubscriptionError
)
from hyprxa.base import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching import CacheError
from hyprxa.exceptions import NotConfiguredError
from hyprxa.routes import (
    admin_router,
    events_router,
    timeseries_router, # Import the timeseries router
    topics_router,
    unitops_router, # Import the unitops router
    users_router
)
from hyprxa.settings import HYPRXA_SETTINGS, LOGGING_SETTINGS
# Import timeseries specific errors
from hyprxa.timeseries import ClientSubscriptionError, SubscriptionLockError
from pymongo.errors import PyMongoError
from starlette.authentication import AuthenticationBackend


# Create an admin user for debug so you dont need to write an authentication backend
ADMIN_USER = BaseUser(
    username="admin",
    first_name="John",
    last_name="Smith",
    email="johnsmith@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            HYPRXA_SETTINGS.admin_scopes,
            HYPRXA_SETTINGS.write_scopes,
            HYPRXA_SETTINGS.read_scopes
        )
    )
)

# Configure logging for this application
LOGGING_SETTINGS.configure_logging()

app = FastAPI(
    debug=True,
    title="hyprxa-demo",
    description="Hyprxa timeseries and event hub demo."
)

DebugAuthenticationMiddleware.set_user(ADMIN_USER)
app.add_middleware(DebugAuthenticationMiddleware, backend=AuthenticationBackend())

# Add routes for event hub service
app.include_router(admin_router)
app.include_router(events_router)
app.include_router(timeseries_router) # Include the timeseries router
app.include_router(topics_router)
app.include_router(unitops_router) # Include the unitops router
app.include_router(users_router)

# Add exception handlers
app.add_exception_handler(NotConfiguredError, handle_NotConfiguredError)
app.add_exception_handler(PyMongoError, handle_PyMongoError)
app.add_exception_handler(DatabaseUnavailable, handle_DatabaseUnavailable)
app.add_exception_handler(ManagerClosed, handle_ManagerClosed)
app.add_exception_handler(SubscriptionTimeout, handle_retryable_SubscriptionError)
app.add_exception_handler(SubscriptionLimitError, handle_retryable_SubscriptionError)
app.add_exception_handler(CacheError, handle_CacheError)

# Add exception handlers for timeseries specific errors
app.add_exception_handler(ClientSubscriptionError, handle_ClientSubscriptionError)
app.add_exception_handler(SubscriptionLockError, handle_retryable_SubscriptionError)
```

Save the file to a folder, navigate to the folder from the command prompt and run...

`uvicorn main:app`

Open http://localhost:8000/docs in a browser

![full-demo-docs-view.JPG](https://github.com/newvicx/hyprxa/blob/master/docs/img/full-demo-docs-view.JPG?raw=true)

The timeseries API is useless without any configured data sources though. You can verify it yourself from the docs. Try to create a unitop...

![timeseries-unitop-fail-no-sources.JPG](https://github.com/newvicx/hyprxa/blob/master/docs/img/timeseries-unitop-fail-no-sources.JPG?raw=true)

Any value for `source` in the data mapping field is going to be invalid because no sources are configured.

#### Hyprxa Data Integration Model

Hyprxa provides a standardized interface for integrations connected to the API. All timeseries data integrations are managed by a `TimeseriesManager`. It is the responsibility of the manager to bridge the gap between a subscriber (the consumer of the data) and the client (the interface between a manager and a data source). In this model, data flows like so...

![data-flow-model.JPG](https://github.com/newvicx/hyprxa/blob/master/docs/img/data-flow-model.JPG?raw=true)

Lets talk about some of the components in this model...

##### Integration

In hyprxa, the term *integration* is used to define a pool of connections to a data source. Integrations abstract away the underlying network protocol used to connect a data source from a `TimeseriesManager`. They subscribe to data points from a source as directed by a manager and manage a pool of connections to the data source. Hyprxa exposes `BaseIntegration` which all clients for data integrations must inherit from. 

##### Connections

In hyprxa, a *connection* represents a logical connection to a data source. Data sources can be anything from CSV files, to REST API's, to a remote database. For a CSV source, a connection may be a file watcher waiting for new files to process whereas for a REST API, a connection is an HTTP/Websocket connection to the source. The underlying protocol is only important to the connection, the manager (and to a large extent the client) doesn't care how the data gets to it. The actual I/O and data processing occurs within a connection. Clients manage a pool of connections to a data source and will spin up new connections as needed to support the subscriptions requested by the manager. Connections are only ever created in the scope of client. Hyprxa exposes `BaseConnection` which all connection implementations must inherit from. 

##### Subscriber

In hyprxa, a *subscriber* represents an asynchronous stream of data for one or more subscriptions from one or more data sources. Subscribers are only ever created in the scope of a manager. Developers should not have to implement their own subscribers.

#### Adding Sources

Assuming you have written a client/connection implementation to a data source, you now want to add that integration to your application. For this you can use the `add_source` method. Lets go back to **main.py**...

**main.py**

```python
from fastapi import FastAPI
from hyprxa.auth import BaseUser
from hyprxa.auth.debug import DebugAuthenticationMiddleware
from hyprxa.exception_handlers import (
	handle_CacheError,
    handle_ClientSubscriptionError,
    handle_DatabaseUnavailable,
    handle_NotConfiguredError,
    handle_PyMongoError,
    handle_retryable_SubscriptionError,
    handle_ClientSubscriptionError
)
from hyprxa.base import (
    ManagerClosed,
    SubscriptionLimitError,
    SubscriptionTimeout
)
from hyprxa.caching import CacheError
from hyprxa.exceptions import NotConfiguredError
from hyprxa.routes import (
    admin_router,
    events_router,
    timeseries_router,
    topics_router,
    unitops_router,
    users_router
)
from hyprxa.settings import HYPRXA_SETTINGS, LOGGING_SETTINGS
from hyprxa.timeseries import ClientSubscriptionError, SubscriptionLockError
from hyprxa.timeseries import add_source # Import add_source method
from pymongo.errors import PyMongoError
from starlette.authentication import AuthenticationBackend

from myapplication.sources import MyClient # Import your client implementation


# We can add a source anywhere in main.py
add_source(MyClient, ...)


# Create an admin user for debug so you dont need to write an authentication backend
ADMIN_USER = BaseUser(
    username="admin",
    first_name="John",
    last_name="Smith",
    email="johnsmith@gmail.com",
    upi=2191996,
    company="Prestige Worldwide",
    scopes=set(
        itertools.chain(
            HYPRXA_SETTINGS.admin_scopes,
            HYPRXA_SETTINGS.write_scopes,
            HYPRXA_SETTINGS.read_scopes
        )
    )
)

# Configure logging for this application
LOGGING_SETTINGS.configure_logging()

app = FastAPI(
    debug=True,
    title="hyprxa-demo",
    description="Hyprxa timeseries and event hub demo."
)

DebugAuthenticationMiddleware.set_user(ADMIN_USER)
app.add_middleware(DebugAuthenticationMiddleware, backend=AuthenticationBackend())

# Add routes for event hub service
app.include_router(admin_router)
app.include_router(events_router)
app.include_router(timeseries_router)
app.include_router(topics_router)
app.include_router(unitops_router)
app.include_router(users_router)

# Add exception handlers
app.add_exception_handler(NotConfiguredError, handle_NotConfiguredError)
app.add_exception_handler(PyMongoError, handle_PyMongoError)
app.add_exception_handler(DatabaseUnavailable, handle_DatabaseUnavailable)
app.add_exception_handler(ManagerClosed, handle_ManagerClosed)
app.add_exception_handler(SubscriptionTimeout, handle_retryable_SubscriptionError)
app.add_exception_handler(SubscriptionLimitError, handle_retryable_SubscriptionError)
app.add_exception_handler(CacheError, handle_CacheError)
app.add_exception_handler(ClientSubscriptionError, handle_ClientSubscriptionError)
app.add_exception_handler(SubscriptionLockError, handle_retryable_SubscriptionError)
```

To add a source, import `add_source` from `hyprxa.timeseries`. You can add a source anywhere in **main.py**, `add_source` does not depend on an application instance. When you add a source, you must specify any arguments to be passed to the client constructor. `add_source` creates a zero argument callable that the manager will use to initialize the client when it is needed.

### Authentication/Authorization

Hyprxa requires that `starlette.middleware.authentication.AuthenticationMiddleware` be installed. Hyprxa ties user tokens to transactions in the database as well as logs. However, the authentication/authorization backend is pluggable. See the examples section for an example of using LDAP authentication against an ActiveDirectory server.

An authenticated user can have any number of scopes associated to them. The users scopes grant them access to resources within your API. Hyprxa has 3 default classes of scopes that can be set with environment variables (HYPRXA_READ_SCOPES/HYPRXA_WRITE_SCOPES/HYPRXA_ADMIN_SCOPES). The scopes should reference an actual scope that can be obtained from the authorization backend. For example, in a domain controlled environment using ActiveDirectory, the READ, WRITE, and ADMIN scopes would be active directory groups (ex APP-HYPRXA-READ). Hyprxa doesn't care about the naming conventions of the scopes it just needs to know what scopes are required for users to access resources.

Hyprxa will also lock down sources you've added to your API. `add_source` has 3 additional arguments that can be passed.

- scopes (Sequence[str]): The scopes required to a access a data source.
- any_ (bool): If `True`, the user can have any of the defined scopes and still access the resource. This is useful when you have scopes that grant increasing levels of access but are backwards compatible. For example, a user with ADMIN access can write and read, a user with WRITE access can write and read, and a user with READ access can only read. A user with any of ADMIN, WRITE, or READ scopes can read data even if they have only 1 of the 3 scopes.
- raise_on_no_scopes (bool): If `True`, a source added with no scopes defined will raise a `NotConfiguredError` when trying to access the data source.

When Hyprxa receives a request to stream data from one or more sources, it verifies the user has the required scopes to access all the data sources.

### Documentation

The examples above give an incomplete view of all the features in hyprxa. We highly encourage you read the documentation and check out the examples.

### Development

Hyprxa is under active development but, the API is more or less finalized. Hyprxa will remain as an alpha release until proper code coverage testing is complete. Please report any bugs.

