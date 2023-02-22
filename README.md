<p align="center">
	<img src="https://github.com/newvicx/hyprxa/blob/master/docs/img/logo.png?raw=true" width="465" height="345" alt='Hyprxa'>
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

Hyprxa is designed to help developers build secure, scalable, and robust event driven applications. Hyprxa scales with your needs using some of the leading brands in distributed database and messaging technologies.

Getting going with hyprxa is easy once you have MongoDB, RabbitMQ, and Memcached running.

**main.py**

```python
from hyprxa.application import Hyprxa

app = Hyprxa(debug=True)
```

And thats it! Save, *main.py* to a folder and fire up a command prompt. Activate your virtual environment (yes you should absolutely be using a virtual environment), navigate to the directory  where you saved *main.py* and run

`uvicorn main:app`

Open your browser to http://localhost:8000/docs and your screen should look like this...

![full-demo-docs-view](https://github.com/newvicx/hyprxa/blob/master/docs/img/full-demo-docs-view.JPG?raw=true)

And with that you're up and running. You can test out some endpoints in the *Events* and *Topics* section which make up the event hub service. The *Timeseries* and *Unitops* sections wont be useful for the moment because you didn't add any data source integrations yet.

## Event Hub

Hyprxa ships with a ready-to-use event hub that can be added as a component to new applications. The core model for the event hub is the **topic**. A topic has a name and a JSON schema that defines the structure for any events associated to that topic. When an event is published to the API, hyprxa validates the event payload against the topic schema. If the validation succeeds, the event is published to the service. Any subscribers listening for the topic (or a sub-topic routing key) will receive the posted event. Additionally, the event is persisted to the database for long term storage. Events can be recalled from the database for auditing and event tracing.

## Data Integrations (Timeseries)

The goal of the hyrpxa data integration framework is to provide developers the tools to create a unified timeseries data stream across different data sources. Resources in a data source are uniquely identified through a **subscription**.  A subscription contains all the information required for a data integration to connect to and stream data for a resource. Hyprxa does not ship with any data integrations, it is up to developers to write the integrations. Hyprxa does not care about the underlying protocol, a data integration could be a file watcher to a folder containing CSV files or it could be a websocket connection to a REST API. Hyprxa only requires that the messages from the integration be in a standard format. It will not forward messages to subscribers if the message does not adhere to the schema.

Hyprxa provides a means of grouping subscriptions through a **unitop**. A unitop is a logical grouping of one or more subscriptions to one or more data sources indexed by a common name. Unitops typically represent a physical asset and the subscriptions are sensors on the asset, work orders associated to the asset, transcations related to the asset, etc. However, a unitop can be any logical grouping. For example, we could have a Massachusetts unitop that streams metrics such as average housing price, crime rate, interest rate, etc.

## Documentation

I am working on writing the documentation for Hyprxa. Stay tuned!

## Development

Hyprxa is under active development. I am discovering new bugs as I go and creating new releases rapidly until the API is more or less stable. Hyprxa will remain as an alpha release until proper code coverage testing and documentation is complete. Please report any bugs.
