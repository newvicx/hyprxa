# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 0.0.1 (21st Feb, 2023)

- Initial release
- The hyprxa API is unlikely to change but the project will remain in alpha until code coverage testing and documentation are complete

## 0.0.2 (22nd Feb, 2023)

- Exposed much more of the hyprxa internals at the top level of modules
- Added `Hyprxa` class which inherits from `FastAPI` and handles much of the boilerplate required for a hyprxa application
- `add_source` now accepts a callable that returns a `BaseIntegration`
- Unfied all exceptions and exception handlers under the `exceptions` module
- Minor bug fixes

## 0.0.3 (22nd Feb, 2023)

- Minor formatting changes
- `chunked_transfer` accepts `Type[None]` formatter. If `None` data will be written directly to `writer`

## 0.0.4 (22nd Feb, 2023)

- Bug fixes
    - token endpoint errored out when in debug mode
    - `SubscriptionMessage` needs to use `AnySourceSubscription` instead of `BaseSourceSubscription`
    - exchage type mismatch for `TimeseriesManager` and `EventManager`
    - /unitops/save errors out on update/insert because `AnySourceSubscription` is not `dict`
    - auth dependencies not able to acquire authentication middleware from app
    - client incorrectly encoding json data
    - `TimeseriesManager.info` not building correct info object
    - empty `ref` in `TimeseriesSubscriber` leading to `AssertionError`
- added `add_admin_scopes` to `Hyprxa`

## 0.0.5 (22nd Feb, 2023)

- Minor bug fixes

## 0.0.6 (24th Feb, 2023)

- Major bug fixes across the board
- Additional admin endpoints for logs
- Get timeseries data for a single data item in a unitop

## 0.0.7 (26th Feb, 2023)

- Additional queries for events. You can now query for events based on a routing key pattern and hyprxa will return the most recent event for each routing key matching the pattern
- Optionally stream a subset of data items for a unitop. Before, when streaming a unitop, all data items would be subscribed to and all. You can now optionally select only a subset to stream
- Optionally download timeseries data for a subset of data items from a unitop. Similar to streaming, you can donwload a subset of data items for a unitop
- You can choose whether to include timeseries/unitop or event/topic routes when declaring a hyprxa application
- Schema updates to events collection to support additional queries