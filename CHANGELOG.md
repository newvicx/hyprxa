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
