# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 0.0.1 (21st Feb, 2023)

- Initial release
- The hyprxa API is unlikely to change but the project will remain in alpha until code coverage testing and documentation are complete

## 0.0.2 (22nd Feb, 2023)

- Exposed much more the hyprxa internals at the top level of modules
- Added `Hyprxa` class which inherits from `FastAPI` and handles much of the boilerplate required for a hyprxa application
- `add_source` now accepts a callable that returns a `BaseIntegration`
- Unfied all exceptions and exception handlers under the `exceptions` module
- Minor bug fixes