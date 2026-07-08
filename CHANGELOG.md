# CHANGELOG

<!-- version list -->

## v2.14.0 (2026-06-11)

### Features

- Integrated simulated broker to core.py
  ([`6443bbb`](https://github.com/nilskujath/scalpwerk/commit/6443bbb151fc00cfe9e74126b3adb7b8a975eee6))


## v2.13.0 (2026-06-09)

### Features

- SimulatedDatafeed made part of core.py
  ([`2f899cc`](https://github.com/nilskujath/scalpwerk/commit/2f899ccdfc3b732dbad7158d413168e900e44e80))


## v2.12.0 (2026-06-09)

### Features

- Added PickleRecorder to core.py
  ([`337c99a`](https://github.com/nilskujath/scalpwerk/commit/337c99a5c767a75785f798b9f057eacb362de332))


## v2.11.0 (2026-06-08)

### Features

- Split up StrategyBase
  ([`eceb9c7`](https://github.com/nilskujath/scalpwerk/commit/eceb9c753148888e5df741e557cfc56c6dbeec52))


## v2.10.0 (2026-05-30)

### Bug Fixes

- Raise on duplicate indicator names in add_input
  ([`ec592d0`](https://github.com/nilskujath/scalpwerk/commit/ec592d0c87dc21c188a29a8840969ffc32ec94fb))

### Features

- Added indicators, fixed some bugs
  ([`53db744`](https://github.com/nilskujath/scalpwerk/commit/53db7442abcb8fbfd412e7bfcf65e12ec3fd531e))


## v2.9.0 (2026-05-29)

### Features

- Extended charting with fills, position tracking, and bar styles
  ([`b98c0e1`](https://github.com/nilskujath/scalpwerk/commit/b98c0e1fb85d6b4737c86d628cbe642c7466f182))


## v2.8.0 (2026-05-28)

### Bug Fixes

- Ignore matplotlib missing imports in mypy config
  ([`cb4c5c3`](https://github.com/nilskujath/scalpwerk/commit/cb4c5c313688e2182d48792d1e08deee21eebe33))

### Features

- Added charting module with candlestick and indicator rendering
  ([`705f058`](https://github.com/nilskujath/scalpwerk/commit/705f058664037af655a9053181a6e410ddd06536))


## v2.7.0 (2026-05-27)

### Features

- Added indicator plot grouping and type aliases
  ([`e65a101`](https://github.com/nilskujath/scalpwerk/commit/e65a101cd03215aeeee39737b06ed490cd30c633))


## v2.6.0 (2026-05-27)

### Features

- Replaced JSONLRecorder with PickleRecorder
  ([`a76ac30`](https://github.com/nilskujath/scalpwerk/commit/a76ac30005c56dae54d6e94a9fe0b29971234f56))


## v2.5.0 (2026-05-27)

### Features

- Added PicklRecorder to recorders module
  ([`a122a40`](https://github.com/nilskujath/scalpwerk/commit/a122a40d0681d03f62cba18a70a3c58f9ec56d01))


## v2.4.0 (2026-05-27)

### Features

- Added recorders module
  ([`65bc937`](https://github.com/nilskujath/scalpwerk/commit/65bc93749f26198863989f32b172fcee12d5d104))


## v2.3.0 (2026-05-27)

### Features

- Added brokers module
  ([`9c6707e`](https://github.com/nilskujath/scalpwerk/commit/9c6707ee6e6002a7bedf518e5649596b3e07b312))


## v2.2.0 (2026-05-27)

### Features

- Added datafeeds module
  ([`2671fc2`](https://github.com/nilskujath/scalpwerk/commit/2671fc286d34f747cb81f6ae69b543f5e23df303))


## v2.1.0 (2026-05-26)

### Features

- Added indicators module
  ([`bf32f46`](https://github.com/nilskujath/scalpwerk/commit/bf32f4626142925994ff386c0aae8f77df62e310))


## v2.0.0 (2026-05-26)

### Bug Fixes

- Updated greenfield reimplementation
  ([`33e36f2`](https://github.com/nilskujath/scalpwerk/commit/33e36f2112b06ad9b8e60fb88c0ec36fb588220e))

- Updated greenfield reimplementation
  ([`62aeb24`](https://github.com/nilskujath/scalpwerk/commit/62aeb24cda844b26182ad04a4704bde51e9d276b))

### Features

- Add ATR-adaptive non-repainting swing detector
  ([`9f9b2cf`](https://github.com/nilskujath/scalpwerk/commit/9f9b2cfd14ae29c128946167b0eb285e1acd2f9c))

- Add Bollinger Band turnaround detector
  ([`1e86ebf`](https://github.com/nilskujath/scalpwerk/commit/1e86ebf70583c1804840968d0e70b3d8f0cb9a4b))

- Add Bollinger Bands, BoostedRSI, and Reverse RSI indicators
  ([`2a0c417`](https://github.com/nilskujath/scalpwerk/commit/2a0c41731cb8ffb31ea134d68f48ca1bea3dbd61))

- Add RSI indicator and fix NaN handling in recorder
  ([`08d4b9d`](https://github.com/nilskujath/scalpwerk/commit/08d4b9d5299c17d820ef92f559688e59f7b78318))

- Add SMA and ATR indicators, BarField enum, START/STOP datafeed filtering
  ([`ed7469c`](https://github.com/nilskujath/scalpwerk/commit/ed7469c2c3f5b0d055faef107adb309a4a0fe8b1))

- Added CSVDatafeedConnector and SMA indicator
  ([`5ec3dc2`](https://github.com/nilskujath/scalpwerk/commit/5ec3dc240c05cce8e295b970ca9f0ac071e46c87))

- Added JSONLRecorder
  ([`04faada`](https://github.com/nilskujath/scalpwerk/commit/04faada239a09e39afa60529be1125b713eeb238))

- Complete rework of core
  ([`3519fad`](https://github.com/nilskujath/scalpwerk/commit/3519fad0330d9def0241ca89bf816830ea7f14da))

- Completed simulated broker
  ([`eef8695`](https://github.com/nilskujath/scalpwerk/commit/eef8695b6e214de7ee25c10335d9def2f66c98b8))

- Implement SimulatedBroker with order matching, commissions, and position tracking
  ([`cc8746b`](https://github.com/nilskujath/scalpwerk/commit/cc8746bedfb31d4758083d217bae093440c52b97))

- Update, started Backtester class
  ([`d6839a5`](https://github.com/nilskujath/scalpwerk/commit/d6839a5dff23aeac077a1009750888d12db26eb5))

### Refactoring

- Strated greenfield reimplementation
  ([`516434f`](https://github.com/nilskujath/scalpwerk/commit/516434fde39d6ad09742029b4b6abf39ea998413))


## v1.8.0 (2026-05-07)

### Features

- CSVDatafeedConnector, wait_until_system_idle, subscribe-before-connect
  ([`71dfe84`](https://github.com/nilskujath/scalpwerk/commit/71dfe84455b448794667a08bce6c2a7eebe36461))

### Refactoring

- Clean up domain model and event comments
  ([`3fbc82d`](https://github.com/nilskujath/scalpwerk/commit/3fbc82d9d6e7095ff69a389761c5b8acf8ccec03))

- Concrete Orchestrator, trigger_shutdown, defensive runtime checks
  ([`25729e4`](https://github.com/nilskujath/scalpwerk/commit/25729e440415f9d47a18a83ec6d578e8d20296d5))

- Event hierarchy, default factory, strategy class variables
  ([`2d9fd4b`](https://github.com/nilskujath/scalpwerk/commit/2d9fd4b1ef66fda21984a571bc96a959e501930a))

- Major architecture overhaul of core.py
  ([`5b48da2`](https://github.com/nilskujath/scalpwerk/commit/5b48da20256e49fa177028efeebbb0d0589895f5))

- Multiple recorders, SQLiteRecorder, drop logging
  ([`d262f5a`](https://github.com/nilskujath/scalpwerk/commit/d262f5aaf5b71ccfe11ed4192c6335337217cc0e))

- Restructure domain model
  ([`314293f`](https://github.com/nilskujath/scalpwerk/commit/314293f4e01aaef4ecfc6bf997dd2610a3ec6c18))

- Simplify broker events, register TimeInForce, use cost basis
  ([`f785e5f`](https://github.com/nilskujath/scalpwerk/commit/f785e5f4daf116ee81e5b298c3914a3cba8a66dd))

- Streamline core architecture and extract recorder/orchestrator bases
  ([`6176535`](https://github.com/nilskujath/scalpwerk/commit/6176535785b2a0cc528c3086fca194853a073f6c))


## v1.7.0 (2026-04-04)

### Features

- Add library-style logging to core.py
  ([`80ebaaa`](https://github.com/nilskujath/scalpwerk/commit/80ebaaa52734024904a4a8dc591f91347c6b6649))


## v1.6.0 (2026-03-19)

### Features

- Implement RunOrchestrator lifecycle with on_fatal shutdown mechanism
  ([`aa58800`](https://github.com/nilskujath/scalpwerk/commit/aa588000b98b5c50c6f1d9e5326f76281e00c194))

### Refactoring

- Harden _RunRecorder with schema versioning, primary keys, and type safety
  ([`0d74adb`](https://github.com/nilskujath/scalpwerk/commit/0d74adbed43e6f571355835fa788745ef13e8445))


## v1.5.0 (2026-03-07)

### Features

- Add Orchestrator and _RunRecorder (both unfinished)
  ([`3040707`](https://github.com/nilskujath/scalpwerk/commit/3040707087c5338cbd51d893e081bc102ce0c887))

- Add Orchestrator and inject symbols/record_type into StrategyBase
  ([`820c3d5`](https://github.com/nilskujath/scalpwerk/commit/820c3d5f3ea9d338326928e43c79c08489afc6a4))

### Refactoring

- Add __all__ exports and prefix internal classes with _
  ([`40786c2`](https://github.com/nilskujath/scalpwerk/commit/40786c272b5f71b3b3c3695753d957179c863cc1))

- Add domain-specific NewTypes, condense comments, and harden type safety
  ([`57c2cf9`](https://github.com/nilskujath/scalpwerk/commit/57c2cf99e4c8206a263321d83e0fd7b1b12bbe56))

- Move SQLiteRunRecorder out of core.py
  ([`ee06b25`](https://github.com/nilskujath/scalpwerk/commit/ee06b25d7f2823ecdc58c3b6acea7f516a6e2a29))


## v1.4.0 (2026-03-03)

### Features

- Add DatafeedBase component with subscribe/unsubscribe interface
  ([`af159f0`](https://github.com/nilskujath/scalpwerk/commit/af159f037dd53fd94c0ccb3ddb4082fb483dd25c))

### Refactoring

- Split SubscriberBase into ComponentBase, SubscriberBase, and EmitterBase
  ([`bfee4c6`](https://github.com/nilskujath/scalpwerk/commit/bfee4c63e3694d5a8346613f50ae3c3827750e65))


## v1.3.0 (2026-03-02)

### Documentation

- Mention domain models in file structure comment
  ([`dd6b693`](https://github.com/nilskujath/scalpwerk/commit/dd6b6934ef17d58458efaeedeb85f753fd93b7c5))

### Features

- Add ExternalComponentMixin for components that interface with external systems
  ([`c1427bb`](https://github.com/nilskujath/scalpwerk/commit/c1427bbed3136a4f10350c2c9b5a88c58d379cb9))


## v1.2.0 (2026-03-02)

### Chores

- Add .claude/ to gitignore
  ([`4b139a7`](https://github.com/nilskujath/scalpwerk/commit/4b139a7fd6f4b58bcb3489ce797f1715274b8a23))

### Features

- Add BrokerBase component with generic emittable event types
  ([`0566f00`](https://github.com/nilskujath/scalpwerk/commit/0566f00145666332cc5f2caafee06482eace4fc3))

### Refactoring

- Consolidate models, events, and messaging into core module
  ([`9a943ee`](https://github.com/nilskujath/scalpwerk/commit/9a943ee7e4ae4c36fc3cacecfec570c5ab57e5ba))


## v1.1.0 (2026-03-01)

### Documentation

- Add comments to Subscriber class explaining threading decisions
  ([`e1585fb`](https://github.com/nilskujath/scalpwerk/commit/e1585fb6e482f10bc76cf2897f696f5d531f0c23))

### Features

- Add messaging module with EventBus and Subscriber
  ([`e3dde62`](https://github.com/nilskujath/scalpwerk/commit/e3dde6226d25293dcf4740462280de94131ed5f2))

### Refactoring

- Flatten event hierarchy and simplify broker ID types
  ([`0ea48d4`](https://github.com/nilskujath/scalpwerk/commit/0ea48d40efd0f5f7587eb142816cc6f633c2e2f3))


## v1.0.0 (2026-02-28)

- Initial Release
