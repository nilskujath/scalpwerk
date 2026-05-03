# scalpwerk

Fast-paced trading infrastructure for Python.

## For Developers

### Broker Implementation

Broker implementations must emit `OrderAccepted` before any `Fill` for the same order,
otherwise fills would arrive before the strategy's working order tracking has the entry to update.

### Logging

`scalpwerk` emits log records but does not configure handlers.
By default, `WARNING` and above print to stderr.
If a different behaviour should be wanted, logging needs to be configured before
calling `RunOrchestrator.run()`, e.g.: 

```python
import logging
logging.basicConfig(level=logging.INFO)
```
