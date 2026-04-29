# scalpwerk

Fast-paced trading infrastructure for Python.

## For Developers

### Logging

`scalpwerk` emits log records but does not configure handlers.
By default, `WARNING` and above print to stderr.
If a different behaviour should be wanted, logging needs to be configured before
calling `RunOrchestrator.run()`, e.g.: 

```python
import logging
logging.basicConfig(level=logging.INFO)
```
