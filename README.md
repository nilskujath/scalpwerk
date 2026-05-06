# scalpwerk

Fast-paced trading infrastructure for Python.

## For Developers

### Broker Implementation

Broker implementations must emit `OrderAccepted` before any `Fill` for the same order,
otherwise fills would arrive before the strategy's working order tracking has the entry to update.
