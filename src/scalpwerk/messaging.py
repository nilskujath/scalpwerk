from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from queue import Queue

import threading
import typing


# The base class for events is defined here so that the messaging system's contracts
# remain self-contained within this module with no external dependencies.
# Defining it as a protocol would not have worked here because `type[Protocol]` isn't
# well-supported by type checkers, and we need `type[EventBase]` for the subscription
# dict keys and for event types as arguments.
@dataclass(kw_only=True, frozen=True, slots=True)
class EventBase:
    occurred_at_ns: int
    created_at_ns: int


# The SubscriberLike protocol explicitly documents which methods the EventBus
# requires from its subscribers. This is necessary because EventBus is defined
# before Subscriber, so it cannot reference Subscriber directly in its type
# annotations.
# While `from __future__ import annotations` would also solve the forward
# reference, the protocol makes the contract immediately visible and a reader
# can see exactly which methods the EventBus depends on without having to
# read through the full Subscriber class.
class SubscriberLike(typing.Protocol):
    def wait_until_idle(self) -> None: ...
    @property
    def is_idle(self) -> bool: ...
    def receive(self, event: EventBase) -> None: ...


class EventBus:
    def __init__(self) -> None:
        # Using a set as the defaultdict's value avoids needing additional logic to
        # guard against duplicate subscriptions in the `add_event_subscription` method.
        self._per_event_subscriptions: defaultdict[
            type[EventBase], set[SubscriberLike]
        ] = defaultdict(set)

        # Because each subscriber in `_per_event_subscriptions` runs in its own thread
        # we need to ensure that `_per_event_subscriptions` can only be accessed from
        # one subscriber thread at a time, for which we need a Lock.
        self._lock: threading.Lock = threading.Lock()

    def add_event_subscription(
        self, subscriber: SubscriberLike, event_type: type[EventBase]
    ) -> None:
        with self._lock:
            self._per_event_subscriptions[event_type].add(subscriber)

    def remove_subscriber(self, subscriber: SubscriberLike) -> None:
        with self._lock:
            for subscriber_set in self._per_event_subscriptions.values():
                subscriber_set.discard(subscriber)

    def publish_event_to_system(self, event: EventBase) -> None:
        # Since the `publish_event_to_system` method is accessed from within the
        # subscriber threads concurrently, we want to minimize the time the Lock is
        # held by each thread so publishing can be done as concurrently as possible
        # between threads. This is why we only copy the set of subscribers for the
        # event type within the Lock and do the rest outside the Lock.
        with self._lock:
            subscribers = self._per_event_subscriptions[type(event)].copy()
        for subscriber in subscribers:
            subscriber.receive(event)

    def wait_until_system_idle(self) -> None:
        # To run backtesting at the maximum possible speed, we want to feed a new piece
        # of market data as soon as the current piece of market data has been completely
        # processed by the system. This is the case when every item that has been put
        # into any of subscribers' internal queue is marked as done via `task_done()`.
        # However, calling each subscriber's `wait_until_idle()` method just once is not
        # sufficient because a subscriber processing its events can publish new events
        # into queues of those subscribers that we have already checked for idleness.
        # This is why after waiting for all subscribers' queues to drain, we need to
        # take a fresh snapshot to verify that every subscriber is still idle.
        # If any subscriber should have become active again, we repeat the process until
        # all of them are truly idle, which is why we need the `while True` loop.
        while True:
            # We only access `_per_event_subscriptions` within the Lock and do the
            # actual waiting outside of it because a subscriber processing its events
            # may publish new events via `publish_event_to_system`, which also needs
            # the Lock.
            # Holding the Lock while waiting would deadlock: the subscriber
            # can't finish processing because it's blocked trying to publish, and we
            # can't stop waiting because the subscriber isn't done.
            with self._lock:
                # The `set().union(*)` deduplicates subscribers that appear under
                # multiple event types so we don't wait on the same subscriber twice.
                subscribers = set().union(*self._per_event_subscriptions.values())
            for subscriber in subscribers:
                subscriber.wait_until_idle()

            with self._lock:
                subscribers = set().union(*self._per_event_subscriptions.values())
            if all(subscriber.is_idle for subscriber in subscribers):
                break


class Subscriber(ABC):
    def __init__(self, event_bus: EventBus) -> None:
        # The `EventBus` is injected rather than used as a hard-coded global instance so
        # that in principle, multiple independent systems could co-exist within the same
        # runtime.
        self._event_bus: EventBus = event_bus

        # `None` in the type union acts as a poison pill: when the event loop receives
        # `None`, it knows to shut down. See `shutdown` and `_event_loop` methods.
        self._queue: Queue[EventBase | None] = Queue()

        # `threading.Event` is used instead of e.g. a boolean flag because it is
        # thread-safe and can be checked from any other thread without needing a Lock.
        # We set it before starting the thread so that `receive` can accept events from
        # the moment the thread is alive. Otherwise, the `_running` check in `receive`
        # would fail and events would be silently dropped.
        self._running: threading.Event = threading.Event()
        self._running.set()
        self._thread = threading.Thread(
            target=self._event_loop, name=self.__class__.__name__
        )
        self._thread.start()

    def wait_until_idle(self) -> None:
        # After shutdown, the event loop thread has exited and no longer calls
        # `task_done()`. If an event was placed on the queue right before shutdown,
        # `join()` would block forever waiting for it to be processed. Since a
        # subscriber is idle per definition after shutdown, the method simply returns.
        if not self._running.is_set():
            return
        self._queue.join()

    # The `wait_until_idle` method blocks until the subscriber's queue is joined. This
    # method is a non-blocking (we do not wait for the queue to be empty) check whether
    # the queue is idle.
    @property
    def is_idle(self) -> bool:
        return self._queue.unfinished_tasks == 0

    def receive(self, event: EventBase) -> None:
        if self._running.is_set():
            self._queue.put(event)

    def shutdown(self):
        if not self._running.is_set():
            return
        self._event_bus.remove_subscriber(self)

        # We clear `_running` before putting the poison pill on the queue so that
        # `receive` stops accepting new events before the event loop exits.
        # Otherwise, the event loop could process the poison pill and exit while
        # `receive` is still queuing events that would never be processed.
        self._running.clear()
        self._queue.put(None)

        # A subscriber's `_on_event` handler might call `shutdown()` on itself, in which
        # case we are already inside the subscriber's thread. Calling `join()` on the
        # current thread would deadlock because a thread cannot join itself. This is an
        # explicit guard against such implementation errors.
        if threading.current_thread() is not self._thread:
            self._thread.join()

    def _subscribe_to_events(self, *event_types: type[EventBase]):
        for event_type in event_types:
            self._event_bus.add_event_subscription(self, event_type)

    def _event_loop(self):
        while True:
            event = self._queue.get()

            if event is None:
                # We still need to call `task_done()` for the poison pill itself,
                # otherwise `wait_until_idle` would block forever on `queue.join()`
                # waiting for this unfinished task.
                self._queue.task_done()
                break

            # Subscribers are expected to handle their own exceptions within
            # `_on_event`. This try/except is a last safety net so that an unhandled
            # exception does not crash the subscriber's thread. The exception is
            # delegated to `_on_exception` and the loop continues processing the next
            # event. `task_done()` is in `finally` so the queue's unfinished task count
            # stays correct even when the handler raises.
            try:
                self._on_event(event)
            except Exception as exc:
                self._on_exception(exc)
            finally:
                self._queue.task_done()

    @abstractmethod
    def _on_event(self, event: EventBase):
        pass

    def _on_exception(self, exception: Exception):
        pass
