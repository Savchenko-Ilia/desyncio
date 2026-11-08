# desyncio

Discrete-event simulation that runs on asyncio itself. `desyncio` replaces the clock
under the event loop with a virtual one, so `asyncio.sleep`, `Queue`, `Semaphore`,
`TaskGroup` and `asyncio.timeout()` work in simulated time. Simulations run as fast
as your CPU allows, or scaled to real time. The same seed always gives the same run.

> **Status:** early development. The example below is the target API.

```python
import asyncio
import desyncio

async def ship(berth: asyncio.Semaphore, name: str) -> None:
    async with berth:
        await asyncio.sleep(12)  # 12 virtual hours, returns instantly
        print(f"{name} leaves at t={asyncio.get_running_loop().time()}")

async def main() -> None:
    berth = asyncio.Semaphore(1)
    async with asyncio.TaskGroup() as tg:
        for i in range(3):
            tg.create_task(ship(berth, f"ship-{i}"))

desyncio.run(main())  # ship-0 at 12, ship-1 at 24, ship-2 at 36
```

## Goals

- Plain `async def` processes on a real asyncio event loop
- Deterministic: seeded random streams and explicit ordering of simultaneous events
- Fast-as-possible and scaled real-time modes
- Classic DES primitives: resources, containers, stores, interrupts

## Documentation

- [How the loop works](docs/how-the-loop-works.md): how asyncio's `_run_once`, timer
  heap and selector are turned into a virtual-time loop, with a traced worked example.

## License

Apache-2.0
