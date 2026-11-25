# Remote Memory Access (RMA) over SerWB

First project layer on top of the imported LiteICLink code: one local node reads and writes a
memory window of one remote node through one SerWB link. Upstream files are not modified.

```
SoC bus -> RMAInitiator -> SERWBCore("slave") -> link -> SERWBCore("master") -> RMATarget -> memory
```

Code: `exiom/rma.py`. Simulation: `bench/rma/sim.py`. Tests: `test/test_rma*.py`.

## Why a layer above SerWB

SerWB already carries Wishbone reads and writes, reports remote errors on reads and returns an
error when the link is down. What it does not give for remote memory:

| Need | SerWB native protocol | RMA |
|------|-----------------------|-----|
| Write completion | Posted: acked locally, lost silently on a link error | Acked only once the remote side confirmed it |
| Write errors | Not reported | Reported, with the cause |
| Address window | None, any address reaches the remote bus | Fixed window, enforced by the Target |
| Remote memory not answering | Link stalled | Error after the Target watchdog |

## Interface

- 32-bit data, byte enables, one access at a time (no pipelining).
- `RMATarget(size, read_only=False, timeout=1024)`: `bus` to `SERWBCore(mode="master").bus`, `mem`
  to the memory (word addresses relative to the window). `timeout` (cycles) must stay below the
  SerWB `response_timeout` of the local side.
- `RMAInitiator(size, origin=0, link_ready=None)`: `bus` to the SoC (`origin` is the byte address
  of the window on this bus), `link` to `SERWBCore(mode="slave").bus`, `link_ready` to
  `phy.init.ready`.

Remote address map (word addresses on the SerWB bus):

| Address | Content |
|---------|---------|
| `0 .. size/4 - 1` | Memory window |
| `RMA_CTRL_BASE + 0` | ID (`RMA1`) |
| `RMA_CTRL_BASE + 1` | STATUS: `[0]` last write failed, `[1]` out of window, `[2]` read-only, `[3]` memory bus error, `[4]` memory timeout, `[31:8]` write count |
| `RMA_CTRL_BASE + 2` | Window size (words) |

## Semantics

- Read: ack with data, or bus error (out of window, memory error or timeout, link down, no response).
- Write: the Initiator sends the SerWB write, then reads STATUS. The access is acked only when the
  Target write count advanced by exactly one and no error flag is set. Otherwise it ends with a
  bus error and `error_code` gives the cause.
- A write error means "not confirmed". When the cause is `RMA_ERROR_LINK` (confirmation lost) the
  write may still have been applied; for `RMA_ERROR_RANGE`, `RMA_ERROR_RO` and `RMA_ERROR_LOST` it
  was not.
- After an error or a link loss, the Initiator reads the remote write count again before the next
  write, so a remote reset does not produce a false error.

| `error_code` | Cause |
|--------------|-------|
| `RMA_ERROR_RANGE` | Outside the window (local check, or refused by the Target) |
| `RMA_ERROR_RO` | Write to a read-only window |
| `RMA_ERROR_BUS` | Remote memory bus error |
| `RMA_ERROR_TIMEOUT` | Remote memory did not answer |
| `RMA_ERROR_LOST` | Write not executed by the Target (lost on the link) |
| `RMA_ERROR_LINK` | Link down, or no response from the remote side |
| `RMA_ERROR_READ` | Read error |

Cost: a confirmed write is one SerWB write plus one read round trip.

## Link recovery

A cut link is only recovered when the SerWB Master PHY is built with `with_auto_retrain=True`:
without it the Slave side resets on the idle line while the Master side stays "ready" and the link
never comes back. The simulation enables it.

## Verification

```sh
python3 -m pytest -v test/test_rma.py test/test_rma_bench.py test/test_rma_sim.py
```

- `test_rma.py`: Initiator and Target over a word-level PHY model (window, read-only, slow memory,
  memory timeout and error, lost write, lost confirmation, link down, remote reset).
- `test_rma_bench.py`: the two-node bench with the bit-level generic PHYs, Migen simulator.
- `test_rma_sim.py`: the same bench with Verilator (runs in CI, skipped without Verilator).

Scope: simulation only. No FPGA synthesis, timing closure or hardware test has been done, and the
generic SerWB PHY is not the target high-speed physical link.

## Not included yet

Pipelined or concurrent requests, bursts, more than two nodes, routing, DDR memory.
