# docker daemon 
- problem type: network controller
- missing crucial kernel module = addrtype2 
  - docker needs it to inject it NAT rules to iptables

-  Linux System Troubleshooting Summary
## 🐛 Problem & Type
- Problem: Docker failed to start post-system upgrade, specifically failing with network/storage driver errors (e.g., vfs or network bridge issues).
- Problem: Docker failed to start following a system upgrade (yay -Syu)
  - manifesting specifically with network errors due to a missing dependency/module reference (addrtype2), preventing bridge creation or storage driver initialization

- Type: Kernel/Network Module Incompatibility. 
  - The issue was a mismatch between the running kernel's modules (Netfilter/iptables) and the new binaries (containerd, Docker service) installed during the system-wide upgrade (yay -Syu).

## diagnostic & cli-tools
- Goal: debug kernel and pacman loggings of the most recent update 
  - Locate the time and package responsible for the system failure.

- Tools needed & used:
fd: fast, regex-enabled file searching, for initial search for pacman.log
  - fd: [pattern] [dir] [OPTIONS]
rg / grep: Used heavily for content filtering of the large log file (/var/log/pacman.log).
Piping (|): Fundamental concept used to chain commands (e.g., cat ... | rg 'pattern').
cat-bat-less: to read the log file content with necessary privileges.

- Key Finding: Logs confirmed a full system upgrade ran at 09:04 on 2025-11-10, followed by the update of the linux kernel package from 6.17.5 to 6.17.7.
  - this update must probably causes *issue dependencies* between docker daemon & kernel modules <
## solution 
- Action: System Reboot (sudo reboot).
- Reasoning: A system restart forces the computer to unload the old kernel and load the new kernel (6.17.7) along with its corresponding, compatible kernel modules. This resolves the conflict with Docker's networking and storage drivers.
# schema.sql 
- lack of SK, lack of metadata attributes/columns for SCD type 2 application 
- symbol UNIQUE CONSTRAINT; doesn't allows an UPDATE if the symbol changes ('TWTR' to 'X' e.g.)
- main problem: the schema don't allow the historical tracking of changes and updates (SCD type 2!)
## facts 
- facts are **immutable**; every *batch* that passed for a ETL/ELT pipeline process, is a snapshot of that data
- the pipeline *freezes* new facts of events in time, while update/expand the dimensions, for reflect the evolution of the business
  - wouldn't NEVER be updated; at less in a wrong data correction
- a good *data warehouse* allows travel in time for any *historical analysis* 
## ia feedback for interview about this project 
- "La granularidad se refiere al nivel de descomposición de los datos. Los datos de grano fino representan eventos atómicos que no se pueden descomponer más sin perder significado de negocio. Los datos de grano grueso son agregaciones que sí se pueden descomponer en componentes más finos."

- "En mi proyecto de stocks, la granularidad es diaria por acción, lo cual considero grano fino para análisis de tendencias a mediano plazo, pero sería grano grueso si tuviera acceso a datos intradía."
## granularity meta-cognition 
- La prueba definitiva: ¿Puedo trazar una línea temporal donde los eventos hijos ocurrieron antes y contribuyeron a los eventos padre?

✅ Sales → Daily Sales → Monthly Sales = SÍ hay relación temporal de composición

❌ Clicks → Sales = NO hay relación directa de composición (son eventos diferentes)
