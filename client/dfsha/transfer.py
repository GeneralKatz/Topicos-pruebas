"""Transferencia de archivos: put y get (bloque D.3).

Aquí se ve el diseño completo del sistema en dos funciones: el ControlNode decide
el esquema y los DataNodes mueven los bytes.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from .api import ControlClient, DataClient, DFShaError
from .chunker import file_size, sha256_of_bytes, sha256_of_file, split_file


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:.1f} TB"


def put(
    control: ControlClient,
    local_path: str,
    remote_path: str,
    *,
    data_client: DataClient | None = None,
    verbose: bool = True,
    write_quorum: int = 2,
) -> dict:
    """Sube un archivo: open -> (allocate + subir) por chunk -> commit.

    El orden no es negociable. El commit va al final porque hasta ese momento nada
    es visible: un lector concurrente ve la versión anterior íntegra, y si el
    cliente muere a mitad, el objeto anterior sigue intacto (RT-07, RT-09).
    """
    data_client = data_client or DataClient()

    total_size = file_size(local_path)
    # El hash del archivo completo se calcula antes de subir nada, leyendo el
    # archivo por bloques. Es lo que el servidor guardará como verdad de la versión
    # y contra lo que se verificará el archivo recuperado (RT-11).
    total_sha = sha256_of_file(local_path)

    # La clave de idempotencia hace que un reintento tras un timeout devuelva la
    # misma carga en lugar de abrir otra y duplicar la versión (RT-08).
    session = control.open_upload(remote_path, total_size, idempotency_key=str(uuid.uuid4()))
    upload_id = session["upload_id"]
    chunk_size = int(session["chunk_size"])

    if verbose:
        print(f"put {local_path} -> {remote_path}")
        print(f"  tamaño      : {_human(total_size)} ({total_size} bytes)")
        print(f"  sha256      : {total_sha}")
        print(f"  chunk_size  : {_human(chunk_size)}")
        print(f"  chunks      : {session['expected_chunks']}")
        print(f"  upload_id   : {upload_id}")
        print(f"  version_id  : {session['version_id']}")
        print()

    started = time.time()
    uploaded = 0
    # Por chunk_id, los nodos que confirmaron haberlo guardado. Se entrega al commit
    # para que el ControlNode registre solo réplicas confirmadas (RT-06).
    confirmed: dict[str, list[str]] = {}
    # Destinos que no respondieron durante ESTA subida. Una vez que uno falla por red no
    # se vuelve a esperar por él en los chunks siguientes: se cuenta como sin ACK y el
    # quórum se decide con el resto. Lo que quede con menos de R copias lo completará
    # la re-replicación. Un nodo que reviva a mitad de la subida no se reintenta.
    dead_targets: set[str] = set()
    try:
        for index, data in split_file(local_path, chunk_size):
            checksum = sha256_of_bytes(data)

            # El cliente pregunta DÓNDE escribir; nunca lo decide (RT-03).
            plan = control.allocate_chunk(upload_id, index, len(data), checksum)
            targets = plan["targets"]
            if not targets:
                raise DFShaError(503, f"el ControlNode no asignó destino para el chunk {index}")

            t0 = time.time()
            # Se escribe en cada destino y se anota QUIÉN confirmó. El ACK es la
            # respuesta 201 del DataNode, que solo la da tras verificar el checksum y
            # forzar el chunk a disco. Si el ACK se pierde, put_chunk reintenta con el
            # mismo chunk_id y el DataNode sobrescribe: el reintento es inofensivo.
            # Un destino caído no aborta la carga mientras se alcance el quórum.
            # (La replicación en pipeline, RT-05, es la etapa siguiente.)
            acked: list[str] = []
            failures: list[str] = []
            for target in targets:
                node = target["node_id"]
                if node in dead_targets:
                    failures.append(f"{node}: sin respuesta en esta subida")
                    continue
                try:
                    # Dos intentos bastan: reintentar un nodo caído solo retrasa la
                    # carga, y el quórum se completa con las demás réplicas.
                    data_client.put_chunk(target["endpoint"], plan["chunk_id"], data, checksum, attempts=2)
                    acked.append(node)
                except DFShaError as e:
                    failures.append(f"{node}: {e.detail}")
                    if e.status == 0:
                        dead_targets.add(node)  # fallo de red: el nodo no responde

            need = min(write_quorum, len(targets))
            if len(acked) < need:
                raise DFShaError(
                    503,
                    f"chunk {index}: {len(acked)} confirmaciones y el quórum exige {need} "
                    f"({'; '.join(failures)})",
                    retryable=True,
                )
            confirmed[plan["chunk_id"]] = acked
            elapsed = time.time() - t0

            uploaded += len(data)
            if verbose:
                # Registro visible del particionamiento y la transferencia: qué
                # chunk, de qué tamaño, hacia qué DataNode (RC-08).
                destinos = ", ".join(f"{t['node_id']}@{t['endpoint']}" for t in targets)
                marca = "  [DEGRADADA]" if plan.get("degraded") else ""
                if failures:
                    marca += f"  [SIN ACK: {', '.join(f.split(':')[0] for f in failures)}]"
                print(
                    f"  chunk {index:>4} | {_human(len(data)):>9} | {destinos}"
                    f" | {elapsed:.2f}s | {checksum[:12]}…{marca}"
                )

        result = control.commit(upload_id, total_sha, confirmed)
    except Exception:
        # Abortar libera el lease y marca los chunks escritos para recolección. Sin
        # esto, la ruta quedaría bloqueada hasta que venciera el lease (RT-09).
        try:
            control.abort(upload_id)
            if verbose:
                print("\n  carga abortada: el objeto anterior permanece intacto", file=sys.stderr)
        except DFShaError:
            pass
        raise

    if verbose:
        total = time.time() - started
        rate = uploaded / total if total > 0 else 0
        print(f"\n  commit ok | version={result['version_id']} | {result['chunks']} chunks"
              f" | {_human(uploaded)} en {total:.1f}s ({_human(rate)}/s)")
    return result


def get(
    control: ControlClient,
    remote_path: str,
    local_path: str,
    *,
    workers: int = 4,
    data_client: DataClient | None = None,
    verbose: bool = True,
) -> dict:
    """Descarga un archivo: manifiesto -> descargas en paralelo -> verificación.

    Las descargas van en paralelo y la escritura en orden, y eso plantea la
    pregunta de dónde guardar un chunk que llega antes de que le toque. La respuesta
    de esta implementación es NO GUARDARLO: el archivo destino se preasigna con su
    tamaño final y cada hilo escribe su chunk directamente en el desplazamiento que
    le corresponde, con os.pwrite.

    Así el consumo de memoria es (hilos × chunk_size) y no (número de chunks ×
    chunk_size): un archivo de 15 GB con 240 chunks se descarga con 4 hilos usando
    256 MB de RAM, no 15 GB (RI-04, RT-10).
    """
    data_client = data_client or DataClient()

    manifest = control.manifest(remote_path)
    chunks = sorted(manifest["chunks"], key=lambda c: c["index"])

    # Desplazamientos acumulados a partir de los tamaños reales del manifiesto. No se
    # calculan como index * chunk_size porque el último chunk es más corto y porque
    # así el reensamblado no depende de que todos midan lo mismo (RT-02).
    offsets: dict[int, int] = {}
    cursor = 0
    for c in chunks:
        offsets[c["index"]] = cursor
        cursor += int(c["size"])
    total_size = cursor

    if verbose:
        print(f"get {remote_path} -> {local_path}")
        print(f"  version_id  : {manifest['version_id']}")
        print(f"  tamaño      : {_human(total_size)} ({total_size} bytes)")
        print(f"  chunks      : {len(chunks)} | hilos: {workers}")
        print(f"  sha256 esp. : {manifest['sha256']}")
        print()

    started = time.time()
    # O_BINARY solo existe en Windows, donde os.open abre en modo texto por omisión y
    # traduciría saltos de línea, corrompiendo el archivo. En Linux y macOS no existe.
    flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC | getattr(os, "O_BINARY", 0)
    fd = os.open(local_path, flags)
    # os.pwrite no existe en Windows. Allí se posiciona y escribe bajo un candado, para
    # que dos hilos no intercalen el desplazamiento y la escritura.
    write_lock = threading.Lock()
    # Endpoints que no respondieron durante ESTA descarga. Se comparte entre los hilos
    # para no volver a esperar por un nodo caído en cada chunk (RQ-26).
    dead_endpoints: set[str] = set()
    try:
        # Preasignar el archivo evita que los hilos compitan por extenderlo.
        os.ftruncate(fd, total_size)

        def fetch(chunk: dict) -> tuple[int, int, float, str]:
            t0 = time.time()
            data, endpoint = _download_with_failover(data_client, chunk, dead_endpoints)
            if hasattr(os, "pwrite"):
                os.pwrite(fd, data, offsets[chunk["index"]])
            else:
                with write_lock:
                    os.lseek(fd, offsets[chunk["index"]], os.SEEK_SET)
                    os.write(fd, data)
            return chunk["index"], len(data), time.time() - t0, endpoint

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch, c): c for c in chunks}
            for future in as_completed(futures):
                index, size, elapsed, endpoint = future.result()
                if verbose:
                    print(f"  chunk {index:>4} | {_human(size):>9} | {endpoint} | {elapsed:.2f}s")
    finally:
        os.close(fd)

    # Verificación extremo a extremo: el hash del archivo reensamblado debe coincidir
    # con el que se registró al subirlo. Los checksums por chunk ya garantizaron que
    # cada pieza llegó intacta; este atrapa un error en el REENSAMBLADO —un chunk
    # omitido, uno escrito en el desplazamiento equivocado— que los otros no ven
    # (RT-11, criterio E.2.4).
    actual = sha256_of_file(local_path)
    if actual != manifest["sha256"]:
        raise DFShaError(
            422,
            f"integridad fallida: el archivo recuperado tiene sha256 {actual}, "
            f"se esperaba {manifest['sha256']}",
        )

    total = time.time() - started
    if verbose:
        rate = total_size / total if total > 0 else 0
        print(f"\n  ok | sha256 verificado | {_human(total_size)} en {total:.1f}s ({_human(rate)}/s)")
    return {"path": remote_path, "size": total_size, "sha256": actual, "chunks": len(chunks)}


def _download_with_failover(
    data_client: DataClient, chunk: dict, dead: set[str] | None = None
) -> tuple[bytes, str]:
    """Descarga un chunk probando sus réplicas hasta que una responda.

    La caída de una réplica no interrumpe la operación y el reintento contra otra es
    invisible para el usuario (RQ-26, RC-10). Cada réplica se verifica contra el
    checksum del manifiesto ANTES de aceptarla: un chunk corrupto se detecta antes de
    escribirlo y se reintenta contra otra copia (RT-13).

    Para que el failover sea rápido:
      1. Primera pasada: UN solo intento por réplica. Si un nodo está caído, esperar
         su retroceso exponencial (varios segundos) es peor que probar la siguiente.
      2. Los nodos que ya fallaron por red (`dead`, compartido entre chunks) se dejan
         para el final, así los chunks siguientes no vuelven a esperar por ellos.
      3. Segunda pasada, solo si NINGUNA réplica sirvió: se reintenta con retroceso
         las que fallaron de forma transitoria. Cubre el caso de una única réplica con
         un fallo momentáneo.
    """
    endpoints = list(chunk.get("endpoints") or [])
    if not endpoints:
        raise DFShaError(503, f"el chunk {chunk['chunk_id']} no tiene ninguna réplica viva")

    dead = dead if dead is not None else set()
    ordered = [e for e in endpoints if e not in dead] + [e for e in endpoints if e in dead]

    errors: list[str] = []
    retry_later: list[str] = []

    def try_replica(endpoint: str, attempts: int) -> bytes | None:
        try:
            data, reported = data_client.get_chunk(endpoint, chunk["chunk_id"], attempts=attempts)
        except DFShaError as e:
            errors.append(f"{endpoint}: {e.detail}")
            if e.status == 0:
                dead.add(endpoint)  # fallo de red: el nodo no responde
            if e.retryable and endpoint not in retry_later:
                retry_later.append(endpoint)
            return None

        dead.discard(endpoint)  # respondió, así que no está caído

        expected = chunk.get("checksum") or reported
        if expected:
            got = sha256_of_bytes(data)
            if got != expected:
                errors.append(f"{endpoint}: checksum {got} != {expected}")
                return None
        if len(data) != int(chunk["size"]):
            errors.append(f"{endpoint}: {len(data)} bytes, se esperaban {chunk['size']}")
            return None
        return data

    for endpoint in ordered:
        data = try_replica(endpoint, 1)
        if data is not None:
            return data, endpoint

    for endpoint in list(retry_later):
        data = try_replica(endpoint, 4)
        if data is not None:
            return data, endpoint

    raise DFShaError(502, f"todas las réplicas del chunk {chunk['chunk_id']} fallaron: {'; '.join(errors)}")
