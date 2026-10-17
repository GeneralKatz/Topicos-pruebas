"""SDK de acceso a DFSha (bloque D.2).

Dos clientes: uno contra el ControlNode (REST, metadatos) y otro contra los
DataNodes (HTTP con flujo, bytes). Son independientes de la consola: un programa
externo puede importar este módulo y operar sin usar la CLI (RC-12).

Solo biblioteca estándar, a propósito: el cliente debe funcionar en cualquier
máquina con Python sin instalar nada.
"""

from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# Métodos y códigos del catálogo de errores (RI-05).
RETRY_STATUSES = {500, 502, 503, 504, 507}


class DFShaError(Exception):
    """Error devuelto por el servicio, con su código y si es reintentable."""

    def __init__(self, status: int, detail: str, retryable: bool = False):
        super().__init__(f"HTTP {status}: {detail}")
        self.status = status
        self.detail = detail
        self.retryable = retryable


class ConflictError(DFShaError):
    """409: otro cliente tiene una carga en curso sobre la misma clave.

    Existe como clase aparte porque NUNCA se reintenta automáticamente (RC-11): el
    estado del servidor no va a cambiar por sí solo y reintentar solo produciría el
    mismo conflicto o una versión duplicada.
    """


def _request(
    method: str,
    url: str,
    *,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
    attempts: int = 4,
) -> tuple[int, dict[str, str], bytes]:
    """Ejecuta una petición HTTP con retroceso exponencial ante fallos transitorios.

    El retroceso lleva un componente aleatorio (*jitter*) para que veinte clientes
    que fallan a la vez no reintenten todos en el mismo instante y vuelvan a
    tumbar el servicio (RC-11, RQ-01).
    """
    headers = dict(headers or {})
    last: Exception | None = None

    for attempt in range(attempts):
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            payload = e.read()
            detail, retryable = _parse_error(payload, e.code)
            if e.code == 409:
                raise ConflictError(e.code, detail) from e
            if e.code not in RETRY_STATUSES and not retryable:
                raise DFShaError(e.code, detail, retryable=False) from e
            last = DFShaError(e.code, detail, retryable=True)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            # Fallo de red: reintentable por definición, la petición pudo no llegar.
            last = DFShaError(0, f"{type(e).__name__}: {e}", retryable=True)

        if attempt < attempts - 1:
            delay = (2**attempt) * 0.25 + random.uniform(0, 0.25)
            time.sleep(delay)

    raise last if last else DFShaError(0, "petición fallida sin causa registrada")


def _parse_error(payload: bytes, status: int) -> tuple[str, bool]:
    try:
        data = json.loads(payload)
        return str(data.get("detail", payload.decode(errors="replace"))), bool(data.get("retryable", False))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return payload.decode(errors="replace"), status in RETRY_STATUSES


class ControlClient:
    """Cliente del plano de control.

    Es lo ÚNICO que el cliente conoce por configuración. Las direcciones de los
    DataNodes nunca están en un archivo: llegan en las respuestas de este servicio,
    en tiempo de ejecución (RC-09, RX-14, RQ-25).
    """

    def __init__(self, base_url: str, user: str = "anonymous", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout

    # -- utilidades internas ------------------------------------------------

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {"X-DFSha-User": self.user, "Accept": "application/json"}
        if extra:
            headers.update(extra)
        return headers

    def _json(self, method: str, path: str, payload: Any = None, extra: dict[str, str] | None = None) -> Any:
        body = None
        headers = self._headers(extra)
        if payload is not None:
            body = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        _, _, data = _request(method, f"{self.base_url}{path}", body=body, headers=headers, timeout=self.timeout)
        return json.loads(data) if data else None

    @staticmethod
    def _q(path: str) -> str:
        return urllib.parse.urlencode({"path": path})

    # -- escritura ----------------------------------------------------------

    def open_upload(self, path: str, size: int, idempotency_key: str | None = None) -> dict:
        """Abre una carga. Devuelve upload_id, version_id y el tamaño de chunk."""
        extra = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        return self._json("POST", "/v1/obj/open", {"path": path, "size": size}, extra)

    def allocate_chunk(self, upload_id: str, index: int, size: int, checksum: str) -> dict:
        """Pide el destino del chunk `index`. El ControlNode decide, el cliente obedece."""
        return self._json(
            "POST",
            f"/v1/obj/{urllib.parse.quote(upload_id)}/chunk",
            {"index": index, "size": size, "checksum": checksum},
        )

    def commit(self, upload_id: str, sha256: str, confirmed: dict[str, list[str]] | None = None) -> dict:
        """Publica la versión. Es la última operación y es atómica (RT-07).

        `confirmed` mapea cada chunk_id a los node_id que confirmaron (ACK) haberlo
        guardado. El ControlNode registra como réplicas solo esos nodos y exige el
        quórum de escritura por chunk (RT-06). Si se omite, asume todos los asignados.
        """
        body: dict[str, Any] = {"sha256": sha256}
        if confirmed is not None:
            body["confirmed"] = confirmed
        return self._json("POST", f"/v1/obj/{urllib.parse.quote(upload_id)}/commit", body)

    def abort(self, upload_id: str) -> dict:
        """Cancela la carga y libera el lease (RT-09)."""
        return self._json("DELETE", f"/v1/obj/{urllib.parse.quote(upload_id)}")

    # -- lectura ------------------------------------------------------------

    def manifest(self, path: str) -> dict:
        """Lista ordenada de chunks con las direcciones vivas de cada réplica."""
        return self._json("GET", f"/v1/obj/manifest?{self._q(path)}")

    # -- namespace ----------------------------------------------------------

    def list(self, path: str) -> dict:
        return self._json("GET", f"/v1/ns?{self._q(path)}")

    def stat(self, path: str) -> dict:
        return self._json("GET", f"/v1/ns/stat?{self._q(path)}")

    def mkdir(self, path: str) -> dict:
        return self._json("POST", "/v1/ns/dir", {"path": path})

    def remove(self, path: str) -> dict:
        return self._json("DELETE", f"/v1/ns?{self._q(path)}")

    def rename(self, src: str, dst: str) -> dict:
        return self._json("POST", "/v1/ns/rename", {"from": src, "to": dst})

    # -- diagnóstico --------------------------------------------------------

    def nodes(self) -> dict:
        return self._json("GET", "/v1/nodes")

    def df(self) -> dict:
        return self._json("GET", "/v1/df")


class DataClient:
    """Cliente del plano de datos. Habla directo con los DataNodes.

    Los bytes NUNCA pasan por el ControlNode (RQ-13): esta clase es la que mueve el
    archivo, y solo conoce direcciones que le entregó el manifiesto.
    """

    # Velocidad mínima que se asume al escribir un chunk. Sirve para acotar el timeout:
    # un chunk de 4 MB no necesita esperar 120 s a un nodo que no responde, mientras que
    # uno de 64 MB por un enlace lento sí necesita tiempo. Es un tope, no una promesa.
    MIN_WRITE_RATE = 256 * 1024  # bytes por segundo

    def __init__(self, timeout: float = 120.0):
        self.timeout = timeout

    def _write_timeout(self, size: int) -> float:
        return min(self.timeout, max(10.0, size / self.MIN_WRITE_RATE))

    @staticmethod
    def _url(endpoint: str, chunk_id: str) -> str:
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"
        return f"{endpoint.rstrip('/')}/chunk/{urllib.parse.quote(chunk_id)}"

    def put_chunk(self, endpoint: str, chunk_id: str, data: bytes, checksum: str, attempts: int = 4) -> dict:
        """Sube un chunk declarando su checksum.

        Reintentar es seguro: el DataNode sobrescribe con los mismos bytes si el chunk ya
        llegó y solo se perdió el ACK. `attempts` lo baja el cliente cuando hay otras
        réplicas que alcanzan el quórum, para no esperar de más a un nodo caído.

        El DataNode verifica el hash antes de aceptarlo, así un chunk corrupto en
        tránsito nunca llega a disco (RD-02).
        """
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Chunk-Checksum": checksum,
            "Content-Length": str(len(data)),
        }
        _, _, body = _request(
            "PUT", self._url(endpoint, chunk_id), body=data, headers=headers,
            timeout=self._write_timeout(len(data)), attempts=attempts,
        )
        return json.loads(body) if body else {}

    def get_chunk(self, endpoint: str, chunk_id: str, attempts: int = 4) -> tuple[bytes, str]:
        """Descarga un chunk y devuelve sus bytes junto al checksum que reportó el nodo.

        `attempts` es cuántas veces se intenta contra ESTE nodo antes de rendirse. El
        failover entre réplicas lo pasa en 1: si el nodo no responde, es más barato
        probar la siguiente réplica que esperar el retroceso exponencial (RQ-26).
        """
        _, headers, body = _request(
            "GET", self._url(endpoint, chunk_id), timeout=self.timeout, attempts=attempts
        )
        return body, headers.get("X-Chunk-Checksum", "")
