import asyncio
from dataclasses import dataclass
from queue import Queue
from typing import Literal, Optional
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from enum import Enum
from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread
from loguru import logger
from uuid import uuid4
from psutil import net_connections
import uvicorn

from collections import deque

app = FastAPI(title="Obd online diagnostic service")

logger.add("logs/file-{time:DD-MM-YYYY}.log", rotation="500 MB")

available_ports = deque(range(10000, 65535 + 1))


class Action(str, Enum):
    EXPOSE = "open"
    CLOSE = "close"


class ExposePort(BaseModel):
    action: Action
    id: Optional[str] = None


class ExposeResponse(BaseModel):
    id: str
    host: str
    masterPort: int
    slavePort: int


class DIAG_STATUS(int, Enum):
    INITIAL = 1
    PENDING_SLAVE_CON = 2
    PENDING_MASTER_CON = 3
    TRANSMITTING = 4
    FINISHED = 5
    ERROR = 6


class Message:
    from_: Literal["master"] | Literal["slave"]
    data: bytes

@dataclass
class DiagData:
    id: str
    masterPort: int
    slavePort: int
    socket_slave: Optional[socket] = None
    socket_master: Optional[socket] = None

    con_slave: Optional[socket] = None
    con_master: Optional[socket] = None

    status: DIAG_STATUS = DIAG_STATUS.INITIAL


diag_data: dict[str, DiagData] = {}


def get_port(used_ports: list[int]) -> int:
    while True:
        port = available_ports.popleft()
        if port in used_ports:
            continue
        return port

def generate_port() -> tuple[int, int]:

    used_ports = [x.laddr.port for x in net_connections() if x.laddr]

    while True:
        if len(available_ports) < 2:
            continue
        return get_port(used_ports), get_port(used_ports)


def expose_socket(port) -> socket:
    s = socket(AF_INET, SOCK_STREAM)
    s.bind(("0.0.0.0", port))
    s.listen()
    return s


def read_data_from_socket(con: socket, address_from: str, queue_to: Queue):
    try:
        while True:
            data = con.recv(1024)
            logger.success(f"Get New message from {address_from}: {data!r}")
            queue_to.put(data)
    except Exception as exc:
        logger.error(f"{exc}, {str(exc)}")


def write_data_to_socket(con: socket, address_to, queue_from: Queue):
    try:
        while True:
            data: bytes = queue_from.get()
            logger.info(f"send message to: {address_to}: {data!r}")
            con.send(data)
            queue_from.task_done()
            logger.success("Message has sended successfully")
    except Exception as exc:
        logger.error(f"{exc}, {str(exc)}")


def perform_transmitting(
    con: socket, address_from: str, address_to: str, queue_from: Queue, que_to: Queue
):
    t1 = Thread(
        target=read_data_from_socket, args=[con, address_from, que_to], daemon=True
    )
    t2 = Thread(
        target=write_data_to_socket, args=[con, address_to, queue_from], daemon=True
    )

    t1.start()
    t2.start()


def expose_pair_of_sockets(diag_session: DiagData):
    q_rx: Queue = Queue()  # очердь отправки ответов от приложения
    q_tx: Queue = Queue()  # очередь отправки запросов от диагноста

    s_user = expose_socket(diag_session.slavePort)
    s_worker = expose_socket(diag_session.masterPort)

    diag_session.socket_master = s_worker
    diag_session.socket_slave = s_user

    diag_session.status = DIAG_STATUS.PENDING_SLAVE_CON
    logger.info("Waiting for slave connection")
    c_user, addr_user = s_user.accept()
    logger.info(f"Accept connection from slave: {addr_user}")
    diag_session.con_slave = c_user

    diag_session.status = DIAG_STATUS.PENDING_MASTER_CON
    logger.info("Waiting for master connection")

    c_worker, addr_worker = s_worker.accept()
    logger.info(f"Accept connection from master: {addr_worker}")
    diag_session.con_master = c_worker
    diag_session.status = DIAG_STATUS.TRANSMITTING

    perform_transmitting(c_user, addr_user, addr_worker, q_tx, q_rx)
    perform_transmitting(c_worker, addr_worker, addr_user, q_rx, q_tx)


@app.post("/expose")
async def expose_port(req_data: ExposePort):
    if req_data.action == Action.EXPOSE:
        idx: str = str(uuid4())
        master, slave = generate_port()

        session = DiagData(id=idx, masterPort=master, slavePort=slave)

        diag_data[idx] = session

        Thread(target=expose_pair_of_sockets, args=[session], daemon=True).start()
        return ExposeResponse(
            id=idx, host="0.0.0.0", masterPort=master, slavePort=slave
        )
    elif req_data.action == Action.CLOSE:
        if req_data.id is None:
            return JSONResponse(
                status_code=422, content={"error": "Не передан идентификатор сессии"}
            )

        session = diag_data[req_data.id]
        if session.con_master:
            session.con_master.close()
        if session.con_slave:
            session.con_slave.close()
        if session.socket_master:
            session.socket_master.close()
        if session.socket_slave:
            session.socket_slave.close()

        available_ports.appendleft(session.masterPort)
        available_ports.appendleft(session.slavePort)

        session.status = DIAG_STATUS.FINISHED

class DiagDataResponse(BaseModel):
    id: str
    masterPort: int
    slavePort: int
    status: DIAG_STATUS = DIAG_STATUS.INITIAL

@app.get("/sessions")
async def list_sessions():
    return [DiagDataResponse(id=x.id, masterPort=x.masterPort, slavePort=x.slavePort, status=x.status) for x in diag_data.values()]

async def main():
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)


if __name__ == "__main__":
    # used_ports = [x.laddr.port for x in psutil.net_connections()]
    # print(set(available_ports) - set(used_ports))
    asyncio.run(main())
