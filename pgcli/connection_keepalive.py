import os
import threading
import time

state = {
    "pgexecute": None,
    "thread": None,
}


def keepalive(pgexecute):
    state["pgexecute"] = pgexecute
    if state["thread"] is None:
        state["thread"] = threading.Thread(target=keepalive_thread, daemon=True)
        state["thread"].start()


def keepalive_thread():
    while True:
        time.sleep(30)
        if state["pgexecute"]:
            try:
                state["pgexecute"].run("--ping")
            except Exception as e:
                print(e)
