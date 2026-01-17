from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Iterable, Optional

from .client import UNDLClient


@dataclass
class DiscoverState:
    jrec: int = 1
    yielded: int = 0

    @classmethod
    def load(cls, path: Optional[str]) -> "DiscoverState":
        if not path or not os.path.exists(path):
            return cls()
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return cls(**d)

    def save(self, path: Optional[str]) -> None:
        if not path:
            return
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"jrec": self.jrec, "yielded": self.yielded}, f, indent=2)
        os.replace(tmp, path)


def iter_recids(
    client: UNDLClient,
    *,
    query: str,
    rg: int = 100,
    ln: str = "en",
    rm: str = "wrd",
    max_records: int = 0,
    state_path: Optional[str] = None,
    checkpoint_every: int = 500,
) -> Iterable[int]:
    st = DiscoverState.load(state_path)
    jrec = st.jrec
    since_ckpt = 0

    while True:
        recids, next_jrec = client.fetch_recids_page(
            query=query, jrec=jrec, rg=rg, ln=ln, rm=rm
        )
        if not recids:
            st.jrec = jrec
            st.save(state_path)
            return

        for recid in recids:
            yield recid
            st.yielded += 1
            since_ckpt += 1

            if max_records and st.yielded >= max_records:
                st.jrec = jrec
                st.save(state_path)
                return

            if state_path and since_ckpt >= checkpoint_every:
                st.jrec = jrec
                st.save(state_path)
                since_ckpt = 0

        if not next_jrec:
            st.jrec = jrec
            st.save(state_path)
            return

        jrec = next_jrec
        st.jrec = jrec
        st.save(state_path)
