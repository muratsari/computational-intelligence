#!/usr/bin/env python3
"""
FR24 benzeri sentetik uçuş olayı (event) üreteci.

Flightradar24 API'nin "live flight positions (full)" çıktısına benzeyen
konum olaylarını üretir ve stdout / dosya / AWS Kinesis'e yazar.

Stream processing kavramlarını (event time, watermark, geç gelen veri,
tekrar eden kayıt, anomali tespiti, veri kalitesi) denemek için kontrollü
"kaos" enjeksiyonu yapar. Her olayda `_gen` alanı ground-truth etiketlerini
taşır (ör. hangi anomali, kaç saniye geç). Değerlendirme için kullanılabilir;
gerçek veriye benzetmek için `--no-meta` ile kapatılabilir.

Yalnızca standart kütüphane kullanır; Kinesis sink'i için `boto3` gerekir.
"""
from __future__ import annotations

import argparse
import heapq
import json
import math
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

GENERATOR_VERSION = "0.1.0"
EARTH_RADIUS_KM = 6371.0
KT_TO_KMH = 1.852

# iata -> (icao, lat, lon)
AIRPORTS = {
    "IST": ("LTFM", 41.2753, 28.7519),
    "SAW": ("LTFJ", 40.8986, 29.3092),
    "ESB": ("LTAC", 40.1281, 32.9951),
    "ADB": ("LTBJ", 38.2924, 27.1570),
    "AYT": ("LTAI", 36.8987, 30.8005),
    "TZX": ("LTCG", 40.9951, 39.7897),
    "ATH": ("LGAV", 37.9364, 23.9445),
    "VIE": ("LOWW", 48.1103, 16.5697),
    "FCO": ("LIRF", 41.8003, 12.2389),
    "MUC": ("EDDM", 48.3537, 11.7750),
    "FRA": ("EDDF", 50.0379, 8.5622),
    "AMS": ("EHAM", 52.3105, 4.7683),
    "CDG": ("LFPG", 49.0097, 2.5479),
    "LHR": ("EGLL", 51.4700, -0.4543),
    "DOH": ("OTHH", 25.2731, 51.6081),
    "DXB": ("OMDB", 25.2532, 55.3657),
}


@dataclass(frozen=True)
class Airline:
    iata: str
    icao: str
    weight: float
    hubs: tuple[str, ...]
    reg_prefix: str
    reg_suffix_len: int
    hex_range: tuple[int, int]  # ICAO 24-bit adres bloğu (ülkeye göre)


AIRLINES = [
    Airline("TK", "THY", 0.35, ("IST",), "TC-J", 2, (0x4B8000, 0x4BFFFF)),
    Airline("PC", "PGT", 0.18, ("SAW",), "TC-N", 2, (0x4B8000, 0x4BFFFF)),
    Airline("VF", "TKJ", 0.10, ("SAW", "ESB"), "TC-L", 2, (0x4B8000, 0x4BFFFF)),
    Airline("LH", "DLH", 0.10, ("FRA", "MUC"), "D-A", 3, (0x3C0000, 0x3FFFFF)),
    Airline("BA", "BAW", 0.05, ("LHR",), "G-", 4, (0x400000, 0x43FFFF)),
    Airline("AF", "AFR", 0.05, ("CDG",), "F-", 4, (0x380000, 0x3BFFFF)),
    Airline("KL", "KLM", 0.05, ("AMS",), "PH-", 3, (0x480000, 0x487FFF)),
    Airline("EK", "UAE", 0.06, ("DXB",), "A6-", 3, (0x896000, 0x896FFF)),
    Airline("QR", "QTR", 0.06, ("DOH",), "A7-", 3, (0x06A000, 0x06AFFF)),
]

NARROWBODY = ("A320", "A321", "B738", "A20N", "A21N", "B38M")
WIDEBODY = ("A333", "B77W", "A359", "B789")

# Anomali türleri: (ad, göreli ağırlık)
ANOMALIES = [
    ("emergency_7700", 0.25),   # acil durum squawk'u + kontrollü alçalma
    ("radio_failure_7600", 0.20),  # geçici telsiz arızası squawk'u
    ("rapid_descent", 0.25),    # squawk normal, dikey hız aşırı negatif
    ("signal_loss", 0.30),      # bir süre hiç olay gelmez (gap)
]
RESERVED_SQUAWKS = {"7500", "7600", "7700", "7000", "2000", "1200"}


# --------------------------------------------------------------------------- #
# Coğrafi yardımcılar
# --------------------------------------------------------------------------- #
def haversine_km(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def bearing_deg(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def intermediate_point(lat1, lon1, lat2, lon2, frac) -> tuple[float, float]:
    """Büyük daire (great circle) üzerinde frac ∈ [0,1] oranındaki nokta."""
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    d = haversine_km(lat1, lon1, lat2, lon2) / EARTH_RADIUS_KM
    if d == 0:
        return lat1, lon1
    a = math.sin((1 - frac) * d) / math.sin(d)
    b = math.sin(frac * d) / math.sin(d)
    x = a * math.cos(p1) * math.cos(l1) + b * math.cos(p2) * math.cos(l2)
    y = a * math.cos(p1) * math.sin(l1) + b * math.cos(p2) * math.sin(l2)
    z = a * math.sin(p1) + b * math.sin(p2)
    return math.degrees(math.atan2(z, math.hypot(x, y))), math.degrees(math.atan2(y, x))


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# Uçuş modeli
# --------------------------------------------------------------------------- #
@dataclass
class Flight:
    fr24_id: str
    airline: Airline
    number: int
    hex: str
    reg: str
    ac_type: str
    squawk: str
    orig: str
    dest: str
    total_km: float
    cruise_alt: int
    cruise_kt: float
    report_offset: float  # tick içindeki rapor zamanı kayması (sn)
    flown_km: float = 0.0
    alt: int = 0
    # anomali durumu
    anomaly: str | None = None
    anomaly_ticks_left: int = 0
    alt_offset: float = 0.0
    squawk_override: str | None = None

    CLIMB_KM = 220.0
    DESCENT_KM = 280.0

    @property
    def remaining_km(self) -> float:
        return max(0.0, self.total_km - self.flown_km)

    @property
    def arrived(self) -> bool:
        return self.flown_km >= self.total_km

    def profile_alt(self) -> float:
        frac = min(1.0, self.flown_km / self.CLIMB_KM, self.remaining_km / self.DESCENT_KM)
        return self.cruise_alt * max(0.0, frac)


class FlightFactory:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self._used_ids: set[str] = set()

    def _unique_hex(self, n_chars: int) -> str:
        while True:
            v = f"{self.rng.getrandbits(n_chars * 4):0{n_chars}x}"
            if v not in self._used_ids:
                self._used_ids.add(v)
                return v

    def _squawk(self) -> str:
        while True:
            s = "".join(str(self.rng.randint(0, 7)) for _ in range(4))
            if s not in RESERVED_SQUAWKS and not s.startswith("7"):
                return s

    def new_flight(self, random_progress: bool) -> Flight:
        rng = self.rng
        al = rng.choices(AIRLINES, weights=[a.weight for a in AIRLINES])[0]
        hub = rng.choice(al.hubs)
        other = rng.choice([a for a in AIRPORTS if a != hub])
        orig, dest = (hub, other) if rng.random() < 0.5 else (other, hub)
        _, la1, lo1 = AIRPORTS[orig]
        _, la2, lo2 = AIRPORTS[dest]
        total = haversine_km(la1, lo1, la2, lo2)
        ac_type = rng.choice(WIDEBODY if total > 2500 or al.icao in ("UAE", "QTR") else NARROWBODY)
        letters = "".join(rng.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZ") for _ in range(al.reg_suffix_len))
        f = Flight(
            fr24_id=self._unique_hex(8),
            airline=al,
            number=rng.randint(100, 2999),
            hex=f"{rng.randint(*al.hex_range):06X}",
            reg=al.reg_prefix + letters,
            ac_type=ac_type,
            squawk=self._squawk(),
            orig=orig,
            dest=dest,
            total_km=total,
            cruise_alt=rng.choice(range(31000, 41001, 1000)),
            cruise_kt=rng.uniform(430, 490),
            report_offset=rng.random(),
        )
        if random_progress:
            # simülasyon başında gökyüzü dolu olsun
            f.flown_km = rng.uniform(0, total * 0.95)
        f.alt = int(f.profile_alt())
        return f


# --------------------------------------------------------------------------- #
# Simülatör
# --------------------------------------------------------------------------- #
@dataclass
class ChaosConfig:
    late_prob: float = 0.0
    max_late_s: float = 60.0
    dup_prob: float = 0.0
    anomaly_prob: float = 0.0
    dirty_prob: float = 0.0


@dataclass
class Stats:
    generated: int = 0
    emitted: int = 0
    late: int = 0
    duplicates: int = 0
    dirty: int = 0
    anomalies_started: dict = field(default_factory=dict)
    suppressed_signal_loss: int = 0
    completed_flights: int = 0


class Simulator:
    def __init__(self, n_flights: int, interval_s: float, chaos: ChaosConfig,
                 rng: random.Random, with_meta: bool):
        self.interval = interval_s
        self.chaos = chaos
        self.rng = rng
        self.with_meta = with_meta
        self.factory = FlightFactory(rng)
        self.flights = [self.factory.new_flight(random_progress=True) for _ in range(n_flights)]
        self.pending: list[tuple[float, int, dict]] = []  # (release_epoch, seq, event)
        self.seq = 0
        self.stats = Stats()

    # --- anomali yönetimi ---------------------------------------------------
    def _maybe_start_anomaly(self, f: Flight) -> None:
        if f.anomaly or f.alt < 10000 or self.rng.random() >= self.chaos.anomaly_prob:
            return
        name = self.rng.choices([a for a, _ in ANOMALIES], weights=[w for _, w in ANOMALIES])[0]
        f.anomaly = name
        self.stats.anomalies_started[name] = self.stats.anomalies_started.get(name, 0) + 1
        if name == "emergency_7700":
            f.squawk_override = "7700"
            f.anomaly_ticks_left = 10**9  # iniş/uçuş sonuna kadar sürer
        elif name == "radio_failure_7600":
            f.squawk_override = "7600"
            f.anomaly_ticks_left = self.rng.randint(6, 30)
        elif name == "rapid_descent":
            f.anomaly_ticks_left = self.rng.randint(3, 6)
        elif name == "signal_loss":
            f.anomaly_ticks_left = self.rng.randint(6, 30)

    def _apply_anomaly_physics(self, f: Flight) -> None:
        if f.anomaly == "emergency_7700":
            # ~3000 ft/dk alçal, 10.000 ft civarında seviye tut
            target = min(0.0, 10000 - f.profile_alt())
            f.alt_offset = max(target, f.alt_offset - 3000 * self.interval / 60)
        elif f.anomaly == "rapid_descent" and f.anomaly_ticks_left > 0:
            f.alt_offset -= 7000 * self.interval / 60

    def _tick_anomaly(self, f: Flight) -> None:
        if not f.anomaly:
            return
        f.anomaly_ticks_left -= 1
        if f.anomaly_ticks_left <= 0:
            if f.anomaly == "radio_failure_7600":
                f.squawk_override = None
            # rapid_descent sonrası irtifa ofseti kalır (yeni seviye)
            f.anomaly = None

    # --- bir tick ------------------------------------------------------------
    def step(self, tick_end: datetime) -> list[dict]:
        tick_start = tick_end - timedelta(seconds=self.interval)
        out: list[dict] = []

        for i, f in enumerate(self.flights):
            self._maybe_start_anomaly(f)
            self._apply_anomaly_physics(f)

            prev_alt = f.alt
            base = f.profile_alt()
            alt = max(0.0, min(base, base + f.alt_offset))
            alt_frac = alt / f.cruise_alt if f.cruise_alt else 0
            gspeed = 150 + (f.cruise_kt - 150) * alt_frac + self.rng.gauss(0, 4)
            f.flown_km += gspeed * KT_TO_KMH * self.interval / 3600
            f.alt = int(round(alt / 25) * 25)
            vspeed = int((f.alt - prev_alt) / self.interval * 60)

            if f.arrived:
                self.stats.completed_flights += 1
                self.flights[i] = self.factory.new_flight(random_progress=False)
                continue

            if f.anomaly == "signal_loss":
                self.stats.suppressed_signal_loss += 1
                self._tick_anomaly(f)
                continue

            ts = tick_start + timedelta(seconds=f.report_offset * self.interval)
            ev = self._build_event(f, ts, gspeed, vspeed)
            self._tick_anomaly(f)
            out.extend(self._apply_delivery_chaos(ev, ts))

        # gecikmesi dolan olayları serbest bırak
        now_epoch = tick_end.timestamp()
        while self.pending and self.pending[0][0] <= now_epoch:
            out.append(heapq.heappop(self.pending)[2])

        out.sort(key=lambda e: e["_gen"]["emitted_at"] if "_gen" in e else 0)
        for ev in out:
            if not self.with_meta:
                ev.pop("_gen", None)
        self.stats.emitted += len(out)
        return out

    def flush_pending(self) -> list[dict]:
        out = [heapq.heappop(self.pending)[2] for _ in range(len(self.pending))]
        for ev in out:
            if not self.with_meta:
                ev.pop("_gen", None)
        self.stats.emitted += len(out)
        return out

    # --- olay inşası ---------------------------------------------------------
    def _build_event(self, f: Flight, ts: datetime, gspeed: float, vspeed: int) -> dict:
        o_icao, la1, lo1 = AIRPORTS[f.orig]
        d_icao, la2, lo2 = AIRPORTS[f.dest]
        frac = min(1.0, f.flown_km / f.total_km)
        lat, lon = intermediate_point(la1, lo1, la2, lo2, frac)
        track = bearing_deg(lat, lon, la2, lo2)
        eta = ts + timedelta(hours=f.remaining_km / max(gspeed * KT_TO_KMH, 1))
        self.seq += 1
        self.stats.generated += 1
        return {
            "fr24_id": f.fr24_id,
            "flight": f"{f.airline.iata}{f.number}",
            "callsign": f"{f.airline.icao}{f.number}",
            "lat": round(lat, 5),
            "lon": round(lon, 5),
            "track": int(round(track)) % 360,
            "alt": f.alt,
            "gspeed": int(round(gspeed)),
            "vspeed": vspeed,
            "squawk": f.squawk_override or f.squawk,
            "timestamp": iso(ts),
            "source": "ADSB",
            "hex": f.hex,
            "type": f.ac_type,
            "reg": f.reg,
            "painted_as": f.airline.icao,
            "operating_as": f.airline.icao,
            "orig_iata": f.orig,
            "orig_icao": o_icao,
            "dest_iata": f.dest,
            "dest_icao": d_icao,
            "eta": iso(eta),
            "_gen": {
                "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
                "seq": self.seq,
                "anomaly": f.anomaly,
                "late_by_s": 0.0,
                "duplicate": False,
                "dirty": None,
                "emitted_at": ts.timestamp(),
                "generator_version": GENERATOR_VERSION,
            },
        }

    def _make_dirty(self, ev: dict) -> None:
        kind = self.rng.choice(["null_lat", "bad_lon", "negative_alt", "empty_callsign", "bad_timestamp"])
        if kind == "null_lat":
            ev["lat"] = None
        elif kind == "bad_lon":
            ev["lon"] = 999.0
        elif kind == "negative_alt":
            ev["alt"] = -self.rng.randint(100, 2000)
        elif kind == "empty_callsign":
            ev["callsign"] = ""
        elif kind == "bad_timestamp":
            ev["timestamp"] = ev["timestamp"].replace("T", " ").rstrip("Z")
        ev["_gen"]["dirty"] = kind
        self.stats.dirty += 1

    def _apply_delivery_chaos(self, ev: dict, ts: datetime) -> list[dict]:
        c = self.chaos
        if c.dirty_prob and self.rng.random() < c.dirty_prob:
            self._make_dirty(ev)

        copies = [ev]
        if c.dup_prob and self.rng.random() < c.dup_prob:
            dup = json.loads(json.dumps(ev))
            dup["_gen"]["duplicate"] = True
            copies.append(dup)
            self.stats.duplicates += 1

        ready = []
        for e in copies:
            if c.late_prob and self.rng.random() < c.late_prob:
                delay = self.rng.uniform(self.interval, max(self.interval, c.max_late_s))
                e["_gen"]["late_by_s"] = round(delay, 3)
                e["_gen"]["emitted_at"] = ts.timestamp() + delay
                heapq.heappush(self.pending, (e["_gen"]["emitted_at"], e["_gen"]["seq"] * 2 + e["_gen"]["duplicate"], e))
                self.stats.late += 1
            else:
                ready.append(e)
        return ready


# --------------------------------------------------------------------------- #
# Sink'ler
# --------------------------------------------------------------------------- #
class StdoutSink:
    def write(self, events: list[dict]) -> None:
        for e in events:
            sys.stdout.write(json.dumps(e, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    def close(self) -> None:
        pass


class FileSink:
    def __init__(self, path: str):
        self.fh = open(path, "a", encoding="utf-8")

    def write(self, events: list[dict]) -> None:
        for e in events:
            self.fh.write(json.dumps(e, ensure_ascii=False) + "\n")
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


class KinesisSink:
    """PutRecords ile batch gönderim. Partition key = fr24_id (uçuş başına sıralama)."""

    MAX_BATCH = 500

    def __init__(self, stream: str, region: str | None, endpoint_url: str | None):
        try:
            import boto3
        except ImportError:
            sys.exit("Kinesis sink için boto3 gerekli: pip install boto3")
        self.stream = stream
        self.client = boto3.client("kinesis", region_name=region, endpoint_url=endpoint_url)

    def write(self, events: list[dict]) -> None:
        for i in range(0, len(events), self.MAX_BATCH):
            records = [
                {"Data": json.dumps(e, ensure_ascii=False).encode("utf-8"),
                 "PartitionKey": e.get("fr24_id") or "unknown"}
                for e in events[i:i + self.MAX_BATCH]
            ]
            self._put_with_retry(records)

    def _put_with_retry(self, records: list[dict], attempts: int = 5) -> None:
        for attempt in range(attempts):
            resp = self.client.put_records(StreamName=self.stream, Records=records)
            if not resp.get("FailedRecordCount"):
                return
            # yalnızca başarısız kayıtları (ör. ProvisionedThroughputExceeded) tekrar dene
            records = [r for r, res in zip(records, resp["Records"]) if "ErrorCode" in res]
            time.sleep(min(2 ** attempt * 0.1, 2.0))
        print(f"[uyarı] {len(records)} kayıt Kinesis'e yazılamadı", file=sys.stderr)

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="FR24 benzeri sentetik uçuş konum olayı üreteci",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    g = p.add_argument_group("simülasyon")
    g.add_argument("--flights", type=int, default=50, help="eşzamanlı uçuş sayısı")
    g.add_argument("--interval", type=float, default=10.0,
                   help="her uçağın rapor periyodu (simülasyon saniyesi)")
    g.add_argument("--speedup", type=float, default=1.0,
                   help="simülasyon/gerçek zaman oranı; 0 = beklemeden mümkün olan en hızlı")
    g.add_argument("--start", type=str, default=None,
                   help="simülasyon başlangıç zamanı (ISO-8601, UTC); varsayılan: şimdi")
    g.add_argument("--ticks", type=int, default=0, help="kaç rapor döngüsü (0 = sonsuz)")
    g.add_argument("--max-events", type=int, default=0, help="en fazla olay sayısı (0 = sınırsız)")
    g.add_argument("--seed", type=int, default=None, help="tekrarlanabilirlik için RNG tohumu")

    c = p.add_argument_group("kaos / veri kalitesi")
    c.add_argument("--late-prob", type=float, default=0.0, help="olayın geç (out-of-order) gelme olasılığı")
    c.add_argument("--max-late", type=float, default=60.0, help="en fazla gecikme (sn)")
    c.add_argument("--dup-prob", type=float, default=0.0, help="olayın tekrar gönderilme olasılığı")
    c.add_argument("--anomaly-prob", type=float, default=0.0,
                   help="tick başına, uçuş başına anomali başlama olasılığı")
    c.add_argument("--dirty-prob", type=float, default=0.0, help="bozuk/eksik alan olasılığı")
    c.add_argument("--chaos", action="store_true",
                   help="makul kaos ön ayarı: late=0.05 dup=0.01 anomaly=0.002 dirty=0.005")

    o = p.add_argument_group("çıktı")
    o.add_argument("--sink", choices=["stdout", "file", "kinesis"], default="stdout")
    o.add_argument("--out", default="events.jsonl", help="file sink yolu")
    o.add_argument("--stream", default="flight-positions", help="Kinesis stream adı")
    o.add_argument("--region", default=None, help="AWS bölgesi")
    o.add_argument("--endpoint-url", default=None, help="ör. LocalStack: http://localhost:4566")
    o.add_argument("--no-meta", action="store_true", help="_gen ground-truth alanını çıkar")
    o.add_argument("--quiet", action="store_true", help="stderr istatistiklerini kapat")
    args = p.parse_args(argv)

    if args.chaos:
        args.late_prob = args.late_prob or 0.05
        args.dup_prob = args.dup_prob or 0.01
        args.anomaly_prob = args.anomaly_prob or 0.002
        args.dirty_prob = args.dirty_prob or 0.005
    if args.interval <= 0:
        p.error("--interval pozitif olmalı")
    return args


def build_sink(args):
    if args.sink == "file":
        return FileSink(args.out)
    if args.sink == "kinesis":
        return KinesisSink(args.stream, args.region, args.endpoint_url)
    return StdoutSink()


def main(argv=None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    chaos = ChaosConfig(args.late_prob, args.max_late, args.dup_prob, args.anomaly_prob, args.dirty_prob)
    sim = Simulator(args.flights, args.interval, chaos, rng, with_meta=not args.no_meta)
    sink = build_sink(args)

    sim_now = (datetime.fromisoformat(args.start.replace("Z", "+00:00")).astimezone(timezone.utc)
               if args.start else datetime.now(timezone.utc))

    stop = False

    def _handle(_sig, _frm):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    tick = 0
    last_report = time.monotonic()
    try:
        while not stop:
            t0 = time.monotonic()
            sim_now += timedelta(seconds=args.interval)
            events = sim.step(sim_now)
            if args.max_events and sim.stats.emitted > args.max_events:
                overflow = sim.stats.emitted - args.max_events
                events = events[:len(events) - overflow]
                sim.stats.emitted = args.max_events
                stop = True
            sink.write(events)
            tick += 1
            if args.ticks and tick >= args.ticks:
                break
            if not args.quiet and time.monotonic() - last_report > 5:
                s = sim.stats
                print(f"[gen] tick={tick} sim={iso(sim_now)} emitted={s.emitted} "
                      f"late_pending={len(sim.pending)} anomalies={s.anomalies_started}",
                      file=sys.stderr)
                last_report = time.monotonic()
            if args.speedup > 0:
                time.sleep(max(0.0, args.interval / args.speedup - (time.monotonic() - t0)))
        if not (args.max_events and sim.stats.emitted >= args.max_events):
            sink.write(sim.flush_pending())
    except BrokenPipeError:  # ör. `| head`
        sys.stderr.close()
        return 0
    finally:
        sink.close()

    if not args.quiet:
        print(f"[gen] bitti: {json.dumps(sim.stats.__dict__, ensure_ascii=False)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
