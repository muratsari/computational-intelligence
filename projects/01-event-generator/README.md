# 01 · event-generator

Flightradar24 API'nin canlı konum çıktısına benzeyen **sentetik uçuş olayları**
üretir. Uçuşlar gerçek havalimanları arasında büyük daire rotası izler;
tırmanış/seyir/alçalma profili, yer hızı ve dikey hız tutarlıdır.

Yalnızca Python standart kütüphanesi gerekir (Kinesis sink'i için `boto3`).

## Kullanım

```bash
# 20 uçuş, gerçek zamanlı, stdout'a JSONL
python3 generator.py --flights 20

# Hızlı: 500 döngü, beklemeden, kaos açık, dosyaya
python3 generator.py --flights 200 --ticks 500 --speedup 0 \
    --chaos --seed 1 --sink file --out data/events.jsonl

# Kinesis (AWS veya LocalStack)
pip install -r requirements.txt
python3 generator.py --sink kinesis --stream flight-positions --region eu-central-1 --chaos
python3 generator.py --sink kinesis --stream flight-positions --region us-east-1 \
    --endpoint-url http://localhost:4566
```

Tüm seçenekler: `python3 generator.py --help`

## Kaos / veri kalitesi enjeksiyonu

| Seçenek | Ne üretir | Flink'te karşılığı |
|---|---|---|
| `--late-prob`, `--max-late` | Olay event time'ından geç, sırasız gönderilir | watermark, allowed lateness, side output |
| `--dup-prob` | Aynı olay iki kez gönderilir | durumlu deduplication |
| `--anomaly-prob` | `emergency_7700`, `radio_failure_7600`, `rapid_descent`, `signal_loss` | filtre, KeyedProcessFunction, CEP, timer |
| `--dirty-prob` | `null` lat, geçersiz lon, negatif irtifa, boş callsign, bozuk timestamp | şema doğrulama, side output |
| `--chaos` | Makul ön ayar (late %5, dup %1, anomaly %0.2, dirty %0.5) | - |

## Olay şeması

FR24 alanları: `fr24_id, flight, callsign, lat, lon, track, alt (ft), gspeed (kt),
vspeed (ft/dk), squawk, timestamp, source, hex, type, reg, painted_as, operating_as,
orig_iata, orig_icao, dest_iata, dest_icao, eta`

Ek olarak `_gen` alanı **ground-truth** taşır (`event_id`, `seq`, `anomaly`,
`late_by_s`, `duplicate`, `dirty`, `emitted_at`). Tespit algoritmalarının
precision/recall ve time-to-detect ölçümü için kullanılır; `--no-meta` ile kapatılır.

Kinesis'te partition key `fr24_id`'dir; uçuş içi sıralama shard düzeyinde korunur.
