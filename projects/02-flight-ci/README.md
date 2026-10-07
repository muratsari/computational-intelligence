# 02 · flight-ci

Flightradar24 verisi üzerinde **Amazon Kinesis + Apache Flink** kullanan bir
Continuous Intelligence sistemi. *(Planlandı, henüz kod yok.)*

Kavramsal arka plan: [docs/01-continuous-intelligence-nedir.md](../../docs/01-continuous-intelligence-nedir.md)

## Hedef mimari

```
FR24 API / 01-event-generator → Kinesis Data Streams → Flink → uyarı · DB · dashboard · S3
```

## Plan (taslak)

- [ ] Yerel ortam: LocalStack (Kinesis) + Flink
- [ ] Producer: FR24 API polling → Kinesis
- [ ] Flink işi: temizleme, deduplication, event-time watermark
- [ ] Tespitler: acil durum squawk, ani alçalma, sinyal kaybı
- [ ] Sunum / aksiyon katmanı
- [ ] Değerlendirme: gecikme, throughput, precision/recall (üretecin ground-truth'u ile)

Geliştirme verisi için: [01-event-generator](../01-event-generator/)
