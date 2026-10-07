# Business Intelligence: Doktora Dersi

Bu depo, doktora düzeyindeki Business Intelligence dersi için hazırlanan ders notlarını ve dönem boyunca geliştirilen uygulamalı projeleri bir arada tutar. Notlar kavramsal ve kuramsal çerçeveyi kurar; projeler ise bu çerçeveyi çalışan sistemlerle sınar.

Dönemin ana ekseni **Continuous Intelligence (CI)**: gerçek zamanlı analitiğin iş süreçlerinin içine gömülmesi, olay akışlarının sürekli işlenmesi ve analiz sonuçlarının doğrudan karara ve aksiyona dönüştürülmesi. Uygulama tarafında hedef, canlı uçuş verisi üzerinde **Amazon Kinesis** ve **Apache Flink** kullanan uçtan uca bir CI sistemi kurmaktır. Bu hedefe, her biri kendi başına anlamlı ve bağımsız çalışabilen küçük projeler üzerinden adım adım ilerlenir.

---

## İçindekiler

- [Depo hiyerarşisi](#depo-hiyerarşisi)
- [Ders notları](#ders-notları)
- [Projeler](#projeler)
- [Projeler arası ilişki](#projeler-arası-ilişki)
- [Yol haritası](#yol-haritası)
- [Hızlı başlangıç](#hızlı-başlangıç)
- [Çalışma kuralları](#çalışma-kuralları)

---

## Depo hiyerarşisi

```
.
├── README.md                                  ← bu dosya: navigasyon ve genel bakış
│
├── docs/                                      ← ders notları (projeden bağımsız, kavramsal)
│   └── 01-continuous-intelligence-nedir.md
│
└── projects/                                  ← uygulamalı projeler (her biri bağımsız)
    ├── 01-event-generator/                    ← sentetik uçuş olayı üreteci
    │   ├── README.md
    │   ├── generator.py
    │   └── requirements.txt
    │
    └── 02-flight-ci/                          ← Kinesis + Flink CI sistemi (planlandı)
        └── README.md
```

Depo iki katmandan oluşur:

- **`docs/`** genel ve kalıcı bilgiyi içerir. Buradaki notlar belirli bir projeye bağlı değildir; bir ders kitabı bölümü gibi okunabilir ve başka bağlamlarda yeniden kullanılabilir.
- **`projects/`** uygulamayı içerir. Her proje kendi klasöründe, kendi `README.md`, bağımlılıkları ve çalıştırma talimatlarıyla birlikte durur. Bir projeyi anlamak ya da çalıştırmak için diğerlerine bakmak gerekmez; projeler arasındaki bağlantılar yalnızca README'lerdeki bağlantılarla kurulur.

---

## Ders notları

| # | Not | Özet |
|---|---|---|
| 01 | [Continuous Intelligence nedir?](docs/01-continuous-intelligence-nedir.md) | CI'nın tanımı, BI'dan CI'ya evrim, akış işlemenin kuramsal temelleri, mimariler, değerlendirme, yönetişim ve araştırma soruları |

### 01 · Continuous Intelligence nedir?

Dönemin kavramsal temelini oluşturan not. Hackathorn'un değer–zaman eğrisinden yola çıkarak verinin zamanla neden değer kaybettiğini açıklar, ardından CI'yı Gartner tanımı ve ilişkili kavramlar (Operational Intelligence, CEP, Decision Intelligence) üzerinden konumlandırır. Kuramsal bölümler event time, watermark, pencereleme, Dataflow modeli ve akış–tablo ikiliğini; sistem bölümleri durum yönetimi, checkpoint ve exactly-once garantilerini ele alır. Not, akış üzerinde analitik (sketch'ler, anomali tespiti, concept drift, CEP), karar katmanı ve otomasyon seviyeleri, değerlendirme metrikleri ve veri yönetişimiyle devam eder; doktora düzeyinde araştırma soruları, alıştırmalar ve doğrulanmış bir kaynakçayla biter.

Hızlı erişim:
[Tanım](docs/01-continuous-intelligence-nedir.md#2-tanım-ve-kapsam) ·
[Tarihçe](docs/01-continuous-intelligence-nedir.md#3-tarihsel-gelişim) ·
[Kuramsal temeller](docs/01-continuous-intelligence-nedir.md#6-akış-işlemenin-kuramsal-temelleri) ·
[Mimariler](docs/01-continuous-intelligence-nedir.md#11-referans-mimariler-ve-teknoloji-ekosistemi) ·
[Metrikler](docs/01-continuous-intelligence-nedir.md#12-değerlendirme-metrikleri) ·
[Araştırma soruları](docs/01-continuous-intelligence-nedir.md#16-araştırma-soruları) ·
[Kaynaklar](docs/01-continuous-intelligence-nedir.md#19-kaynaklar)

---

## Projeler

| # | Proje | Hafta | Durum | Teknoloji | Açıklama |
|---|---|---|---|---|---|
| 01 | [event-generator](projects/01-event-generator/) | - | ✅ Ön çalışma tamam | Python | FR24 benzeri sentetik uçuş olayı üreteci |
| 02 | [flight-ci](projects/02-flight-ci/) | - | 🗓️ Planlandı | Kinesis, Flink | Canlı uçuş verisi üzerinde CI sistemi |

### 01 · event-generator

Flightradar24 API'nin canlı konum çıktısına benzeyen sentetik uçuş olayları üreten bir Python aracı. Uçuşlar gerçek havalimanları arasında büyük daire rotası izler ve tırmanış, seyir, alçalma profili ile hız değerleri birbiriyle tutarlıdır. Asıl değeri, akış işlemenin zor problemlerini **kontrollü biçimde** üretebilmesidir: geç ve sırasız gelen olaylar, tekrar eden kayıtlar, bozuk alanlar ve dört tür anomali (acil durum squawk'u, telsiz arızası, ani alçalma, sinyal kaybı). Her olay, ne tür bir bozulmaya uğradığını gösteren ground-truth etiketleri taşır; bu sayede sonraki projelerde geliştirilen tespit algoritmaları nesnel olarak ölçülebilir. Çıktı stdout'a, dosyaya ya da doğrudan Kinesis'e yazılabilir.

→ [Proje README'si](projects/01-event-generator/README.md)

### 02 · flight-ci

Dönemin ana projesi. Olay kaynağından (önce sentetik üreteç, sonra gerçek FR24 API) Kinesis Data Streams'e, oradan Apache Flink'e uzanan ve sonuçları uyarı, veri deposu ve dashboard katmanlarına taşıyan uçtan uca bir Continuous Intelligence sistemi. Ders notlarındaki kavramların (watermark, durumlu işleme, CEP, exactly-once) gerçek bir sistemde nasıl davrandığını gözlemlemek ve ölçmek amaçlanır. Şu an yalnızca plan ve hedef mimari mevcuttur.

→ [Proje README'si](projects/02-flight-ci/README.md)

---

## Projeler arası ilişki

```
 docs/01  Continuous Intelligence nedir?
    │        (kavramsal çerçeve)
    │
    ▼
 ┌─────────────────────────┐        olay akışı         ┌─────────────────────────┐
 │ 01 · event-generator    │ ────────────────────────► │ 02 · flight-ci          │
 │ sentetik veri +         │   JSONL / Kinesis         │ Kinesis → Flink →       │
 │ ground-truth etiketleri │                           │ uyarı · DB · dashboard  │
 └─────────────────────────┘                           └─────────────────────────┘
                                                                  ▲
                                         FR24 API (gerçek veri) ──┘
```

Üreteç, ana sistem için hem **geliştirme verisi** hem de **değerlendirme düzeneği** görevi görür: aynı tohum (seed) ile aynı olay akışı yeniden üretilebildiği için deneyler tekrarlanabilir, ground-truth etiketleri sayesinde de tespitlerin doğruluğu ölçülebilir. Gerçek FR24 verisine geçildiğinde ana sistem değişmeden kalır; yalnızca olay kaynağı değişir.

---

## Yol haritası

Projeler haftalara gevşek biçimde bağlıdır: bazı haftalar yeni bir proje doğurur, bazıları mevcut projeyi derinleştirir. Numaralandırma haftayı değil, projelerin sırasını gösterir.

| Aşama | Kapsam | İlgili proje |
|---|---|---|
| Ön çalışma | CI kavramları, sentetik veri üreteci | `docs/01`, `01-event-generator` |
| Altyapı | Yerel ortam (LocalStack ile Kinesis, Flink), üreteçten Kinesis'e akış | `02-flight-ci` |
| İşleme | Temizleme, deduplication, event-time watermark, pencereleme | `02-flight-ci` |
| Tespit | Acil durum squawk'u, ani alçalma, sinyal kaybı, CEP desenleri | `02-flight-ci` |
| Gerçek veri | FR24 API producer'ı, polling ve maliyet tasarımı | `02-flight-ci` veya yeni proje |
| Değerlendirme | Gecikme, throughput, precision/recall, time-to-detect | `02-flight-ci` veya yeni proje |

---

## Hızlı başlangıç

Ön koşul: Python 3.10+. Kinesis'e yazmak için ayrıca `boto3` ve AWS ya da LocalStack erişimi gerekir.

```bash
# Üreteci dene: 20 uçuş, stdout'a JSONL
python3 projects/01-event-generator/generator.py --flights 20

# Kaos açık, hızlı modda dosyaya 100 bin civarı olay
python3 projects/01-event-generator/generator.py \
    --flights 200 --ticks 500 --speedup 0 --chaos --seed 1 \
    --sink file --out data/events.jsonl
```

---

## Çalışma kuralları

**Adlandırma**
- Notlar: `docs/NN-konu-adi.md` (ör. `docs/02-akis-isleme-zaman-semantigi.md`)
- Projeler: `projects/NN-kisa-ad/` (ör. `projects/03-fr24-producer/`)
- `NN` iki haneli sıra numarasıdır; notlar ve projeler ayrı ayrı numaralanır.

**Yeni proje eklerken**
1. `projects/NN-kisa-ad/` klasörünü ve içinde bir `README.md` oluştur (amaç, kullanım, bağımlılıklar).
2. Bağımlılıkları projenin kendi klasöründe tut (`requirements.txt`, `pyproject.toml`, `pom.xml` vb.).
3. Bu dosyadaki **Projeler** tablosuna bir satır, altına kısa bir açıklama paragrafı ve gerekiyorsa **Projeler arası ilişki** şemasına bir kutu ekle.

**Yeni not eklerken**
1. `docs/NN-konu-adi.md` dosyasını oluştur; notlar projeden bağımsız ve genel kalmalı.
2. Bu dosyadaki **Ders notları** tablosuna bir satır ve kısa bir özet paragrafı ekle.
3. Kaynakları doğrulanmış künyelerle (DOI, sayfa, yayın yeri) ver.

**Veri ve gizli bilgiler**
- Üretilen veri dosyaları (`data/`, `*.jsonl`) ve `.env` git'e eklenmez (`.gitignore`).
- API anahtarları (FR24, AWS) yalnızca ortam değişkenleri ya da yerel `.env` dosyası üzerinden kullanılır.
