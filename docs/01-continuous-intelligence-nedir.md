# Continuous Intelligence (CI): Ders Notları

> Business Intelligence (Doktora)

---

## İçindekiler

1. [Giriş ve motivasyon](#1-giriş-ve-motivasyon)
2. [Tanım ve kapsam](#2-tanım-ve-kapsam)
3. [Tarihsel gelişim](#3-tarihsel-gelişim)
4. [BI'dan CI'ya evrim](#4-bidan-ciya-evrim)
5. [CI döngüsü ve gecikme bileşenleri](#5-ci-döngüsü-ve-gecikme-bileşenleri)
6. [Akış işlemenin kuramsal temelleri](#6-akış-işlemenin-kuramsal-temelleri)
7. [Durum, tutarlılık ve hata toleransı](#7-durum-tutarlılık-ve-hata-toleransı)
8. [Ölçeklenebilirlik ve performans](#8-ölçeklenebilirlik-ve-performans)
9. [Akış üzerinde analitik](#9-akış-üzerinde-analitik)
10. [Karar ve aksiyon katmanı](#10-karar-ve-aksiyon-katmanı)
11. [Referans mimariler ve teknoloji ekosistemi](#11-referans-mimariler-ve-teknoloji-ekosistemi)
12. [Değerlendirme metrikleri](#12-değerlendirme-metrikleri)
13. [Veri kalitesi ve yönetişim](#13-veri-kalitesi-ve-yönetişim)
14. [Uygulama alanları](#14-uygulama-alanları)
15. [Zorluklar ve açık problemler](#15-zorluklar-ve-açık-problemler)
16. [Araştırma soruları](#16-araştırma-soruları)
17. [Tartışma ve alıştırma soruları](#17-tartışma-ve-alıştırma-soruları)
18. [Sözlük](#18-sözlük)
19. [Kaynaklar](#19-kaynaklar)

---

## 1. Giriş ve motivasyon

Klasik iş zekâsı (BI), verinin bir veri ambarına yüklendiği, sonra sorgulanıp raporlandığı bir **"önce sakla, sonra analiz et"** (store-then-analyze) paradigmasına dayanır. Bu paradigma geçmişi anlamak için güçlüdür; ancak kararın değeri olayın gerçekleşme anına yakınlığıyla orantılı olduğunda yetersiz kalır.

### 1.1 Verinin zaman değeri

Hackathorn (2004), bir iş olayı ile ona verilen tepki arasındaki süreyi üç gecikme bileşenine ayırır ve **değer–zaman eğrisi** (value-time curve) ile açıklar:

```
 İş değeri
   ▲
   │█
   │█▇
   │█▇▆▅
   │█▇▆▅▄▃
   │█▇▆▅▄▃▂▂▁▁▁ ▁ ▁   ▁    ▁
   └─────────────────────────────────────────►  zaman
   ↑    ↑         ↑              ↑
 olay  veri      analiz         karar / aksiyon
       hazır     hazır
   ├────┤─────────┤──────────────┤
   veri   analiz    karar
   gecikmesi gecikmesi gecikmesi
```

- **Veri gecikmesi (data latency):** Olayın yakalanıp analize hazır hâle gelmesi.
- **Analiz gecikmesi (analysis latency):** Verinin bilgiye dönüştürülmesi.
- **Karar gecikmesi (decision latency):** Bilginin aksiyona dönüşmesi. Çoğu zaman insan faktörü nedeniyle en uzun bileşen.

Eğrinin biçimi alana göre değişir: dolandırıcılık tespitinde değer saniyeler içinde sıfıra iner; stratejik planlamada günler boyunca anlamlı kalır. **CI'nın amacı üç gecikmeyi birlikte küçültmektir.** Yalnızca altyapıyı hızlandırmak (veri gecikmesi) yetmez, analiz ve karar da sürece gömülmelidir.

### 1.2 Neden şimdi?

- **Olay kaynaklarının çoğalması:** IoT sensörleri, mobil uygulamalar, tıklama akışları, değişiklik verisi yakalama (CDC).
- **Dayanıklı, yeniden oynatılabilir log'lar:** Kafka ve benzerleri olayları kalıcı ve sıralı biçimde saklayarak akışı "birinci sınıf veri" yaptı.
- **Olgunlaşmış akış motorları:** Durumlu, event-time doğru ve exactly-once garantili işleme artık üretim düzeyinde.
- **Bulut ve yönetilen hizmetler:** Operasyonel karmaşıklığın azalması.
- **Çevrimiçi ML ve karar otomasyonu:** Modelin akış içinde skorlanması ve kararın otomatik uygulanması.

---

## 2. Tanım ve kapsam

**Continuous Intelligence (Sürekli Zekâ)**, gerçek zamanlı analitiğin iş süreçlerinin *içine* gömüldüğü bir tasarım kalıbıdır. Gartner (2019) tanımı:

> "Continuous intelligence is a design pattern in which real-time analytics are integrated within a business operation, processing current and historical data to prescribe actions in response to events. It provides decision automation or decision support. Continuous intelligence leverages multiple technologies such as augmented analytics, event stream processing, optimization, business rule management and ML."

Türkçesiyle: Gerçek zamanlı analitiğin bir iş operasyonunun içine entegre edildiği; güncel ve tarihsel veriyi işleyerek olaylara yanıt olarak **aksiyon reçete eden** (prescribe) bir tasarım kalıbıdır. Karar otomasyonu ya da karar desteği sağlar; artırılmış analitik, olay akışı işleme, optimizasyon, iş kuralı yönetimi ve makine öğrenmesi gibi birden çok teknolojiyi bir araya getirir.

Gartner aynı açıklamada şu öngörüde bulunmuştu: *"By 2022, more than half of major new business systems will incorporate continuous intelligence that uses real-time context data to improve decisions."* Öngörünün tutup tutmadığından bağımsız olarak bu, kavramın BI gündemine girişini işaretler.

Tanımdaki üç vurgu:

1. **Sürekli:** Analiz periyodik (gece batch'i, haftalık rapor) değil, olay akışı boyunca kesintisiz çalışır.
2. **Bağlamsal:** Anlık olay, tarihsel bağlam (müşteri profili, referans veri, model) ile birleştirilir.
3. **Aksiyona dönük:** Çıktı bir dashboard'da beklemekle kalmaz; uyarı, otomatik karar veya iş akışı tetikler.

CI bir ürün değil, bir **mimari yaklaşımdır**: akış işleme, olay tabanlı mimari, karmaşık olay işleme (CEP), çevrimiçi makine öğrenmesi ve karar otomasyonunun bir bileşimi.

### 2.1 İlişkili kavramlar

Literatürde ve endüstride sınırları bulanık birçok terim vardır:

| Kavram | Odak | CI ile ilişkisi |
|---|---|---|
| Real-time BI | Raporların/dashboard'ların düşük gecikmeyle güncellenmesi | CI'nın "görselleştirme" alt kümesi; aksiyon katmanı zayıf |
| Operational Intelligence (OI) | Operasyonel süreçlerin canlı izlenmesi | CI'nın öncülü; daha çok izleme odaklı |
| Streaming analytics | Akış üzerinde hesaplama | CI'nın teknik çekirdeği |
| Event Stream Processing (ESP) | Olay akışlarının sürekli işlenmesi | Teknoloji katmanı |
| Complex Event Processing (CEP) | Olay desenlerinden üst düzey olay türetme | CI'nın analiz araçlarından biri |
| Decision Intelligence | Karar verme sürecinin mühendisliği | CI'nın karar katmanını kuramsallaştırır |
| Digital Twin | Fiziksel sistemin sürekli güncellenen modeli | CI'nın bir uygulama biçimi |
| Augmented Analytics | ML ile otomatik içgörü üretimi | CI'nın analiz katmanını zenginleştirir |

**Ayırt edici özellik:** CI'yı diğerlerinden ayıran, *sürekli analiz* ile *kapalı döngü aksiyonun* birlikte bulunmasıdır.

---

## 3. Tarihsel gelişim

| Dönem | Gelişme | Önemi |
|---|---|---|
| 1980'ler–90'lar | Aktif veri tabanları, tetikleyiciler (ECA kuralları) | "Olay → koşul → aksiyon" fikrinin ilk biçimi |
| 1990'lar sonu | CEP araştırmaları (Luckham, Rapide) | Olay hiyerarşileri ve desen eşleme |
| 2002–2006 | Veri akışı yönetim sistemleri (DSMS): **STREAM** (Stanford), **Aurora/Borealis** (Brown/MIT/Brandeis), **TelegraphCQ** (Berkeley) | Sürekli sorgu (continuous query) kavramı, CQL, yük atma (load shedding) |
| 2004–2008 | Hackathorn'un değer–zaman eğrisi, "operational BI" | İş değeri ile gecikme arasındaki bağ |
| 2010–2011 | **S4** (Yahoo), **Kafka** (LinkedIn) | Dağıtık akış işleme ve dayanıklı log |
| 2011–2014 | **Storm** (Twitter), **Lambda mimarisi** (Marz) | Büyük ölçekli akış; batch + hız katmanı |
| 2013 | **MillWheel** (Google), **Spark Streaming / D-Streams** | Hata toleranslı, durumlu akış; mikro-batch yaklaşımı |
| 2014 | **Kappa mimarisi** (Kreps) | "Her şey bir log" |
| 2015 | **Dataflow modeli** (Google), **Apache Flink** olgunlaşması | Event time, watermark, trigger; birleşik batch/stream |
| 2017–2018 | Kafka exactly-once, Spark **Structured Streaming**, Kafka Streams | Exactly-once'ın yaygınlaşması; akış–tablo ikiliği |
| 2019 | Gartner "Continuous Intelligence" trendi | Kavramın iş dünyasında adlandırılması |
| 2019 → | Streaming SQL standartlaşması, gerçek zamanlı OLAP (Druid, Pinot), streaming lakehouse, çevrimiçi ML | CI'nın erişilebilir hâle gelmesi |

---

## 4. BI'dan CI'ya evrim

| Boyut | Geleneksel BI | Gerçek zamanlı / Operasyonel BI | Continuous Intelligence |
|---|---|---|---|
| Soru | "Ne oldu?" | "Şu an ne oluyor?" | "Şimdi ne yapmalıyız?" |
| Veri | Durağan (data at rest) | Yakın-gerçek-zamanlı | Hareket hâlindeki veri (data in motion) + tarihsel |
| Gecikme | Saat / gün | Saniye / dakika | Milisaniye / saniye |
| İşleme | Batch ETL, DWH | Mikro-batch, CDC | Olay bazlı akış işleme |
| Sorgu modeli | Veriye sorgu gönder | Sık yenilenen sorgu | **Sorgu sürekli çalışır, veri sorgudan akar** |
| Veri modeli | Yıldız/kar tanesi şema | Operasyonel veri deposu (ODS) | Olay log'u + materyalize görünümler |
| Çıktı | Rapor, dashboard | Canlı dashboard | Uyarı, otomatik aksiyon, öneri |
| Analitik türü | Betimleyici | Tanılayıcı | Kestirimci + **reçete edici** (prescriptive) |
| İnsan rolü | Karar verici | Gözlemci + karar verici | Döngüde/denetleyici (human-on-the-loop) |
| Hata maliyeti | Yanlış rapor | Yanlış alarm | Yanlış otomatik aksiyon |

Önemli bir paradigma değişimi: Klasik veri tabanında **veri durur, sorgu gelir**. Akış işlemede **sorgu durur, veri gelir**. Bu tersine çevirme; durum (state) yönetimi, zaman semantiği ve hata toleransı problemlerini merkezî hâle getirir.

### 4.1 Analitik olgunluk modeli

```
 Değer
   ▲                                              ┌───────────────┐
   │                                   ┌──────────┤ Reçete edici  │ "Ne yapmalıyız?"
   │                        ┌──────────┤Kestirimci└───────────────┘    ← CI'nın hedefi
   │             ┌──────────┤Tanılayıcı└─ "Ne olacak?"
   │  ┌──────────┤Betimleyici└─ "Neden oldu?"
   │  │          └─ "Ne oldu?"
   └──┴─────────────────────────────────────────────────────►  Karmaşıklık
```

CI, bu merdivenin tüm basamaklarını **aynı anda ve sürekli** çalıştırır: olayı betimler, bağlamla tanılar, modelle kestirir ve kural/optimizasyonla aksiyon önerir.

---

## 5. CI döngüsü ve gecikme bileşenleri

```
   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
   │  ALGILA  │ →  │  ANALİZ  │ →  │  KARAR   │ →  │  AKSİYON │
   │ (Sense)  │    │(Analyze) │    │ (Decide) │    │  (Act)   │
   └──────────┘    └──────────┘    └──────────┘    └──────────┘
        ↑                                                │
        └──────────────── geri besleme ──────────────────┘
```

Bu döngü, John Boyd'un **OODA** (Observe–Orient–Decide–Act) döngüsüyle ve kontrol teorisindeki kapalı döngü sistemlerle yakından ilişkilidir. Rekabet avantajı, döngüyü rakipten (ya da saldırgandan, arızadan) daha hızlı kapatmaktan gelir.

| Aşama | Görev | Tipik teknoloji | Gecikme kaynağı |
|---|---|---|---|
| Algıla | Olayı yakala, log'a yaz | Sensör, CDC, SDK, mesaj kuyruğu | Ağ, batching, polling aralığı |
| Analiz | Temizle, zenginleştir, pencerele, skorla | Akış motoru, feature store, model | Watermark beklemesi, pencere boyu, model çıkarımı |
| Karar | Kural/politika/optimizasyon uygula | Kural motoru, optimizasyon çözücü | Onay süreçleri, insan müdahalesi |
| Aksiyon | Sistemi değiştir, bildir | API çağrısı, iş akışı motoru | Hedef sistemin yanıt süresi |
| Geri besleme | Aksiyonun sonucunu ölç, modeli güncelle | Etiket toplama, A/B test | Etiketlerin gecikmeli gelmesi |

**Gözlem:** Geri besleme döngüsündeki etiket gecikmesi (ör. bir işlemin dolandırıcılık olduğu haftalar sonra kesinleşir) çevrimiçi öğrenmenin en temel zorluklarından biridir.

---

## 6. Akış işlemenin kuramsal temelleri

### 6.1 Veri modeli

- **Olay (event):** Belirli bir anda gerçekleşmiş, değişmez (immutable) bir olgu. Örn. "Müşteri 4711, 12:00:03'te 250 TL'lik ödeme yaptı."
- **Akış (stream):** Zaman damgalı olayların sınırsız (unbounded), sıralı olmak zorunda olmayan çoklu kümesi. Biçimsel olarak `S = {(e, τ)}`; `τ` olay zamanı.
- **Sınırlı vs sınırsız veri:** Batch, sınırsız verinin özel bir hâlidir (*batch is a special case of streaming*). Bu bakış, aynı programın hem tarihsel hem canlı veride çalışmasını sağlar.

**CQL modeli (Arasu, Babu & Widom, 2006)** akış ve ilişki (relation) arasında üç operatör sınıfı tanımlar:

```
            stream-to-relation              relation-to-relation
  AKIŞ  ───────(pencereler)──────►  İLİŞKİ ─────────(SQL)─────────► İLİŞKİ
    ▲                                                                  │
    └──────────────── relation-to-stream (Istream, Dstream, Rstream) ──┘
```

- **Istream:** İlişkiye eklenen satırların akışı.
- **Dstream:** İlişkiden silinen satırların akışı.
- **Rstream:** Her anda ilişkinin tamamının akışı.

Bu model, bugünkü streaming SQL dillerinin (Flink SQL, Kafka SQL, Spark SQL) kuramsal temelidir.

### 6.2 Akış–tablo ikiliği (stream–table duality)

- Bir **tablo**, bir değişiklik akışının (changelog) belirli bir andaki birikimidir: `tablo = ∫ akış`.
- Bir **akış**, bir tablonun zaman içindeki değişimlerinin türevidir: `akış = d(tablo)/dt`.

Bu ikilik, veri tabanı replikasyonunun, CDC'nin, olay kaynaklamanın (event sourcing) ve materyalize görünümlerin ortak dilidir. CI'da "tarihsel bağlam" genellikle bir akıştan türetilmiş tablo olarak tutulur.

### 6.3 Zaman semantiği

- **Event time:** Olayın kaynağında gerçekleştiği zaman (sensörün, cihazın, uygulamanın zaman damgası).
- **Ingestion time:** Olayın sisteme (mesaj kuyruğu / log) girdiği zaman.
- **Processing time:** Olayın operatör tarafından işlendiği zaman.

```
 processing time
     ▲
     │                        ●  ← geç olay
     │                  ●   ●
     │              ● ●            ideal (skew = 0)
     │          ●  ●         .·´
     │       ● ●        .·´
     │    ●●       .·´
     │  ●     .·´
     │ ●  .·´
     │.·´
     └──────────────────────────────►  event time
        dikey uzaklık = skew (gecikme)
```

Skew; ağ gecikmesi, çevrimdışı cihazlar, yeniden denemeler, saat kaymaları (clock skew) nedeniyle değişkendir ve **öngörülemez**. Doğru analitik için **event time** esas alınmalıdır; processing time yalnızca sistem metrikleri ve bazı operasyonel zaman aşımları için uygundur.

### 6.4 Watermark

Watermark `W(t)`, processing time `t` anında sistemin şu beyanıdır: "Event time'ı `W(t)`'den küçük olan olayların tümünün geldiğine inanıyorum." `W` monoton artandır.

- **Mükemmel watermark:** Girdi hakkında tam bilgi varsa (ör. sıralı log) hiçbir olay geç kalmaz.
- **Sezgisel (heuristic) watermark:** Çoğu gerçek sistemde; tahmindir, bazı olaylar geç kalabilir.

Tipik strateji *bounded out-of-orderness*: `W = max(görülen event time) − δ`.

Watermark, **tamlık (completeness)** ile **gecikme (latency)** arasındaki ödünleşimi yönetir:

| δ | Gecikme | Geç olay oranı | Sonuç |
|---|---|---|---|
| Küçük (agresif) | Düşük | Yüksek | Hızlı ama eksik sonuçlar |
| Büyük (muhafazakâr) | Yüksek | Düşük | Doğru ama geç sonuçlar |

Watermark'tan sonra gelen **geç olaylar (late events)** için seçenekler: atmak, yan çıktıya (side output) yönlendirmek, ya da `allowed lateness` süresince pencereyi güncelleyip düzeltilmiş sonuç yaymak.

**Paralel kaynaklarda** watermark, girdi bölümlerinin watermark'larının **minimumudur**; tek bir boşta (idle) bölüm tüm akışı durdurabilir. Bu, pratikte sık karşılaşılan bir hatadır (idle source detection gerektirir).

### 6.5 Pencereleme (windowing)

| Pencere | Tanım | Örnek |
|---|---|---|
| Tumbling | Sabit, örtüşmeyen | Her 1 dakikadaki toplam satış |
| Sliding / Hopping | Sabit uzunluk, örtüşen | Son 5 dk'nın ortalama yanıt süresi, 30 sn'de bir |
| Session | Hareketsizlik boşluğuyla (gap) kapanan, veri güdümlü | Kullanıcının web sitesindeki oturumu |
| Count-based | Son N olay | Son 100 işlemin ortalaması |
| Global + trigger | Tek pencere, özel tetikleyici | Sipariş tamamlanana kadar biriken özet |

```
 Tumbling  │■■■■│■■■■│■■■■│■■■■│
 Sliding   │■■■■■■■■│
              │■■■■■■■■│
                 │■■■■■■■■│
 Session   │■■■ ■■│      │■■■■ ■│   │■│
                  ←gap→         ←gap→
```

Session pencereleri **birleşebilir (mergeable)**: geç gelen bir olay iki ayrı oturumu tek oturuma bağlayabilir. Bu, durum yönetimini karmaşıklaştırır.

### 6.6 Dataflow modelinin dört sorusu

Akidau vd. (2015) akış hesaplamasını dört ortogonal soruya ayırır:

| Soru | Kavram | Örnek |
|---|---|---|
| **Ne** hesaplanıyor? | Dönüşüm (transformation) | Toplam, ortalama, model skoru |
| Event time'da **nerede**? | Pencere (window) | 1 dakikalık tumbling |
| Processing time'da **ne zaman** yayılıyor? | Tetikleyici (trigger) + watermark | Watermark geçince; ayrıca her 10 sn'de erken sonuç |
| Düzeltmeler **nasıl** ilişkili? | Birikim modu (accumulation) | Discarding / accumulating / retracting |

**Birikim modları:** Bir pencere birden çok kez sonuç yayıyorsa (erken + zamanında + geç):
- **Discarding:** Her yayım yalnızca yeni katkıyı içerir (aşağı akış toplar).
- **Accumulating:** Her yayım o ana kadarki toplamı içerir (aşağı akış üzerine yazar).
- **Accumulating & retracting:** Yeni toplam + bir öncekinin geri çekilmesi (aşağı akışta yeniden gruplama varsa doğruluk için gerekli).

### 6.7 Akışlarda birleştirme (join)

| Join türü | Tanım | Durum gereksinimi | Örnek |
|---|---|---|---|
| Pencereli akış–akış | Aynı penceredeki olaylar | Pencere süresince iki taraf | Tıklama ↔ gösterim |
| Interval join | `a.t ∈ [b.t − x, b.t + y]` | Aralık süresince | Sipariş ↔ 1 saat içindeki ödeme |
| Akış–tablo (lookup) | Olay, güncel referans veriyle | Tablo (veya dış sorgu) | İşlem ↔ müşteri profili |
| Temporal (as-of) join | Olay, *olay anındaki* tablo sürümüyle | Tablonun sürüm geçmişi | İşlem ↔ o anki döviz kuru |
| Sınırsız akış–akış | Tüm geçmişle | Sınırsız (TTL gerekir) | Kaçınılmalı |

**Temporal join**, BI'daki *yavaş değişen boyut* (SCD Type 2) kavramının akıştaki karşılığıdır ve yeniden işleme (reprocessing) sırasında belirlenimci (deterministic) sonuç için kritiktir.

---

## 7. Durum, tutarlılık ve hata toleransı

### 7.1 Durum (state)

Akış işleme çoğu zaman **durum tutan (stateful)** hesaplamadır: anahtar başına son değer, hareketli ortalama, pencere içerikleri, desen eşleme otomatı, model parametreleri.

- **Keyed state:** Anahtara göre bölümlenmiş durum (ör. müşteri ID'si); aynı anahtar her zaman aynı paralel örneğe gider.
- **Operator state:** Paralel örnek başına durum (ör. kaynak ofsetleri).
- **Durum backend'leri:** Bellek içi (hızlı, sınırlı) veya gömülü disk tabanlı (ör. RocksDB; büyük durum, artımlı checkpoint).
- **State TTL:** Sınırsız akışta durumun sınırsız büyümesini önlemek için süre aşımı.

### 7.2 Tutarlı anlık görüntü (checkpoint)

Chandy–Lamport (1985) dağıtık anlık görüntü algoritmasının akış motorlarına uyarlanmış hâli **Asynchronous Barrier Snapshotting (ABS)** olarak bilinir (Carbone vd., 2015):

```
 Kaynak ──[e5][e4]║B1║[e3][e2][e1]──►  Operatör A ──►  Operatör B ──► Hedef
                  ↑
          bariyer (checkpoint n)
```

1. Koordinatör kaynaklara bariyer enjekte eder; kaynak ofsetini kaydeder.
2. Bariyer akışla birlikte ilerler; her operatör tüm girdilerinden bariyeri aldığında durumunu **asenkron** olarak kaydeder (bariyer hizalama).
3. Tüm operatörler onayladığında checkpoint tamamlanır.
4. Hata durumunda: tüm operatörler son checkpoint'e geri döner, kaynak o ofsetten yeniden oynatılır.

Hizalama, backpressure altında gecikmeye yol açabilir; **hizalanmamış (unaligned) checkpoint**'ler uçuştaki (in-flight) verileri de anlık görüntüye ekleyerek bunu azaltır.

### 7.3 Teslim garantileri

| Garanti | Anlamı | Mekanizma | Risk |
|---|---|---|---|
| At-most-once | Olay en fazla bir kez etkiler | Yeniden deneme yok | Veri kaybı |
| At-least-once | En az bir kez etkiler | Yeniden oynatma | Tekrar (duplicate) |
| Exactly-once (durum) | Durum her olaydan tam bir kez etkilenir | Checkpoint + yeniden oynatma | Yok |
| Uçtan uca exactly-once | Dış dünyaya da tam bir kez yansır | Yeniden oynatılabilir kaynak + **idempotent** veya **transactional (2PC)** hedef | Gecikme artışı |

"Exactly-once" aslında **effectively-once**'dır: olay birden çok kez *işlenebilir*, ancak gözlemlenebilir *etkisi* bir kezdir.

**İki aşamalı commit (2PC) hedefleri:** Checkpoint sırasında hedefe yazılan veri "ön commit" edilir; checkpoint tamamlanınca commit edilir. Sonuç: hedefte görünürlük gecikmesi ≈ checkpoint aralığı.

### 7.4 Belirlenimcilik (determinism)

Yeniden oynatmanın doğru sonuç vermesi için hesaplamanın belirlenimci olması gerekir. Belirlenimciliği bozan etkenler: processing time'a bağlı mantık, dış servis çağrıları, rastgelelik, sırası belirsiz birleştirmeler. Bu, **reprocessing** ve **backfill** senaryolarında kritik bir tasarım ilkesidir.

---

## 8. Ölçeklenebilirlik ve performans

- **Bölümleme (partitioning):** Anahtar bazlı hash bölümleme paralelliği sağlar; anahtar içi sırayı korur, anahtarlar arası sırayı korumaz.
- **Veri çarpıklığı (skew / hot keys):** Az sayıda anahtar trafiğin büyük kısmını taşıyorsa (Zipf dağılımı) tek bir paralel örnek darboğaz olur. Çözümler: anahtar tuzlama (salting) + iki aşamalı toplama, yerel ön-toplama (local pre-aggregation).
- **Backpressure:** Aşağı akış yavaşladığında yukarı akışın yavaşlatılması; kredi tabanlı akış kontrolü. Alternatifi **yük atma** (load shedding); DSMS literatüründe (Aurora) kalite–yük ödünleşimi olarak incelenmiştir.
- **Yeniden ölçekleme (rescaling):** Durumlu operatörlerde paralellik değişince durumun yeniden dağıtılması gerekir; anahtarlar sabit sayıda **anahtar grubuna** (key group) atanarak durum blok hâlinde taşınır.
- **Gecikme–throughput ödünleşimi:** Ağ arabellekleri, mikro-batch boyutu ve checkpoint aralığı büyüdükçe throughput artar, gecikme de artar.
- **Operatör zincirleme (operator chaining):** Aynı iş parçacığında art arda operatörler serileştirme maliyetini ortadan kaldırır.

---

## 9. Akış üzerinde analitik

Akışta veriye **tek geçiş** (single pass), **sınırlı bellek** ve **olay başına sınırlı süre** kısıtları altında erişilir. Bu, klasik algoritmaların yeniden tasarlanmasını gerektirir.

### 9.1 Akış algoritmaları ve özet yapılar (sketches)

| Problem | Yapı | Bellek | Hata garantisi |
|---|---|---|---|
| Rastgele örneklem | Reservoir sampling (Vitter) | O(k) | Tam düzgün örneklem |
| Farklı eleman sayısı | HyperLogLog | O(log log n) / kayıtçık | ~1.04/√m göreli standart hata |
| Frekans tahmini | Count-Min Sketch | O((1/ε)·log(1/δ)) | Fazla tahmin ≤ εN, olasılık ≥ 1−δ |
| En sık elemanlar | Space-Saving, Misra–Gries | O(k) | Deterministik sınır |
| Kantiller (p95, p99) | t-digest, KLL, DDSketch | Alt-doğrusal | Uçlarda yüksek doğruluk / göreli hata |
| Üyelik | Bloom filter | O(n) bit | Yanlış pozitif, yanlış negatif yok |
| Hareketli istatistik | EWMA, Welford algoritması | O(1) | Sayısal kararlı ortalama/varyans |

Bu yapıların çoğu **birleştirilebilirdir (mergeable)**: paralel örneklerdeki özetler birleştirilerek global özet elde edilir. Bu, dağıtık akış işleme için temel bir özelliktir.

### 9.2 Akışta anomali tespiti

| Yaklaşım | Yöntem | Güçlü yön | Zayıf yön |
|---|---|---|---|
| Kural tabanlı | Eşik, iş kuralı | Açıklanabilir, hızlı | Bakım maliyeti, bilinmeyeni yakalamaz |
| İstatistiksel | z-skoru, EWMA kontrol kartları, CUSUM | Basit, düşük maliyet | Durağanlık varsayımı |
| Mevsimsellik farkında | STL ayrıştırma, Holt–Winters | Periyodik desenler | Pencere/periyot seçimi |
| Ağaç tabanlı akış | Robust Random Cut Forest, Half-Space Trees | Çok değişkenli, akışa uygun | Yorumlanabilirlik |
| Derin öğrenme | Otokodlayıcı, LSTM tahmin hatası | Karmaşık desenler | Eğitim maliyeti, drift |

**Değerlendirme notu:** Akışta anomali tespiti yalnızca precision/recall ile değil, **tespit süresi (time-to-detect)** ve **erken uyarı** ile de değerlendirilmelidir.

### 9.3 Çevrimiçi öğrenme ve concept drift

- **Çevrimiçi (online / incremental) öğrenme:** Model her olayla (veya mini-batch ile) güncellenir. Örnekler: Hoeffding ağaçları (VFDT), çevrimiçi lojistik regresyon (SGD), adaptif rastgele orman.
- **Prequential değerlendirme** (test-then-train): Her örnek önce test için, sonra eğitim için kullanılır.
- **Concept drift:** `P(X, y)` dağılımının zamanla değişmesi.

| Drift türü | Tanım |
|---|---|
| Ani (sudden) | Dağılım bir anda değişir (ör. yeni regülasyon) |
| Kademeli (gradual) | Eski ve yeni kavram bir süre birlikte görülür |
| Artımlı (incremental) | Yavaş, sürekli kayma |
| Tekrarlayan (recurring) | Eski kavram geri döner (mevsimsellik) |
| Sanal (virtual) | Yalnızca `P(X)` değişir, `P(y|X)` aynı kalır |

**Drift dedektörleri:** DDM, EDDM, ADWIN (uyarlanabilir pencere), Page–Hinkley testi.

**Pratik desen:** Ağır modeller batch'te eğitilir, akışta **skorlanır** (model serving); hafif adaptasyon (eşik kalibrasyonu, drift alarmı) akışta yapılır. Tutarlılık için **feature store** hem eğitim hem çıkarımda aynı özellik tanımlarını sağlar (training–serving skew'i önler).

### 9.4 Karmaşık olay işleme (CEP)

CEP, düşük seviyeli olay dizilerinden **anlamlı üst düzey olaylar** türetir. Desen operatörleri:

| Operatör | Anlam |
|---|---|
| Sıra (SEQ) | A'yı B izler |
| Ardışıklık | Kesintisiz (strict) veya araya olay girebilir (relaxed) |
| Yineleme | A{3}, A+ |
| Olumsuzlama | A'dan sonra belirli süre B *gelmedi* |
| Zaman kısıtı | WITHIN 5 dakika |
| Seçim politikası | Skip-till-next-match, skip-till-any-match |

Örnek desen (hesap ele geçirme şüphesi): *"3 başarısız giriş → 1 dk içinde farklı ülkeden başarılı giriş → şifre değişikliği."*

CEP desenleri genellikle **sonlu durum otomatları** (NFA) olarak derlenir; olumsuzlama ve zaman aşımı zamanlayıcı (timer) gerektirir. SQL dünyasında karşılığı `MATCH_RECOGNIZE` (SQL:2016) yapısıdır.

---

## 10. Karar ve aksiyon katmanı

CI'yı streaming analytics'ten ayıran katmandır.

### 10.1 Karar mekanizmaları

| Mekanizma | Açıklama | Örnek |
|---|---|---|
| İş kuralları | Bildirimsel kurallar (ör. DMN karar tabloları, kural motorları) | "Skor > 0.9 ve tutar > 10.000 TL → işlemi beklet" |
| Skor + eşik | Model çıktısının maliyet duyarlı eşiklenmesi | Yanlış pozitif/negatif maliyetine göre eşik |
| Optimizasyon | Kısıtlar altında en iyi aksiyon | Dinamik fiyat, kaynak/filo atama |
| Pekiştirmeli öğrenme / bandit | Aksiyon–ödül geri beslemesiyle öğrenme | Öneri, fiyat deneyleri |
| İnsan onayı | Vaka yönetim sistemine yönlendirme | Analist incelemesi |

**Maliyet duyarlı karar:** Optimal eşik, sınıflandırıcının doğruluğundan değil, aksiyonun **beklenen faydasından** türetilmelidir:
`aksiyon al ⟺ p(olay) · fayda(doğru pozitif) > (1 − p(olay)) · maliyet(yanlış pozitif)`

### 10.2 Otomasyon seviyeleri

Parasuraman, Sheridan & Wickens (2000) otomasyonu bilgi işlemenin dört aşamasında (bilgi edinme, analiz, karar seçimi, aksiyon uygulama) ayrı ayrı derecelendirir. CI için sadeleştirilmiş bir ölçek:

| Seviye | Sistem | İnsan |
|---|---|---|
| 0 | Yalnızca veri gösterir | Her şeyi yapar |
| 1 | Uyarı üretir | Yorumlar, karar verir |
| 2 | Aksiyon önerir | Onaylar / reddeder (human-in-the-loop) |
| 3 | Aksiyonu uygular, insan veto edebilir | Denetler (human-on-the-loop) |
| 4 | Tam otomatik | Yalnızca politika belirler (human-out-of-the-loop) |

Seviye seçimi; aksiyonun **geri alınabilirliği**, hata maliyeti, düzenleyici gereksinimler ve sistemin kanıtlanmış doğruluğuna bağlıdır.

### 10.3 Aksiyonun güvenliği

- **İdempotent aksiyonlar:** Yeniden oynatmada aynı aksiyon iki kez uygulanmamalı (aksiyon kimliği ile).
- **Devre kesici (circuit breaker):** Anormal aksiyon hacminde otomasyonu durdurma.
- **Gölge mod (shadow mode):** Yeni kararlar uygulanmadan kaydedilir ve mevcut sistemle karşılaştırılır.
- **Denetim izi (audit trail):** Hangi olay, hangi model sürümü, hangi kural → hangi aksiyon.

---

## 11. Referans mimariler ve teknoloji ekosistemi

### 11.1 Lambda mimarisi (Marz)

```
              ┌──► Batch katmanı (tüm geçmiş, doğru, yavaş) ──► Batch görünümleri ──┐
 Olaylar ─────┤                                                                     ├──► Sunum katmanı
              └──► Hız katmanı (son veriler, yaklaşık, hızlı) ──► Gerçek zamanlı ───┘
```

Dezavantaj: aynı iş mantığının iki farklı sistemde yazılıp bakımı ve iki sonucun uzlaştırılması.

### 11.2 Kappa mimarisi (Kreps)

```
 Olaylar ──► Dayanıklı log (uzun saklama) ──► Akış işleyici v1 ──► Sunum
                         │
                         └── yeniden oynat ──► Akış işleyici v2 ──► Sunum (yeni)
```

Her şey bir akıştır; yeniden hesaplama gerektiğinde log baştan oynatılır. Ön koşullar: uzun saklama süreli log, event-time doğru ve belirlenimci işleme.

### 11.3 Genel CI mimarisi

```
 ┌───────────┐   ┌───────────────┐   ┌──────────────────┐   ┌──────────────────┐
 │ Kaynaklar │   │ Olay alımı    │   │ Akış işleme      │   │ Karar / Aksiyon  │
 │ • IoT     │ → │ (dayanıklı,   │ → │ • doğrulama      │ → │ • kural/politika │
 │ • uygulama│   │  bölümlenmiş, │   │ • zenginleştirme │   │ • uyarı          │
 │ • CDC     │   │  yeniden      │   │ • pencereleme    │   │ • otomatik aksiyon│
 │ • API     │   │  oynatılabilir│   │ • CEP / ML skor  │   │ • vaka yönetimi  │
 └───────────┘   │  log)         │   └────────┬─────────┘   └────────┬─────────┘
                 └───────────────┘            │                      │
                        ▲            ┌────────┴─────────┐   ┌────────┴─────────┐
                        │            │ Tarihsel bağlam  │   │ Sunum            │
                        │            │ (DWH, feature    │   │ (gerçek zamanlı  │
                        │            │  store, modeller)│   │  OLAP, dashboard,│
                        │            └──────────────────┘   │  data lake)      │
                        │                                   └──────────────────┘
                        └──────────── aksiyon sonuçları / etiketler (geri besleme) ──┘
```

### 11.4 Teknoloji ekosistemi

| Katman | Örnek teknolojiler |
|---|---|
| Olay alımı / log | Apache Kafka, Amazon Kinesis Data Streams, Apache Pulsar, Google Pub/Sub, Azure Event Hubs, Redpanda |
| CDC | Debezium, AWS DMS, Flink CDC |
| Akış işleme | Apache Flink, Spark Structured Streaming, Kafka Streams, Apache Beam (Google Dataflow), RisingWave, Materialize |
| CEP | Flink CEP, Esper, `MATCH_RECOGNIZE` |
| Gerçek zamanlı OLAP | Apache Druid, Apache Pinot, ClickHouse |
| Streaming lakehouse | Apache Iceberg, Delta Lake, Apache Hudi, Apache Paimon |
| Feature store / model serving | Feast, Tecton; Seldon, KServe |
| Çevrimiçi ML | River (Python), MOA (Java) |
| Yönetilen hizmetler | Amazon Managed Service for Apache Flink, Confluent Cloud, Databricks, Google Dataflow |

### 11.5 Streaming SQL örnekleri

Aşağıdaki örnekler Flink SQL sözdizimindedir; kavramlar diğer motorlara aktarılabilir.

**Event time ve watermark tanımı:**

```sql
CREATE TABLE payments (
  payment_id   STRING,
  customer_id  STRING,
  merchant_id  STRING,
  amount       DECIMAL(12, 2),
  country      STRING,
  event_time   TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH ( ... );
```

**Tumbling pencere ile toplama:**

```sql
SELECT window_start, window_end, merchant_id,
       COUNT(*)    AS tx_count,
       SUM(amount) AS tx_total
FROM TABLE(
  TUMBLE(TABLE payments, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, merchant_id;
```

**Temporal join (olay anındaki kur):**

```sql
SELECT p.payment_id, p.amount * r.rate AS amount_eur
FROM payments AS p
JOIN fx_rates FOR SYSTEM_TIME AS OF p.event_time AS r
  ON p.currency = r.currency;
```

**CEP (`MATCH_RECOGNIZE`):**

```sql
SELECT *
FROM logins
MATCH_RECOGNIZE (
  PARTITION BY user_id
  ORDER BY event_time
  MEASURES FIRST(F.event_time) AS first_fail,
           S.country           AS new_country
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (F{3} S) WITHIN INTERVAL '1' MINUTE
  DEFINE
    F AS F.success = FALSE,
    S AS S.success = TRUE AND S.country <> LAST(F.country)
);
```

---

## 12. Değerlendirme metrikleri

| Kategori | Metrik | Not |
|---|---|---|
| Gecikme | Uçtan uca gecikme (p50, p95, p99, p99.9) | Ortalama yanıltıcıdır; kuyruk gecikmesi önemlidir |
| | Olay zamanı gecikmesi (event-time lag) | `şimdi − watermark` |
| | Tüketici gecikmesi (consumer lag) | Log'da işlenmemiş olay sayısı/süresi |
| Throughput | Olay/sn, MB/sn (sürdürülebilir) | Gecikme SLA'sı altında ölçülmeli |
| Tazelik | **Bilgi yaşı (Age of Information)** | `Δ(t) = t − u(t)`; `u(t)`: elde edilen en güncel bilginin üretim zamanı |
| Doğruluk | Batch referansına göre sapma | Watermark ve geç olay politikasının etkisi |
| | Geç olay / atılan olay oranı | |
| Tespit kalitesi | Precision, recall, F1, PR-AUC | Dengesiz sınıflarda ROC-AUC yanıltıcı olabilir |
| | Time-to-detect, erken uyarı süresi | Akışa özgü |
| Karar kalitesi | Aksiyon başına iş değeri, önlenen kayıp | Nihai başarı ölçütü |
| | Uyarı hacmi, yanlış alarm oranı | Uyarı yorgunluğu |
| Dayanıklılık | Kurtarma süresi (recovery time), checkpoint süresi | Hata enjeksiyonu ile ölçülür |
| Maliyet | Olay başına maliyet, kaynak kullanımı | |

**Kıyaslama (benchmark) notu:** Yahoo Streaming Benchmark, Nexmark (Beam/Flink) ve benzeri kıyaslamalar vardır; ancak sonuçlar iş yüküne, yapılandırmaya ve ölçüm yöntemine (koordineli ihmal, *coordinated omission*) çok duyarlıdır. Bilimsel çalışmalarda ölçüm metodolojisi açıkça raporlanmalıdır.

---

## 13. Veri kalitesi ve yönetişim

### 13.1 Akışta veri kalitesi boyutları

| Boyut | Akıştaki karşılığı | Önlem |
|---|---|---|
| Tamlık | Eksik alanlar, kayıp olaylar, sinyal boşlukları | Şema doğrulama, boşluk tespiti (timer) |
| Geçerlilik | Aralık dışı/biçimsiz değerler | Kural tabanlı doğrulama, side output (dead-letter) |
| Tutarlılık | Çelişen kaynaklar, sıra ihlalleri | Event-time sıralama, uzlaştırma |
| Benzersizlik | Tekrar eden olaylar | Olay kimliği ile durumlu deduplication |
| Zamanlılık | Geç olaylar, saat kayması | Watermark, gecikme izleme |
| Doğruluk | Gerçeği yansıtma | Referans veriyle çapraz kontrol |

**Tasarım ilkesi:** Kötü veri *sessizce atılmamalı*; yan çıktıya yönlendirilip sayılmalı ve izlenmelidir. Veri kalitesi metrikleri de birer akıştır.

### 13.2 Şema evrimi

Olay üreticileri ve tüketicileri bağımsız sürümlenir. **Şema kayıt defteri** (schema registry) ve uyumluluk kuralları (geriye/ileriye uyumluluk) ile Avro/Protobuf gibi biçimler kullanılır. **Veri sözleşmeleri** (data contracts) üretici ile tüketici arasındaki beklentileri biçimselleştirir.

### 13.3 Yönetişim, gizlilik ve etik

- **Köken (lineage):** Hangi kararın hangi olaylardan ve model sürümünden türediği.
- **Gizlilik:** KVKK/GDPR kapsamında kişisel verinin akışta maskelenmesi, amaçla sınırlılık, saklama süresi; "unutulma hakkı"nın değişmez log'larla gerilimi (ör. crypto-shredding).
- **Otomatik karar hakları:** GDPR Madde 22: yalnızca otomatik işlemeye dayalı ve kişiyi önemli ölçüde etkileyen kararlara itiraz ve insan müdahalesi hakkı.
- **Açıklanabilirlik ve adalet:** Gerçek zamanlı kararlar da yanlılık denetiminden geçmelidir.

---

## 14. Uygulama alanları

| Alan | CI kullanım örneği | Kritik gereksinim |
|---|---|---|
| Finans | İşlem anında dolandırıcılık tespiti, algoritmik risk limitleri, piyasa gözetimi | Milisaniye gecikme, denetlenebilirlik |
| E-ticaret / perakende | Gerçek zamanlı öneri, dinamik fiyatlama, stok uyarısı, sepet terk müdahalesi | Kişiselleştirme, A/B test |
| Lojistik / ulaşım | Filo takibi, rota sapması, ETA tahmini, yoğunluk izleme | Coğrafi-zamansal işleme |
| Üretim (Endüstri 4.0) | Kestirimci bakım, kalite sapması tespiti, dijital ikiz | Yüksek frekanslı sensör verisi, edge işleme |
| Enerji | Şebeke dengeleme, talep tahmini, arıza tespiti | Güvenilirlik, düzenleme |
| Telekom | Ağ anomalisi, abone kaybı (churn) sinyalleri, kapasite yönetimi | Çok yüksek hacim |
| Sağlık | Hasta monitörlerinden erken uyarı (ör. sepsis), salgın gözetimi | Yanlış alarm yorgunluğu, gizlilik |
| Siber güvenlik | Saldırı desenlerinin gerçek zamanlı korelasyonu (SIEM/SOAR) | CEP, otomatik müdahale |
| Akıllı şehir | Trafik sinyal optimizasyonu, toplu taşıma yönetimi | Çok kaynaklı veri birleştirme |
| Medya / oyun | Canlı etkileşim analitiği, hile tespiti | Ölçek, düşük gecikme |

---

## 15. Zorluklar ve açık problemler

- **Doğruluk vs gecikme:** Eksik veriyle erken karar mı, tam veriyle geç karar mı? Bu ödünleşimin biçimsel optimizasyonu çoğu zaman sezgisel yapılır.
- **Veri kalitesi:** Bozuk, eksik, tekrar eden ve sırasız olaylar akışta düzeltilmek zorunda; ground truth çoğu zaman yoktur.
- **Durum büyümesi:** Sınırsız akışta durumun sınırlı tutulması (TTL, sıkıştırma, özet yapılar).
- **Tarihsel bağlamla birleştirme:** Değişen referans verilerle zaman-tutarlı join.
- **Yeniden işleme (reprocessing):** İş mantığı değiştiğinde geçmişin yeniden hesaplanması; belirlenimcilik, maliyet ve geçiş (cutover) yönetimi.
- **Etiket gecikmesi ve geri besleme:** Aksiyonların sonuçları geç ve kısmi gözlemlenir; aksiyon, gözlenen veriyi de değiştirir (*performative prediction*, seçim yanlılığı).
- **Test edilebilirlik:** Zamana bağlı, durumlu, dağıtık sistemlerin testi; olay zamanı simülasyonu ve sentetik veri üretimi.
- **Operasyonel karmaşıklık:** Sürüm yükseltme, şema evrimi, durumun taşınması (savepoint uyumluluğu).
- **Güven ve yönetişim:** Otomatik aksiyonların açıklanabilirliği, sorumluluk ve denetlenebilirliği.
- **Edge–bulut sürekliliği:** Hesaplamanın nerede yapılacağı (cihaz, edge, bulut) ve buna bağlı gecikme–maliyet–gizlilik dengesi.

---

## 16. Araştırma soruları

1. **Gecikme–doğruluk ödünleşimi:** Watermark stratejisi ile anomali tespitinin precision/recall'u ve *time-to-detect* metriği arasındaki ilişki nasıl modellenebilir? Uyarlanabilir (adaptive) watermark'lar sabit δ'dan ne ölçüde üstündür?
2. **Yaklaşık hesaplama:** Sketch'ler ile kaynak tüketimi ve hata sınırları arasındaki denge; yaklaşık sonuçların karar kalitesine etkisi.
3. **Çevrimiçi öğrenme ve concept drift:** Mevsimsellik ve olağanüstü olaylar altında modellerin uyarlanması; drift tespiti ile yanlış alarm arasındaki denge.
4. **Akışta veri kalitesi:** Bozuk/eksik kayıtlar ne zaman atılmalı, ne zaman düzeltilmeli (imputation)? Kalitenin aşağı akış kararlarına etkisi nasıl nicelendirilir?
5. **CI sistemlerinin değerlendirilmesi:** Teknik metrikler (gecikme, throughput) ile iş metrikleri (önlenen kayıp) nasıl tek bir çerçevede ilişkilendirilir? Sentetik veri ile gerçek veri arasındaki genelleme farkı nedir?
6. **İnsan–makine etkileşimi:** Uyarı yorgunluğu, açıklanabilirlik ve otomasyon derecesinin karar kalitesine etkisi.
7. **Pull tabanlı kaynaklardan akış üretimi:** Polling frekansı, maliyet ve bilgi tazeliği (*Age of Information*) optimizasyonu.
8. **Karar–veri geri besleme döngüsü:** Otomatik aksiyonların gelecekteki veri dağılımını değiştirdiği durumlarda değerlendirme ve öğrenme (karşı-olgusal değerlendirme, off-policy öğrenme).
9. **Streaming SQL'in ifade gücü:** Hangi analitik görevler bildirimsel olarak ifade edilebilir, hangileri düşük seviyeli API gerektirir?

---

## 17. Tartışma ve alıştırma soruları

1. Kendi sektörünüzden bir süreç seçin; Hackathorn'un üç gecikme bileşenini tahmin edin. Hangisi baskın? CI hangisini en çok azaltabilir?
2. Bir mobil uygulamada kullanıcılar çevrimdışıyken olaylar cihazda birikiyor ve saatler sonra gönderiliyor. Saatlik aktif kullanıcı sayısı için watermark ve geç olay politikasını tasarlayın. Hangi birikim modunu seçersiniz, neden?
3. Lambda ve Kappa mimarilerini; geliştirme maliyeti, doğruluk, yeniden işleme süresi ve operasyonel karmaşıklık açısından karşılaştırın. Hangi koşullarda Lambda hâlâ tercih edilebilir?
4. Uçtan uca exactly-once için gereken üç koşulu açıklayın. Hedef bir e-posta servisi ise ne yapılabilir?
5. Bir dolandırıcılık modelinin precision'ı %60, recall'ı %80. Yanlış pozitif başına 5 TL müşteri deneyimi maliyeti, yanlış negatif başına ortalama 2.000 TL kayıp varsa otomasyon seviyesi ve eşik nasıl belirlenmeli?
6. Count-Min Sketch ile `ε = 0.001`, `δ = 0.01` için gereken genişlik ve derinliği hesaplayın (`w = ⌈e/ε⌉`, `d = ⌈ln(1/δ)⌉`).
7. Bir session penceresinde geç gelen tek bir olay iki oturumu birleştiriyor. Aşağı akıştaki "ortalama oturum süresi" metriği nasıl etkilenir? Retraction olmadan sonuç neden yanlış olur?
8. GDPR Madde 22 bağlamında, tam otomatik bir kredi limiti artırma sistemi için hangi güvenceler gereklidir?

---

## 18. Sözlük

| Terim | Türkçe karşılık / açıklama |
|---|---|
| Event time | Olay zamanı |
| Ingestion time | Alım zamanı |
| Processing time | İşlenme zamanı |
| Skew | Olay zamanı ile işlenme zamanı arasındaki fark |
| Watermark | Su işareti; event time ilerleme beyanı |
| Late event | Geç olay |
| Allowed lateness | İzin verilen gecikme |
| Trigger | Tetikleyici; sonucun ne zaman yayılacağı |
| Accumulation mode | Birikim modu (discarding / accumulating / retracting) |
| Window (tumbling/sliding/session) | Pencere (ardışık/kayan/oturum) |
| Keyed state | Anahtarlı durum |
| State backend | Durum deposu |
| Checkpoint / savepoint | Kontrol noktası / elle alınan kayıt noktası |
| Barrier | Bariyer; checkpoint işaretçisi |
| Backpressure | Geri basınç |
| Load shedding | Yük atma |
| Exactly-once | Tam olarak bir kez işleme (effectively-once) |
| Idempotent | Tekrar uygulandığında sonucu değiştirmeyen |
| Partition / shard | Bölüm; paralellik birimi |
| Partition key | Bölümleme anahtarı |
| Hot key | Sıcak anahtar; aşırı yüklenen bölüm |
| Changelog | Değişiklik akışı |
| CDC (Change Data Capture) | Değişiklik verisi yakalama |
| Event sourcing | Olay kaynaklama |
| Temporal join | Zamansal birleştirme (as-of join) |
| CEP | Karmaşık olay işleme |
| Sketch | Özet veri yapısı (olasılıksal) |
| Concept drift | Kavram kayması |
| Prequential evaluation | Önce test et, sonra eğit değerlendirmesi |
| Feature store | Özellik deposu |
| Age of Information (AoI) | Bilgi yaşı |
| Dead-letter queue | Hatalı kayıt kuyruğu |
| Data contract | Veri sözleşmesi |
| Human-in/on-the-loop | Döngüde / döngü üzerinde insan |

---

## 19. Kaynaklar

> Tüm künyeler Ekim 2026'da yayıncı sayfaları, DOI kayıtları, arXiv ve yazar sayfaları üzerinden doğrulanmıştır.

**Veri akışı sistemleri: temel çalışmalar**
- Babcock, B., Babu, S., Datar, M., Motwani, R. & Widom, J. (2002). *Models and Issues in Data Stream Systems.* Proc. 21st ACM PODS, 1–16. doi:10.1145/543613.543615
- Abadi, D. J., Carney, D., Çetintemel, U., Cherniack, M., Convey, C., Lee, S., Stonebraker, M., Tatbul, N. & Zdonik, S. (2003). *Aurora: A New Model and Architecture for Data Stream Management.* The VLDB Journal 12(2), 120–139.
- Chandrasekaran, S. et al. (2003). *TelegraphCQ: Continuous Dataflow Processing for an Uncertain World.* Proc. 1st CIDR, Asilomar, CA.
- Abadi, D. J. et al. (2005). *The Design of the Borealis Stream Processing Engine.* Proc. 2nd CIDR, 277–289.
- Arasu, A., Babu, S. & Widom, J. (2006). *The CQL Continuous Query Language: Semantic Foundations and Query Execution.* The VLDB Journal 15(2), 121–142. doi:10.1007/s00778-004-0147-z
- Cugola, G. & Margara, A. (2012). *Processing Flows of Information: From Data Stream to Complex Event Processing.* ACM Computing Surveys 44(3), Article 15. doi:10.1145/2187671.2187677

**Modern akış işleme**
- Neumeyer, L., Robbins, B., Nair, A. & Kesari, A. (2010). *S4: Distributed Stream Computing Platform.* Proc. IEEE ICDM Workshops, 170–177. doi:10.1109/ICDMW.2010.172
- Kreps, J., Narkhede, N. & Rao, J. (2011). *Kafka: A Distributed Messaging System for Log Processing.* Proc. NetDB Workshop, Athens, 1–7.
- Akidau, T. et al. (2013). *MillWheel: Fault-Tolerant Stream Processing at Internet Scale.* PVLDB 6(11), 1033–1044. doi:10.14778/2536222.2536229
- Zaharia, M., Das, T., Li, H., Hunter, T., Shenker, S. & Stoica, I. (2013). *Discretized Streams: Fault-Tolerant Streaming Computation at Scale.* Proc. 24th ACM SOSP, 423–438. doi:10.1145/2517349.2522737
- Toshniwal, A. et al. (2014). *Storm@twitter.* Proc. ACM SIGMOD, 147–156. doi:10.1145/2588555.2595641
- Akidau, T. et al. (2015). *The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing.* PVLDB 8(12), 1792–1803. doi:10.14778/2824032.2824076
- Carbone, P., Katsifodimos, A., Ewen, S., Markl, V., Haridi, S. & Tzoumas, K. (2015). *Apache Flink: Stream and Batch Processing in a Single Engine.* Bulletin of the IEEE Computer Society Technical Committee on Data Engineering 38(4), 28–38.
- Carbone, P., Fóra, G., Ewen, S., Haridi, S. & Tzoumas, K. (2015). *Lightweight Asynchronous Snapshots for Distributed Dataflows.* arXiv:1506.08603.
- Carbone, P., Ewen, S., Fóra, G., Haridi, S., Richter, S. & Tzoumas, K. (2017). *State Management in Apache Flink: Consistent Stateful Distributed Stream Processing.* PVLDB 10(12), 1718–1729. doi:10.14778/3137765.3137777
- Armbrust, M. et al. (2018). *Structured Streaming: A Declarative API for Real-Time Applications in Apache Spark.* Proc. ACM SIGMOD, 601–613. doi:10.1145/3183713.3190664
- Sax, M. J., Wang, G., Weidlich, M. & Freytag, J.-C. (2018). *Streams and Tables: Two Sides of the Same Coin.* Proc. BIRTE 2018, Article 1, 1–10.
- Begoli, E., Akidau, T., Hueske, F., Hyde, J., Knight, K. & Knowles, K. (2019). *One SQL to Rule Them All: An Efficient and Syntactically Idiomatic Approach to Management of Streams and Tables.* Proc. ACM SIGMOD, 1757–1772. doi:10.1145/3299869.3314040
- Fragkoulis, M., Carbone, P., Kalavri, V. & Katsifodimos, A. (2024). *A Survey on the Evolution of Stream Processing Systems.* The VLDB Journal 33(2), 507–541. doi:10.1007/s00778-023-00819-8

**Dağıtık sistemler**
- Chandy, K. M. & Lamport, L. (1985). *Distributed Snapshots: Determining Global States of Distributed Systems.* ACM Transactions on Computer Systems 3(1), 63–75. doi:10.1145/214451.214456

**Akış algoritmaları ve çevrimiçi öğrenme**
- Vitter, J. S. (1985). *Random Sampling with a Reservoir.* ACM Transactions on Mathematical Software 11(1), 37–57. doi:10.1145/3147.3165
- Domingos, P. & Hulten, G. (2000). *Mining High-Speed Data Streams.* Proc. 6th ACM SIGKDD (KDD 2000), 71–80. doi:10.1145/347090.347107
- Cormode, G. & Muthukrishnan, S. (2005). *An Improved Data Stream Summary: The Count-Min Sketch and its Applications.* Journal of Algorithms 55(1), 58–75. doi:10.1016/j.jalgor.2003.12.001
- Flajolet, P., Fusy, É., Gandouet, O. & Meunier, F. (2007). *HyperLogLog: The Analysis of a Near-Optimal Cardinality Estimation Algorithm.* AofA 2007, DMTCS Proceedings AH, 127–146. doi:10.46298/dmtcs.3545
- Bifet, A. & Gavaldà, R. (2007). *Learning from Time-Changing Data with Adaptive Windowing.* Proc. SIAM SDM, 443–448. doi:10.1137/1.9781611972771.42
- Gama, J., Žliobaitė, I., Bifet, A., Pechenizkiy, M. & Bouchachia, A. (2014). *A Survey on Concept Drift Adaptation.* ACM Computing Surveys 46(4), Article 44. doi:10.1145/2523813
- Guha, S., Mishra, N., Roy, G. & Schrijvers, O. (2016). *Robust Random Cut Forest Based Anomaly Detection on Streams.* Proc. ICML, PMLR 48, 2712–2721.
- Dunning, T. & Ertl, O. (2019). *Computing Extremely Accurate Quantiles Using t-Digests.* arXiv:1902.04023.

**Karar, otomasyon ve tazelik**
- Sheridan, T. B. & Verplank, W. L. (1978). *Human and Computer Control of Undersea Teleoperators.* Technical Report, MIT Man-Machine Systems Laboratory, Cambridge, MA.
- Parasuraman, R., Sheridan, T. B. & Wickens, C. D. (2000). *A Model for Types and Levels of Human Interaction with Automation.* IEEE Transactions on Systems, Man, and Cybernetics, Part A 30(3), 286–297. doi:10.1109/3468.844354
- Kaul, S., Yates, R. & Gruteser, M. (2012). *Real-Time Status: How Often Should One Update?* Proc. IEEE INFOCOM, 2731–2735. doi:10.1109/INFCOM.2012.6195689

**İş zekâsı perspektifi**
- Hackathorn, R. (2004). *The BI Watch: Real-Time to Real-Value.* DM Review 14(1).
- Gartner (2019, 18 Şubat). *Gartner Identifies Top 10 Data and Analytics Technology Trends for 2019* [Basın bülteni]. <https://www.gartner.com/en/newsroom/press-releases/2019-02-18-gartner-identifies-top-10-data-and-analytics-technolo>

**Kitaplar**
- Luckham, D. C. (2002). *The Power of Events: An Introduction to Complex Event Processing in Distributed Enterprise Systems.* Addison-Wesley.
- Marz, N. & Warren, J. (2015). *Big Data: Principles and Best Practices of Scalable Realtime Data Systems.* Manning.
- Kleppmann, M. (2017). *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems* (1. baskı). O'Reilly Media. (Bölüm 11: Stream Processing)
- Akidau, T., Chernyak, S. & Lax, R. (2018). *Streaming Systems: The What, Where, When, and How of Large-Scale Data Processing.* O'Reilly Media.
- Bifet, A., Gavaldà, R., Holmes, G. & Pfahringer, B. (2018). *Machine Learning for Data Streams: With Practical Examples in MOA.* MIT Press. doi:10.7551/mitpress/10654.001.0001
- Hueske, F. & Kalavri, V. (2019). *Stream Processing with Apache Flink: Fundamentals, Implementation, and Operation of Streaming Applications.* O'Reilly Media.

**Diğer**
- Kreps, J. (2013, 16 Aralık). *The Log: What Every Software Engineer Should Know About Real-Time Data's Unifying Abstraction.* LinkedIn Engineering Blog. <https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying>
- Kreps, J. (2014). *Questioning the Lambda Architecture.* O'Reilly Radar. <https://www.oreilly.com/radar/questioning-the-lambda-architecture/>
