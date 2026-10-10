# SyncSphere — Gerçek Zamanlı İşbirlikçi Dashboard Backend'i

[English](README.md) · **Türkçe**

ERP/CRM'den gelen canlı veri değişikliklerini, birden çok sunucu düğümüne yayılmış binlerce bağlı tarayıcıya **sıralı, kayıpsız ve düşük gecikmeyle** iten bir backend.
ASP.NET Core SignalR + Redis backplane, .NET 10.

SignalR + Redis backplane "at-most-once" bir taşıyıcıdır: mesaj kaybolabilir, sıra karışabilir. Bu repo onun üstüne, sürüm numaraları ve tekrar oynatma günlüğüyle,
**istemcinin kayıp ve sıra bozulmasını fark edip kendini onardığı** bir akış kuruyor ve bunu kasıtlı mesaj düşürerek test ediyor.

## Neler yapıyor

| Alan | Yaklaşım | Kanıt |
|---|---|---|
| **Çok düğümlü yayın** | Hub grupları + Redis backplane; bir düğümde alınan değişiklik tüm düğümlerdeki istemcilere ulaşır | Test: 2 düğüm, 200 istemci, 200 güncelleme → hepsinin durumu sunucu anlık görüntüsüyle birebir aynı |
| **Dashboard başına tekil sürüm** | Tek bir Redis Lua betiği: çakışma kontrolü + sürüm artırma + durum yazma + günlüğe ekleme atomik | Test: 2 düğümden 300 eşzamanlı yayıncı → sürümler tam `1..300`, boşluk/tekrar yok |
| **Kayıp ve sıra bozulması onarımı** | İstemci sürümleri izler; boşluk `GapTimeoutMs` kalıcı olursa `Replay(sürüm)` ile günlükten tamamlar, günlük yetmezse anlık görüntüye düşer | Test: her 4. yayın kasıtlı düşürülür → istemci yine aynı duruma gelir, anlık görüntü gerekmez. Son yayın düşürülürse kalp atışı ("head" sürümü) ile bulunur |
| **Çakışma toplama (conflation)** | Pencere içindeki aynı widget güncellemeleri tek mesaja iner; mesaj, kapsadığı **tüm sürümleri** taşır, istemci kayıp sanmaz | Test: tek widget'a 500 güncelleme → istemci <400 delta işler, son durum ve sürüm tam, 0 yeniden eşitleme |
| **Yeniden bağlanma** | Son sürümden devam: yalnızca kaçırılanlar gelir | Test: bağlantı kopar, 39 güncelleme kaçar, yeniden bağlanır → ek anlık görüntü yok; log kısaysa anlık görüntüye düşer |
| **Çok kiracılık** | Grup adı token'daki `tenant` claim'inden türetilir; istemci başka kiracının dashboard'ını adresleyemez | Test: iki kiracı aynı dashboard kimliğiyle birbirinin verisini ve izleyicisini görmez |
| **Yetki** | JWT (WebSocket için `access_token` sorgusu), yazma API'si `ingest` rolü ister, `viewer` rolü hub'dan düzenleyemez | Test: 401/403 ve hub reddi |
| **İşbirlikçi düzenleme** | `expectedWidgetVersion` ile iyimser eşzamanlılık; çakışan, güncel durumu geri alır | Test: aynı widget'a iki editör → tam biri kazanır, kaybeden güncel JSON'u görür ve yeniden dener |
| **Varlık (presence)** | Redis sıralı küme + TTL; kalp atışıyla yenilenir, düğümler arası paylaşılır | Test: iki düğümde görünür; kalp atışı kesilen kullanıcı TTL sonunda düşer |
| **Sınırlar** | Widget ≤16 KB, dashboard başına ≤500 widget, bağlantı başına ≤20 dashboard, kimlik doğrulaması | Test (bağlantı başına dashboard sınırı hariç) |

## Ölçüm

AMD Ryzen 7 9800X3D (8 çekirdek/16 iş parçacığı), Windows, Release derleme. **İki sunucu düğümü + Redis 8 (Docker) + yük üreteci aynı makinede.** Yayın gecikmesi =
sunucunun değişikliği kabul ettiği an (ms damgası) ile istemcinin uygulaması arasındaki süre; tek saat, ms çözünürlüğü. Çakışma toplama penceresi 25 ms (ortalama ~12 ms ekler).

| Bağlı istemci | Güncelleme hızı | Teslimat | Gecikme p50 / p95 / p99 / maks | Sonuç |
|---|---|---|---|---|
| 2.000 | 200/sn, 20 sn | 7,89M (394 bin/sn) | 46 / 78 / 91 / 470 ms | 2000/2000 yakınsadı, 0 yeniden eşitleme |
| 5.000 | 100/sn, 20 sn | 9,97M (499 bin/sn) | 106 / 246 / 356 / 501 ms | 5000/5000 yakınsadı, 0 yeniden eşitleme |
| 10.000 | 100/sn | geçersiz | doyum | 10.000 bağlantı kuruldu ama yük üreteci süreci ~5 GB'a şişti, sunucu `keep-alive` süresi aşıldı, bağlantılar düştü |

**10.000 satırı bir sunucu sonucu değildir.** Tek makinede yük üreteci (10 bin `HubConnection`) + 2 sunucu + Redis aynı CPU ve belleği paylaşıyor; ölçüm düzeneği doydu.
Ayrı makinelerde yük üretimi yapılmadığı için tek bir sunucunun 10 bin eşzamanlı bağlantıyı kaldırdığı **kanıtlanmadı**. Doğru okuma: bu tek makinede 5 bin bağlantıda
saniyede ~500 bin teslimat, p99 < 400 ms, kayıpsız.

Aracı: [bench/SyncSphere.LoadTest](bench/SyncSphere.LoadTest) (`--clients --rate --seconds --servers`).

## Hızlı başlangıç

```bash
docker compose up -d --build        # Redis + 2 düğüm (5101, 5102); Development ortamı, /dev/token vardır
ING=$(curl -s "localhost:5101/dev/token?sub=erp&tenant=acme&role=ingest" | jq -r .token)
curl -X PUT localhost:5101/api/dashboards/sales/widgets/revenue -H "authorization: Bearer $ING" \
     -H 'content-type: application/json' -d '{"json":"{\"today\":128450}"}'
curl localhost:5102/api/dashboards/sales -H "authorization: Bearer $ING"      # öteki düğümden aynı durum
curl localhost:5101/stats
```

İstemci: [SyncSphere.Client](src/SyncSphere.Client) — bağlanma, sürüm takibi, boşluk onarımı, otomatik yeniden bağlanma, anlık görüntü/tekrar oynatma.

```csharp
await using var client = new DashboardClient(new DashboardClientOptions
{
    HubUrl = new Uri("http://localhost:5101/hubs/dashboard"),
    DashboardId = "sales",
    AccessToken = () => Task.FromResult<string?>(token),
});
await client.ConnectAsync();
var widgets = client.Widgets;
```

## Mimari

```
ERP/CRM ──PUT──▶ Düğüm A ─┐          ┌─▶ Düğüm A istemcileri
                           ├─ Lua ─▶ Redis ──backplane──┤
tarayıcı ──hub───▶ Düğüm B ─┘  (sürüm++, durum, günlük)    └─▶ Düğüm B istemcileri
                                  │
              Replay(sürüm) ◀─────┘   (istemci boşluk görürse günlükten çeker)
```

Sunucu kuralları: katılırken önce gruba eklenir, sonra anlık görüntü/replay okunur (arada gelen push ile çakışabilir ama boşluk oluşmaz; istemci tekrarları sürüm ve widget sürümüyle eler).
Kararlar: [docs/adr](docs/adr).

## Testler

```bash
dotnet test --solution SyncSphere.slnx     # Docker gerekir (Testcontainers ile gerçek Redis 8), sunucular gerçek Kestrel
```

14 test; art arda 8 tam koşu geçti.

**Testler ve yük testi gerçek hatalar buldu:**
1. Redis bağlantısı tembel kuruluyordu; ani 200 bağlantıda iş parçacığı havuzu doyunca bağlantı zaman aşımına uğruyor, birkaç istemci takılıyordu. Artık açılışta kurulur, `AbortOnConnectFail=false`.
2. Kalp atışı "head sürümü istemciden büyük" görünce hemen yeniden eşitleme tetikliyordu; canlı akışta bu normal olduğundan tüm istemciler birden onarıma girdi (2.000 istemcide ~1.700 gereksiz yeniden eşitleme).
   Şimdi boşluk, `GapTimeoutMs` boyunca **ilerlemeden** sürerse onarılır; sonuç: aynı yükte 0 yeniden eşitleme.
3. Boşluk yaşı ilk boşluktan sayılıyordu; sürekli akışta hep küçük bir boşluk olduğundan yaş hiç sıfırlanmıyordu. Artık sürekli sürüm ilerlemesi sayacı sıfırlar.
4. 10.000 istemci denemesinde istemci tarafı port tükenmesi: her `HubConnection` negotiate için ayrıca bir TCP bağlantısı tutuyordu (istemci başına 2 soket, Windows'ta ~16 bin geçici port). `WebSocketsOnly` seçeneği negotiate'i atlar.

## Bilinen sınırlar (dürüst liste)

- Ölçümler tek makinede, loopback üzerinde; ayrı makinelerde dağıtık yük üretimi yapılmadı. 10.000 eşzamanlı bağlantı **gösterilmedi**.
- Redis backplane pub/sub "en çok bir kez" teslim eder. Kayıp ve sıra bozulması sürüm/tekrar oynatma ile onarılır (düşürülen yayınlarla test edildi), ama **gerçek bir Redis kesintisi** (container durdurma) senaryosu test edilmedi.
- Yük dengeleyici arkasında yapışkan oturum/düğüm ölümü ile otomatik yeniden bağlanma uçtan uca denenmedi; testlerde istemciler bir düğüme doğrudan bağlanır.
- Her dashboard tek Redis anahtar grubuna (aynı hash etiketi) yazar; çok sıcak bir dashboard tek bir Redis parçasının Lua işleme hızıyla sınırlıdır.
- Tekrar oynatma günlüğü sınırlıdır (varsayılan 10.000 sürüm). Daha uzun süre çevrimdışı kalan istemci anlık görüntüye düşer.
- Çakışma toplama pencereli ve düğüm başınadır: abonelerin her ara değeri görmesi garanti değil, son değer ve sürüm kapsamı garanti. Her olayı isteyen tüketici replay günlüğünü kullanmalı.
- Düğüm çökerse varlık kayıtları TTL (30 sn) dolana kadar kalır; anında yayın yok.
- JSON protokolü; MessagePack bant genişliği ve CPU'yu düşürürdü, denenmedi. Widget içeriği opak JSON, şema doğrulaması yok.
- JWT doğrulaması testlerde simetrik anahtarla; gerçek bir OIDC `Authority` ile denenmedi. `/stats` kimlik doğrulamasız.
- Kullanıcı başına hız sınırı yok (yalnızca boyut ve sayı sınırları var).
