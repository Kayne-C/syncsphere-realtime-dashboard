# ADR 0003 — Kiracı izolasyonu token'dan türetilir

**Durum:** Kabul edildi

- Bir istemcinin hangi kiracıya ait olduğu **yalnızca doğrulanmış JWT'deki `tenant` claim'inden** gelir; hub yöntemlerinde kiracı parametresi yoktur. Grup adı (`{tenant}/{dashboard}`), Redis anahtarları (`ss:{tenant:dashboard}:…`) ve varlık kayıtları aynı değerden kurulur.
  İstemci, başka bir kiracının dashboard'ını adreslemenin bir yoluna sahip değildir (yetki kontrolü unutulsa bile).
- `tenant` claim'i olmayan token hiçbir uç noktaya erişemez (fallback politika).
- Dashboard ve widget kimlikleri `^[A-Za-z0-9_-]{1,64}$` ile doğrulanır; Redis anahtarına ve grup adına `/`, `:`, `{` gibi karakterler giremez.
- Yazma: REST ucu `ingest` rolü ister; hub üzerinden düzenleme `viewer` rolüne kapalıdır. Düzenleme için önce `Join` gerekir.
- Test: aynı dashboard kimliği, iki kiracı: veri, sürüm ve izleyiciler tamamen ayrık.
- Sınır: kiracı başına kota veya hız sınırı yok.
