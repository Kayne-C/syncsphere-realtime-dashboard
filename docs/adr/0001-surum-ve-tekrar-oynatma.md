# ADR 0001 — Sürüm numarası + tekrar oynatma günlüğü: güvenilmez taşıyıcı üstünde güvenilir akış

**Durum:** Kabul edildi

## Bağlam

SignalR grup yayını ve Redis backplane (pub/sub) teslim garantisi vermez: bir düğüm yeniden başlarken, Redis bağlantısı kopunca veya istemci yeniden bağlanırken mesajlar kaybolur.
Dashboard'da kaybolan tek bir güncelleme, bir sonraki güncellemeye kadar yanlış bir sayı göstermek demektir (ya da hiç düzelmemek).

## Karar

- Her dashboard'un tek, artan bir **sürümü** var. Lua betiği sürümü atomik artırır, widget'ı yazar ve değişikliği bir Redis Stream'ine (`ID = sürüm-0`) ekler. Akış `MAXLEN` ile sınırlı.
- Sunucu yalnızca "anlık görüntü" ve "sürüm > x olan değişiklikler" sorgularını da aynı yapıdan cevaplar (`Join`, `Replay`, `GetSnapshot`).
- İstemci kesintisiz sürüm sınırını (`contiguous`) tutar, önde gelen sürümleri bir kümede biriktirir. İlerleme `GapTimeoutMs` boyunca durursa `Replay(contiguous)` çağırır; günlük geriye yetmiyorsa anlık görüntüye düşer.
- Uygulama kuralı **widget sürümü**ne göre "en yeni kazanır"dır; tekrar, sıra bozulması ve çakışma toplama durumun yakınsamasını bozamaz.
- Kuyruk sonu kaybı (son yayın düşer, sonrası gelmez) için kalp atışı sunucunun güncel sürümünü döndürür; istemci geride ise onarır.

## Sonuçlar

- "Kesinlikle bir kez" yok; "yakınsar ve kayıp fark edilir" var. Bedel: bir Redis anahtar grubu ve sınırlı bir günlük.
- Test: yayınları kasıtlı düşüren bir hata enjeksiyonu (her 4. yayın, ve yalnız sonuncusu) ile iki durumda da istemci sunucuyla aynı duruma gelir.
