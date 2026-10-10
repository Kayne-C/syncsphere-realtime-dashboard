# ADR 0002 — Çakışma toplama (conflation) ve sürüm hesabı

**Durum:** Kabul edildi

- Yüksek frekanslı KPI'larda her değişikliği her istemciye göndermek ağı ve istemci CPU'sunu boşa harcar. Düğüm, dashboard başına kısa bir pencere (varsayılan 25 ms) biriktirir ve **widget başına yalnızca en yenisini** gönderir.
- Toplama, sürüm boşluğu algılamayı bozmamalı. Bu yüzden her mesaj, kapsadığı **tüm sürümlerin listesini** (`Versions`) taşır; istemci, deltaları atlansa bile bu sürümleri "hesaplandı" sayar. Boşluk yalnızca hiçbir mesajın kapsamadığı sürümde oluşur.
- Pencere düğüm başınadır; iki düğüm aynı dashboard'a yazarsa sürümler iki mesaja bölünür. İstemci bunları sürüm kümesiyle birleştirir; sıra karışıklığı widget sürümü kuralıyla çözülür.
- Gecikme bedeli: ortalama pencerenin yarısı. `ConflationWindowMs=0` her değişikliği anında gönderir.
- Test: 500 ardışık güncelleme → istemci <400 delta uygular, son değer ve son sürüm tam, yeniden eşitleme sayısı 0.
