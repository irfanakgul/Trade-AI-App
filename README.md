# Trade-AI-App
Note: github ssh key is used from yasin yilmaz key. you need to change this key maybe ? 
https://chatgpt.com/c/699cbbd6-868c-8330-bad2-a52d88db8f34


super simdi ayni header sidebar ve footeri koruyarak, Orders sayfasini yapmamiz lazim. 

burada limit parametreleri ve acik orderlar olacak. 

limit paremetreleri icin kullanilacak tablo: live.buy_limits tablosundan username filtresi uygilandiktan sonraki ibkr_mode  = LIVE/PAPER tablari icin (cuzdan da oldugu gibi) bazi sutunlarin gosterimini yapacagiz. burada exchange sutunundaki unique verilerin tamami icin, suan satir satir AEB,NASDAQ,NYSE var ama sen bunu uniqe olarak al ve olan her exchange bir blokta alt alta goster. exchange sayisi artabilir zamanla, buna uyumlu olsun. her borsa icin verecegim sutunlarin bilgilerini gostermemiz lazim. 

Gosterilecek sutunlar: total_max_open_positions, max_daily_trade_count,exchange_max_open_positions,current_open_position_count,remaining_open_position_slots,today_buy_count_used,today_buy_count_remaining,allocated_budget_pct,allocated_budget_amount,available_funds, planned_buy_count

her borsa icin bu sutunlar gosterilece ama guzel bir sutun ismi ile ve yine kucuk bi aciklama ekle.