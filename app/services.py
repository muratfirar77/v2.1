# app/services.py

def calculate_cari_oran(donen_varliklar, kisa_vadeli_yukumlulukler):
    if kisa_vadeli_yukumlulukler is None or kisa_vadeli_yukumlulukler == 0:
        return None
    if donen_varliklar is None:
        return None
    try:
        return round(float(donen_varliklar) / float(kisa_vadeli_yukumlulukler), 4)
    except (TypeError, ValueError):
        return None

def calculate_borc_ozkaynak_orani(toplam_yukumlulukler, oz_kaynaklar):
    if oz_kaynaklar is None or oz_kaynaklar == 0:
        return None
    if toplam_yukumlulukler is None:
        return None
    try:
        return round(float(toplam_yukumlulukler) / float(oz_kaynaklar), 4)
    except (TypeError, ValueError):
        return None

def calculate_altman_z_score_updated(
    donen_varliklar, aktif_toplami, kisa_vadeli_yukumlulukler,
    dagitilmamis_karlar, vergi_oncesi_kar_zarar, oz_kaynaklar,
    toplam_yukumlulukler, net_satislar
    ):
    try:
        donen_varliklar = float(donen_varliklar)
        aktif_toplami = float(aktif_toplami)
        kisa_vadeli_yukumlulukler = float(kisa_vadeli_yukumlulukler)
        dagitilmamis_karlar = float(dagitilmamis_karlar)
        vergi_oncesi_kar_zarar = float(vergi_oncesi_kar_zarar)
        oz_kaynaklar = float(oz_kaynaklar)
        toplam_yukumlulukler = float(toplam_yukumlulukler)
        net_satislar = float(net_satislar)
    except (TypeError, ValueError, AttributeError):
        return None

    if aktif_toplami == 0 or toplam_yukumlulukler == 0 or oz_kaynaklar == 0:
        return None

    net_isletme_sermayesi = donen_varliklar - kisa_vadeli_yukumlulukler
    x1 = net_isletme_sermayesi / aktif_toplami
    x2 = dagitilmamis_karlar / aktif_toplami
    x3 = vergi_oncesi_kar_zarar / aktif_toplami 
    x4 = oz_kaynaklar / toplam_yukumlulukler 
    x5 = net_satislar / aktif_toplami
    
    z_score = (0.717 * x1) + (0.847 * x2) + (3.107 * x3) + (0.420 * x4) + (0.998 * x5)
    return round(z_score, 4)
    
    
    
# app/services.py veya yeni bir app/financial_statement_service.py dosyasına eklenebilir

from app.models import YevmiyeFisiSatiri, Firma
from sqlalchemy import func, case
from app import db
from collections import defaultdict

# BASİTLEŞTİRİLMİŞ HESAP PLANI EŞLEŞTİRME ÖRNEĞİ
# Bu yapının çok daha kapsamlı ve firmanın hesap planına uygun olması gerekir.
HESAP_PLANI_MAP = {
    'BILANCO': {
        'DONEN_VARLIKLAR': {
            'HAZIR_DEGERLER': ['100', '101', '102', '103', '108'], # Kasa, Al.Çek, Bankalar, Ver.Çek.Öd.Em, Diğ.Haz.Değ.
            'MENKUL_KIYMETLER': ['110', '111', '112', '118', '119'],
            'TICARI_ALACAKLAR': ['120', '121', '122', '126', '127', '128', '129'],
            'STOKLAR': ['150', '151', '152', '153', '157', '159'],
            # ... diğer dönen varlık grupları
        },
        'DURAN_VARLIKLAR': {
            'MADDI_DURAN_VARLIKLAR': ['250', '251', '252', '253', '254', '255', '256', '257', '258', '259'],
            # ... diğer duran varlık grupları
        },
        'KISA_VADELI_YABANCI_KAYNAKLAR': {
            'MALI_BORCLAR_KV': ['300', '303', '304', '305', '306', '308', '309'],
            'TICARI_BORCLAR_KV': ['320', '321', '322', '326', '329'],
            'ODENECEK_VERGI_VE_DIGER_YUKUMLULUKLER_KV': ['360', '361', '368', '369'],
            # ... diğer KVYK grupları
        },
        # ... UZUN VADELI YABANCI KAYNAKLAR ...
        # ... ÖZKAYNAKLAR ... (500 Sermaye vb.)
    },
    'GELIR_TABLOSU': {
        'BRUT_SATISLAR': ['600', '601', '602'],
        'SATIS_INDIRIMLERI': ['610', '611', '612'], # (-) Karakterli, Net Satışlar için çıkarılacak
        'SATISLARIN_MALIYETI': ['620', '621', '622', '623'], # (-) Karakterli
        'FAALIYET_GIDERLERI': { # (-) Karakterli
            'ARASTIRMA_GELISTIRME_GIDERLERI': ['630'],
            'PAZARLAMA_SATIS_DAGITIM_GIDERLERI': ['631'],
            'GENEL_YONETIM_GIDERLERI': ['632', '750', '760', '770'] # 7'li maliyet hesapları da buraya yansıtılabilir
        },
        # ... DİĞER FAALİYET GELİRLERİ/GİDERLERİ ...
        # ... FİNANSMAN GELİRLERİ/GİDERLERİ ... (642 Faiz Gelirleri, 660 Kısa Vad. Borç. Gid.)
        # ... OLAĞANDIŞI GELİR/GİDERLER ...
        # ... DÖNEM KARI VERGİ VE DİĞER YASAL YÜKÜMLÜLÜK KARŞILIKLARI ...
        # ... DÖNEM NET KARI/ZARARI ...
    }
}

# Hesap karakterleri (B: Borç kalanı verir, A: Alacak kalanı verir)
HESAP_KARAKTERI = {
    '1': 'B', '2': 'B', # Aktifler
    '3': 'A', '4': 'A', '5': 'A', # Pasifler
    '6': 'A', # Gelirler
    '7': 'B', # Giderler (Maliyet hesapları)
    # 610, 611, 612 gibi indirim hesapları borç çalışır ama gelir tablosunda negatif etki yapar.
    # 620, 621, 630, 631, 632 gibi gider/maliyet hesapları borç çalışır, gelir tablosunda negatif etki yapar.
}


def get_hesap_bakiyeleri(firma_id, dosya_donemi_bitis_date):
    """
    Belirli bir firma ve dönem sonu için hesapların borç ve alacak toplamlarını
    ve karakterlerine göre net bakiyelerini döndürür.
    """
    # İlgili döneme ait tüm yevmiye satırlarını çek
    # dosya_donemi_bitis_date'e göre YevmiyeMaddesiBasligi'ndan ilgili maddeleri,
    # sonra onlara bağlı YevmiyeFisiSatiri'ndan hareketleri almalıyız.
    # Bu sorgu daha karmaşık olabilir ve tüm yevmiye maddelerinin başlık dönemini kapsamalı.
    # Şimdilik basitleştirilmiş bir yaklaşım: O tarihe kadar olan tüm hareketler.
    # Daha doğrusu, sadece o döneme ait e-defter dosyasındaki hareketler alınmalı.
    
    # YevmiyeMaddesiBasligi üzerinden ilgili döneme ait maddeleri filtrele
    ilgili_maddeler_ids = db.session.query(YevmiyeMaddesiBasligi.id)\
        .filter(YevmiyeMaddesiBasligi.firma_id == firma_id,
                YevmiyeMaddesiBasligi.dosya_donemi_bitis == dosya_donemi_bitis_date)\
        .subquery()

    # Bu maddelere ait satırları toplulaştır
    results = db.session.query(
        YevmiyeFisiSatiri.hesap_kodu,
        func.sum(YevmiyeFisiSatiri.borc_tutari).label('toplam_borc'),
        func.sum(YevmiyeFisiSatiri.alacak_tutari).label('toplam_alacak')
    ).filter(
        YevmiyeFisiSatiri.yevmiye_maddesi_id.in_(ilgili_maddeler_ids)
    ).group_by(
        YevmiyeFisiSatiri.hesap_kodu
    ).all()

    hesap_ozetleri = {}
    for row in results:
        hesap_kodu = row.hesap_kodu
        toplam_borc = float(row.toplam_borc or 0.0)
        toplam_alacak = float(row.toplam_alacak or 0.0)
        
        # Hesap karakterine göre net bakiye (çok basitleştirilmiş)
        # Örneğin, 1xx, 2xx, 7xx borç karakterli; 3xx, 4xx, 5xx, 6xx alacak karakterli varsayalım
        # Bu, Tek Düzen Hesap Planı'na göre daha detaylı olmalı.
        net_bakiye = 0
        karakter = HESAP_KARAKTERI.get(hesap_kodu[0] if hesap_kodu else '', 'B') # İlk haneye göre karakter varsayımı

        if karakter == 'B': # Aktif veya Gider
            net_bakiye = toplam_borc - toplam_alacak
        elif karakter == 'A': # Pasif veya Gelir
            net_bakiye = toplam_alacak - toplam_borc
            
        hesap_ozetleri[hesap_kodu] = {
            'borc': toplam_borc,
            'alacak': toplam_alacak,
            'bakiye': net_bakiye,
            'karakter': karakter 
        }
    return hesap_ozetleri


def generate_mali_tablo_kalemi(hesap_bakiyeleri, hesap_kod_listesi, kalem_negatif_mi=False):
    """
    Verilen hesap kod listesindeki hesapların bakiyelerini toplayarak
    bir mali tablo kalemi oluşturur.
    """
    toplam_bakiye = 0
    for kod_grubu in hesap_kod_listesi:
        # Bu kısım daha detaylı olabilir, örn: ['100-108'], '12X' gibi aralıklar veya desenler
        # Şimdilik tam eşleşme varsayıyoruz.
        if isinstance(kod_grubu, str): # Tek bir hesap koduysa
            if kod_grubu in hesap_bakiyeleri:
                toplam_bakiye += hesap_bakiyeleri[kod_grubu]['bakiye']
        elif isinstance(kod_grubu, list): # Bir liste ise (bu örnekte direkt liste kullandık map'te)
             for kod in kod_grubu: # Bu map'teki gibi olmalı: HESAP_PLANI_MAP['BILANCO']['DONEN_VARLIKLAR']['HAZIR_DEGERLER']
                if kod in hesap_bakiyeleri:
                    toplam_bakiye += hesap_bakiyeleri[kod_grubu]['bakiye']


    return -toplam_bakiye if kalem_negatif_mi else toplam_bakiye


def generate_bilanco_from_hesap_bakiyeleri(hesap_bakiyeleri):
    bilanco = {}
    # Bu fonksiyon HESAP_PLANI_MAP'i kullanarak hesap_bakiyeleri'nden bilançoyu oluşturur.
    # Örnek:
    # bilanco['DONEN_VARLIKLAR_TOPLAMI'] = 0
    # for grup_adi, hesap_kodlari in HESAP_PLANI_MAP['BILANCO']['DONEN_VARLIKLAR'].items():
    #     grup_toplami = 0
    #     for kod in hesap_kodlari:
    #         if kod in hesap_bakiyeleri:
    #             grup_toplami += hesap_bakiyeleri[kod]['bakiye'] # Karakterine göre bakiye kullanılmalı
    #     bilanco[grup_adi] = grup_toplami
    #     bilanco['DONEN_VARLIKLAR_TOPLAMI'] += grup_toplami
    # ... Bu kısım çok detaylı bir şekilde yazılmalı ...
    
    # Çok kaba bir örnek:
    bilanco['DONEN_VARLIKLAR'] = sum(
        v['bakiye'] for k, v in hesap_bakiyeleri.items() if k.startswith('1') and v['karakter'] == 'B'
    )
    bilanco['DURAN_VARLIKLAR'] = sum(
        v['bakiye'] for k, v in hesap_bakiyeleri.items() if k.startswith('2') and v['karakter'] == 'B'
    )
    bilanco['AKTIF_TOPLAMI'] = bilanco['DONEN_VARLIKLAR'] + bilanco['DURAN_VARLIKLAR']

    bilanco['KVYK'] = sum(
        v['bakiye'] for k, v in hesap_bakiyeleri.items() if k.startswith('3') and v['karakter'] == 'A'
    )
    bilanco['UVYK'] = sum(
        v['bakiye'] for k, v in hesap_bakiyeleri.items() if k.startswith('4') and v['karakter'] == 'A'
    )
    bilanco['OZKAYNAKLAR'] = sum(
        v['bakiye'] for k, v in hesap_bakiyeleri.items() if k.startswith('5') and v['karakter'] == 'A'
    )
    bilanco['PASIF_TOPLAMI'] = bilanco['KVYK'] + bilanco['UVYK'] + bilanco['OZKAYNAKLAR']

    # Bilanço denkliği kontrolü (Aktifler = Pasifler) yapılabilir.
    return bilanco


def generate_gelir_tablosu_from_hesap_bakiyeleri(hesap_bakiyeleri):
    gelir_tablosu = {}
    # Bu fonksiyon HESAP_PLANI_MAP'i kullanarak gelir tablosunu oluşturur.
    # Örnek:
    # NET_SATISLAR: 600+601+602 - (610+611+612)
    # SATISLARIN_MALIYETI: 620+621+622+623 (-)
    # ... Bu kısım çok detaylı bir şekilde yazılmalı ...

    # Çok kaba bir örnek:
    brut_satislar = sum(v['bakiye'] for k,v in hesap_bakiyeleri.items() if k in ['600','601','602'] and v['karakter']=='A')
    satis_indirimleri = sum(v['bakiye'] for k,v in hesap_bakiyeleri.items() if k in ['610','611','612'] and v['karakter']=='B') # Borç bakiyesi verir ama etkileri negatif
    gelir_tablosu['NET_SATISLAR'] = brut_satislar - satis_indirimleri
    
    smm = sum(v['bakiye'] for k,v in hesap_bakiyeleri.items() if k in ['620','621','622'] and v['karakter']=='B')
    gelir_tablosu['SATISLARIN_MALIYETI'] = smm # Bu zaten gider olduğu için pozitif tutulur, tabloda eksi gösterilir
    
    gelir_tablosu['BRUT_KAR_ZARAR'] = gelir_tablosu['NET_SATISLAR'] - gelir_tablosu['SATISLARIN_MALIYETI']
    
    # ... Diğer gelir tablosu kalemleri ...
    return gelir_tablosu    