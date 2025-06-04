from app.models import YevmiyeMaddesiBasligi, YevmiyeFisiSatiri, Firma
from sqlalchemy import func, and_, or_
from app import db
from collections import defaultdict
from datetime import date, timedelta
import logging
from decimal import Decimal, ROUND_HALF_UP # Parasal hesaplamalar için Decimal kullanmak daha iyidir

# Uygulama logger'ını kullanmak daha merkezi olabilir, ancak bu şekilde de çalışır.
# from flask import current_app
# logger = current_app.logger
logger = logging.getLogger(__name__)
if not logger.handlers: # Birden fazla handler eklenmesini önle
    handler = logging.StreamHandler() # Veya projenizin ana log handler'ı
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO) # Veya DEBUG

# HESAP PLANI EŞLEŞTİRME VE NİTELİKLERİ
# type: 'A' (Aktif), 'P' (Pasif), 'OZ' (Özkaynak),
#       'GELIR' (Gelir), 'GIDER' (Gider/Maliyet), 'INDIRIM' (Satış İndirimi),
#       'REG_A' (Düzenleyici Aktif -), 'REG_P' (Düzenleyici Pasif/Özkaynak -)
# normal_balance: 'D' (Debit/Borç), 'C' (Credit/Alacak)
# fs_section: 'BILANCO_AKTIF', 'BILANCO_PASIF', 'GELIR_TABLOSU'
# fs_group: Mali tablodaki ana grup adı (BILANCO_YAPISI ve GELIR_TABLOSU_YAPISI'ndaki anahtarlarla eşleşmeli)
# fs_sub_group: Mali tablodaki alt grup adı
HESAP_DETAYLARI = {
    # DÖNEN VARLIKLAR
    '100': {'adi': 'KASA', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'A. HAZIR DEĞERLER'},
    '101': {'adi': 'ALINAN ÇEKLER', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'A. HAZIR DEĞERLER'},
    '102': {'adi': 'BANKALAR', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'A. HAZIR DEĞERLER'},
    '103': {'adi': 'VERİLEN ÇEKLER VE ÖDEME EMİRLERİ (-)', 'type': 'REG_A', 'normal_balance': 'C', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'A. HAZIR DEĞERLER'},
    '120': {'adi': 'ALICILAR', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'C. TİCARİ ALACAKLAR'},
    '121': {'adi': 'ALACAK SENETLERİ', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'C. TİCARİ ALACAKLAR'},
    '153': {'adi': 'TİCARİ MALLAR', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'D. STOKLAR'},
    '190': {'adi': 'DEVREDEN KDV', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'E. DİĞER DÖNEN VARLIKLAR'},
    '191': {'adi': 'İNDİRİLECEK KDV', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'I. DÖNEN VARLIKLAR', 'fs_sub_group': 'E. DİĞER DÖNEN VARLIKLAR'},

    # DURAN VARLIKLAR
    '255': {'adi': 'DEMİRBAŞLAR', 'type': 'A', 'normal_balance': 'D', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'II. DURAN VARLIKLAR', 'fs_sub_group': 'B. MADDİ DURAN VARLIKLAR'},
    '257': {'adi': 'BİRİKMİŞ AMORTİSMANLAR (-)', 'type': 'REG_A', 'normal_balance': 'C', 'fs_section': 'BILANCO_AKTIF', 'fs_group': 'II. DURAN VARLIKLAR', 'fs_sub_group': 'B. MADDİ DURAN VARLIKLAR'},

    # KISA VADELİ YABANCI KAYNAKLAR
    '320': {'adi': 'SATICILAR', 'type': 'P', 'normal_balance': 'C', 'fs_section': 'BILANCO_PASIF', 'fs_group': 'III. KISA VADELİ YABANCI KAYNAKLAR', 'fs_sub_group': 'B. TİCARİ BORÇLAR'},
    '391': {'adi': 'HESAPLANAN KDV', 'type': 'P', 'normal_balance': 'C', 'fs_section': 'BILANCO_PASIF', 'fs_group': 'III. KISA VADELİ YABANCI KAYNAKLAR', 'fs_sub_group': 'E. ÖDENECEK VERGİ VE DİĞER YÜKÜMLÜLÜKLER'},

    # ÖZKAYNAKLAR
    '500': {'adi': 'SERMAYE', 'type': 'OZ', 'normal_balance': 'C', 'fs_section': 'BILANCO_PASIF', 'fs_group': 'V. ÖZKAYNAKLAR', 'fs_sub_group': 'A. ÖDENMİŞ SERMAYE'},
    '590': {'adi': 'DÖNEM NET KARI', 'type': 'OZ', 'normal_balance': 'C', 'fs_section': 'BILANCO_PASIF', 'fs_group': 'V. ÖZKAYNAKLAR', 'fs_sub_group': 'E. DÖNEM NET KÂRI/ZARARI'},
    '591': {'adi': 'DÖNEM NET ZARARI (-)', 'type': 'REG_P', 'normal_balance': 'D', 'fs_section': 'BILANCO_PASIF', 'fs_group': 'V. ÖZKAYNAKLAR', 'fs_sub_group': 'E. DÖNEM NET KÂRI/ZARARI'},


    # GELİR TABLOSU HESAPLARI
    '600': {'adi': 'YURTİÇİ SATIŞLAR', 'type': 'GELIR', 'normal_balance': 'C', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'A. BRÜT SATIŞLAR'},
    '610': {'adi': 'SATIŞTAN İADELER (-)', 'type': 'INDIRIM', 'normal_balance': 'D', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'B. SATIŞ İNDİRİMLERİ (-)'},
    '621': {'adi': 'SATILAN TİCARİ MALLAR MALİYETİ (-)', 'type': 'GIDER', 'normal_balance': 'D', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'C. SATIŞLARIN MALİYETİ (-)'},
    '632': {'adi': 'GENEL YÖNETİM GİDERLERİ (Yansıtılan)', 'type': 'GIDER', 'normal_balance': 'D', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'D. FAALİYET GİDERLERİ (-)'}, # Bu grup adı YAPISI ile eşleşmeli
    '642': {'adi': 'FAİZ GELİRLERİ', 'type': 'GELIR', 'normal_balance': 'C', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'E. DİĞER FAALİYETLERDEN OLAĞAN GELİR VE KÂRLAR'},
    '660': {'adi': 'KISA VADELİ BORÇLANMA GİDERLERİ (-)', 'type': 'GIDER', 'normal_balance': 'D', 'fs_section': 'GELIR_TABLOSU', 'fs_group': 'G. FİNANSMAN GİDERLERİ (-)'},
    '770': {'adi': 'GENEL YÖNETİM GİDERLERİ (Maliyet)', 'type': 'GIDER_MALIYET', 'normal_balance': 'D'}, # Doğrudan GT'ye değil, 632'ye yansır
}

# MALİ TABLO YAPI ŞABLONLARI (Daha Detaylı ve TDHP'ye Yakın)
BILANCO_YAPISI = {
    "AKTIFLER": {
        "I. DÖNEN VARLIKLAR": {
            "A. HAZIR DEĞERLER": ['100', '101', '102', '103', '108'],
            "C. TİCARİ ALACAKLAR": ['120', '121', '122', '128', '129'], # 122, 129 düzenleyici
            "D. STOKLAR": ['153'],
            "E. DİĞER DÖNEN VARLIKLAR": ['190', '191']
        },
        "II. DURAN VARLIKLAR": {
            "B. MADDİ DURAN VARLIKLAR": ['252', '254', '255', '257'], # 257 düzenleyici
        }
        # ... Diğer Duran Varlık Grupları
    },
    "PASIFLER": {
        "III. KISA VADELİ YABANCI KAYNAKLAR": {
            "B. TİCARİ BORÇLAR": ['320'],
            "E. ÖDENECEK VERGİ VE DİĞER YÜKÜMLÜLÜKLER": ['360','391']
        },
        "IV. UZUN VADELİ YABANCI KAYNAKLAR": {
            # ...
        },
        "V. ÖZKAYNAKLAR": {
            "A. ÖDENMİŞ SERMAYE": ['500'],
            "D. GEÇMİŞ YILLAR KÂRLARI/ZARARLARI": ['570', '580'], # 580 düzenleyici
            "E. DÖNEM NET KÂRI/ZARARI": ['590', '591'] # 591 düzenleyici
        }
    }
}

GELIR_TABLOSU_YAPISI = [
    {'kalem_adi': 'A. BRÜT SATIŞLAR', 'hesap_gruplari': ['A_BRUT_SATISLAR']},
    {'kalem_adi': 'B. SATIŞ İNDİRİMLERİ (-)', 'hesap_gruplari': ['B_SATIS_INDIRIMLERI']},
    {'kalem_adi': 'NET SATIŞLAR', 'hesaplama': lambda gt: gt.get('A. BRÜT SATIŞLAR', Decimal(0)) + gt.get('B. SATIŞ İNDİRİMLERİ (-)', Decimal(0))}, # İndirimler zaten negatif fs_impact ile gelecek
    {'kalem_adi': 'C. SATIŞLARIN MALİYETİ (-)', 'hesap_gruplari': ['C_SATISLARIN_MALIYETI']},
    {'kalem_adi': 'BRÜT SATIŞ KÂRI (ZARARI)', 'hesaplama': lambda gt: gt.get('NET SATIŞLAR', Decimal(0)) + gt.get('C. SATIŞLARIN MALİYETİ (-)', Decimal(0))}, # SMM zaten negatif fs_impact ile gelecek
    {'kalem_adi': 'D. FAALİYET GİDERLERİ (-)', 'hesap_gruplari': ['D_FAALIYET_GIDERLERI_GYG']}, # HESAP_DETAYLARI'ndaki grup adı ile eşleşmeli
    {'kalem_adi': 'ESAS FAALİYET KÂRI (ZARARI)', 'hesaplama': lambda gt: gt.get('BRÜT SATIŞ KÂRI (ZARARI)', Decimal(0)) + gt.get('D. FAALİYET GİDERLERİ (-)', Decimal(0))},
    {'kalem_adi': 'E. DİĞER FAALİYETLERDEN OLAĞAN GELİR VE KÂRLAR', 'hesap_gruplari': ['E_DIGER_FAALIYET_GELIR_KAR']},
    # {'kalem_adi': 'F. DİĞER FAALİYETLERDEN OLAĞAN GİDER VE ZARARLAR (-)', 'hesap_gruplari': ['F_DIGER_FAALIYET_GIDER_ZARARLAR']}, # Bu grup HESAP_DETAYLARI'nda tanımlanmalı
    {'kalem_adi': 'OLAĞAN KÂR (ZARAR)', 'hesaplama': lambda gt: gt.get('ESAS FAALİYET KÂRI (ZARARI)', Decimal(0)) + gt.get('E. DİĞER FAALİYETLERDEN OLAĞAN GELİR VE KÂRLAR', Decimal(0)) }, # + gt.get('F. DİĞER FAALİYETLERDEN OLAĞAN GİDER VE ZARARLAR (-)', Decimal(0))
    {'kalem_adi': 'G. FİNANSMAN GİDERLERİ (-)', 'hesap_gruplari': ['G_FINANSMAN_GIDERLERI']},
    {'kalem_adi': 'SÜRDÜRÜLEN FAALİYETLER VERGİ ÖNCESİ KÂRI (ZARARI)', 'hesaplama': lambda gt: gt.get('OLAĞAN KÂR (ZARAR)', Decimal(0)) + gt.get('G. FİNANSMAN GİDERLERİ (-)', Decimal(0))},
    # TODO: Dönem Karı Vergi Karşılıkları ve Dönem Net Karı/Zararı
]


def get_donem_sonu_bakiyeleri(firma_id: int, donem_bitis_date: date):
    """
    Belirtilen firma ve dönem sonu tarihi itibarıyla tüm hesapların
    kümülatif dönem sonu bakiyelerini hesaplar.
    Bu, Bilanço için gereklidir.
    """
    try:
        results = db.session.query(
            YevmiyeFisiSatiri.hesap_kodu,
            func.sum(YevmiyeFisiSatiri.borc_tutari).label('kümülatif_borc'),
            func.sum(YevmiyeFisiSatiri.alacak_tutari).label('kümülatif_alacak')
        ).join(YevmiyeMaddesiBasligi, YevmiyeFisiSatiri.yevmiye_maddesi_id == YevmiyeMaddesiBasligi.id)\
        .filter(
            YevmiyeMaddesiBasligi.firma_id == firma_id,
            YevmiyeFisiSatiri.muhasebe_kayit_tarihi <= donem_bitis_date # O tarihe kadar olan TÜM hareketler
        ).group_by(
            YevmiyeFisiSatiri.hesap_kodu
        ).all()

        hesap_bakiyeleri = {}
        for row in results:
            hesap_kodu = row.hesap_kodu
            kümülatif_borc = Decimal(row.kümülatif_borc or 0.0).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            kümülatif_alacak = Decimal(row.kümülatif_alacak or 0.0).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            hesap_detayi = HESAP_DETAYLARI.get(hesap_kodu) or HESAP_DETAYLARI.get(hesap_kodu[:3])
            if not hesap_detayi:
                logger.warning(f"Firma {firma_id}, Dönem Sonu {donem_bitis_date}: Bakiye hesaplamada bilinmeyen hesap kodu {hesap_kodu}.")
                continue

            net_bakiye = Decimal(0)
            if hesap_detayi['normal_balance'] == 'D':
                net_bakiye = kümülatif_borc - kümülatif_alacak
            elif hesap_detayi['normal_balance'] == 'C':
                net_bakiye = kümülatif_alacak - kümülatif_borc
                
            hesap_bakiyeleri[hesap_kodu] = {
                'adi': hesap_detayi['adi'],
                'kümülatif_borc': kümülatif_borc,
                'kümülatif_alacak': kümülatif_alacak,
                'dönem_sonu_bakiye': net_bakiye,
                'normal_balance': hesap_detayi['normal_balance'],
                'fs_impact_bilanco': hesap_detayi.get('fs_impact_bilanco', 0), # Varsayılan 0, sadece map'te olanlar etki etsin
                'bilanco_grup': hesap_detayi.get('bilanco_grup')
            }
        logger.info(f"Firma {firma_id}, Dönem Sonu {donem_bitis_date} için {len(hesap_bakiyeleri)} hesabın kümülatif bakiyesi hesaplandı.")
        return hesap_bakiyeleri
    except Exception as e:
        logger.error(f"get_donem_sonu_bakiyeleri hata: {e}", exc_info=True)
        raise


def get_donem_ici_hareketler(firma_id: int, donem_baslangic_date: date, donem_bitis_date: date):
    """
    Belirtilen firma ve dönem aralığı için hesapların net dönem içi hareketlerini hesaplar.
    Bu, Gelir Tablosu için gereklidir.
    """
    try:
        # Bu fonksiyon bir önceki yanıttaki get_hesap_bakiyeleri_for_period ile aynı mantıkta
        # Sadece ismi daha açıklayıcı oldu.
        results = db.session.query(
            YevmiyeFisiSatiri.hesap_kodu,
            func.sum(YevmiyeFisiSatiri.borc_tutari).label('donem_borc_hareket'),
            func.sum(YevmiyeFisiSatiri.alacak_tutari).label('donem_alacak_hareket')
        ).join(YevmiyeMaddesiBasligi, YevmiyeFisiSatiri.yevmiye_maddesi_id == YevmiyeMaddesiBasligi.id)\
        .filter(
            YevmiyeMaddesiBasligi.firma_id == firma_id,
            YevmiyeFisiSatiri.muhasebe_kayit_tarihi >= donem_baslangic_date,
            YevmiyeFisiSatiri.muhasebe_kayit_tarihi <= donem_bitis_date
        ).group_by(
            YevmiyeFisiSatiri.hesap_kodu
        ).all()

        hesap_hareketleri = {}
        for row in results:
            hesap_kodu = row.hesap_kodu
            donem_borc = Decimal(row.donem_borc_hareket or 0.0).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            donem_alacak = Decimal(row.donem_alacak_hareket or 0.0).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
            
            hesap_detayi = HESAP_DETAYLARI.get(hesap_kodu) or HESAP_DETAYLARI.get(hesap_kodu[:3])
            if not hesap_detayi:
                logger.warning(f"Firma {firma_id}, Dönem {donem_baslangic_date}-{donem_bitis_date}: Hareket hesaplamada bilinmeyen hesap kodu {hesap_kodu}.")
                continue
            
            # Gelir tablosu hesapları için dönem içi net hareket
            net_hareket = Decimal(0)
            if hesap_detayi['normal_balance'] == 'D': # Gider/Maliyet/İndirim hesapları
                net_hareket = donem_borc - donem_alacak
            elif hesap_detayi['normal_balance'] == 'C': # Gelir hesapları
                net_hareket = donem_alacak - donem_borc
                
            hesap_hareketleri[hesap_kodu] = {
                'adi': hesap_detayi['adi'],
                'donem_borc_hareket': donem_borc,
                'donem_alacak_hareket': donem_alacak,
                'net_donem_hareketi': net_hareket,
                'normal_balance': hesap_detayi['normal_balance'],
                'fs_impact_gelir_tablosu': hesap_detayi.get('fs_impact_gelir_tablosu', 0),
                'gelir_tablosu_grup': hesap_detayi.get('gelir_tablosu_grup')
            }
        logger.info(f"Firma {firma_id}, Dönem {donem_baslangic_date}-{donem_bitis_date} için {len(hesap_hareketleri)} hesabın dönem içi hareketi hesaplandı.")
        return hesap_hareketleri
    except Exception as e:
        logger.error(f"get_donem_ici_hareketler hata: {e}", exc_info=True)
        raise

def _generate_fs_recursive(hesap_verileri, yapi_seviyesi, anahtar_bakiye_alanı, anahtar_impact_alanı, anahtar_grup_alanı):
    """ Mali tablo kalemlerini ve alt toplamlarını rekürsif olarak hesaplar. """
    kalem_sonuclari = {}
    seviye_toplami = Decimal(0)

    for grup_adi, alt_yapi in yapi_seviyesi.items(): # Örn: "I. DÖNEN VARLIKLAR", {"A. HAZIR DEĞERLER": [...]}
        if isinstance(alt_yapi, dict): # Bu bir ana grup, içini işle (alt gruplar)
            alt_grup_sonuclari, alt_grup_toplami = _generate_fs_recursive(hesap_verileri, alt_yapi, anahtar_bakiye_alanı, anahtar_impact_alanı, anahtar_grup_alanı)
            kalem_sonuclari[grup_adi] = alt_grup_sonuclari
            if alt_grup_toplami is not None : # Eğer alt grup toplamı hesaplanabildiyse
                 kalem_sonuclari[grup_adi]['GRUP_TOPLAMI'] = alt_grup_toplami
                 seviye_toplami += alt_grup_toplami
        elif isinstance(alt_yapi, list): # Bu bir alt grup, hesap kodlarını içerir
            alt_grup_toplami_degeri = Decimal(0)
            alt_grup_detaylari = {}
            for kod in alt_yapi: # ['100', '101', ...]
                if kod in hesap_verileri:
                    hesap_data = hesap_verileri[kod]
                    bakiye = hesap_data[anahtar_bakiye_alanı] # 'dönem_sonu_bakiye' veya 'net_donem_hareketi'
                    impact = Decimal(hesap_data.get(anahtar_impact_alanı, 1)) # HESAP_DETAYLARI'ndan gelen impact
                    
                    # Düzenleyici hesaplar için impact zaten negatif olmalı.
                    # Bakiye * impact, kalemin toplama olan net etkisini verir.
                    etkilenmis_bakiye = bakiye * impact
                    alt_grup_detaylari[f"{kod} {hesap_data.get('adi','')}"] = etkilenmis_bakiye
                    alt_grup_toplami_degeri += etkilenmis_bakiye # Burada impact zaten uygulandı
            kalem_sonuclari[grup_adi] = {
                "detay": alt_grup_detaylari,
                "ALT_GRUP_TOPLAMI": alt_grup_toplami_degeri
            }
            seviye_toplami += alt_grup_toplami_degeri
            
    return kalem_sonuclari, seviye_toplami


def generate_bilanco_v3(donem_sonu_bakiyeleri):
    bilanco = {"AKTIFLER": {}, "PASIFLER": {}}
    logger.info("Bilanço v3 oluşturuluyor...")

    try:
        aktif_detay, aktif_toplami = _generate_fs_recursive(
            donem_sonu_bakiyeleri, BILANCO_YAPISI["AKTIFLER"],
            anahtar_bakiye_alanı='dönem_sonu_bakiye',
            anahtar_impact_alanı='fs_impact_bilanco', # HESAP_DETAYLARI'ndan bu impact'i alacağız
            anahtar_grup_alanı='bilanco_grup'
        )
        bilanco["AKTIFLER"] = aktif_detay
        bilanco["AKTIFLER"]["GENEL_TOPLAM"] = aktif_toplami

        pasif_detay, pasif_toplami = _generate_fs_recursive(
            donem_sonu_bakiyeleri, BILANCO_YAPISI["PASIFLER"],
            anahtar_bakiye_alanı='dönem_sonu_bakiye',
            anahtar_impact_alanı='fs_impact_bilanco',
            anahtar_grup_alanı='bilanco_grup'
        )
        bilanco["PASIFLER"] = pasif_detay
        bilanco["PASIFLER"]["GENEL_TOPLAM"] = pasif_toplami
        
        logger.info(f"Bilanço v3 oluşturuldu. Aktif Toplam: {aktif_toplami}, Pasif Toplam: {pasif_toplami}")
        if abs(aktif_toplami - pasif_toplami) > Decimal('0.01'): # Tolerans
            denklik_farki = aktif_toplami - pasif_toplami
            logger.warning(f"BİLANÇO DENKLİĞİ SAĞLANAMADI! Fark: {denklik_farki:.2f} (Aktif: {aktif_toplami}, Pasif: {pasif_toplami})")
            bilanco["DENKLIK_SORUNU"] = f"Fark: {denklik_farki:.2f} (Aktif: {aktif_toplami}, Pasif: {pasif_toplami})"
    except Exception as e:
        logger.error(f"generate_bilanco_v3 hata: {e}", exc_info=True)
        bilanco["HATA"] = str(e)


    def convert_decimals_to_str_recursive(node):
        if isinstance(node, dict):
            return {k: convert_decimals_to_str_recursive(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [convert_decimals_to_str_recursive(item) for item in node]
        elif isinstance(node, Decimal):
            return f"{node:.2f}" # İki ondalık basamakla string'e çevir
        return node
        
    return convert_decimals_to_str_recursive(bilanco)


def generate_gelir_tablosu_v3(donem_ici_hareketler):
    gelir_tablosu_sonuclari = {}
    logger.info("Gelir Tablosu v3 oluşturuluyor...")
    
    try:
        for item in GELIR_TABLOSU_YAPISI:
            kalem_adi = item['kalem_adi']
            if 'hesaplama' in item and callable(item['hesaplama']):
                # Ara toplamlar veya özel hesaplamalar
                gelir_tablosu_sonuclari[kalem_adi] = item['hesaplama'](gelir_tablosu_sonuclari)
            elif 'hesap_gruplari' in item:
                kalem_toplami = Decimal(0)
                detaylar = {}
                for grup_anahtari in item['hesap_gruplari']: # örn: 'A_BRUT_SATISLAR'
                    for kod, hesap_data in donem_ici_hareketler.items():
                        if hesap_data.get('gelir_tablosu_grup') == grup_anahtari:
                            hareket = hesap_data['net_donem_hareketi']
                            # fs_impact_gelir_tablosu HESAP_DETAYLARI'ndan gelir ve zaten +/- içerir.
                            # Gelirler pozitif, giderler/indirimler negatif impact'e sahip olmalı.
                            etkilenmis_hareket = hareket * Decimal(hesap_data.get('fs_impact_gelir_tablosu', 1))
                            detaylar[f"{kod} {hesap_data.get('adi','')}"] = etkilenmis_hareket
                            kalem_toplami += etkilenmis_hareket
                gelir_tablosu_sonuclari[kalem_adi] = {
                    "TUTAR": kalem_toplami,
                    "DETAY": detaylar
                } if detaylar else kalem_toplami # Eğer detay yoksa sadece tutarı ver
    except Exception as e:
        logger.error(f"generate_gelir_tablosu_v3 hata: {e}", exc_info=True)
        gelir_tablosu_sonuclari["HATA"] = str(e)

    logger.info(f"Gelir Tablosu v3 oluşturuldu: Net Satışlar: {gelir_tablosu_sonuclari.get('NET SATIŞLAR')}")

    def convert_decimals_to_str_recursive(node):
        if isinstance(node, dict):
            return {k: convert_decimals_to_str_recursive(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [convert_decimals_to_str_recursive(item) for item in node]
        elif isinstance(node, Decimal):
            return f"{node:.2f}"
        return node
        
    return convert_decimals_to_str_recursive(gelir_tablosu_sonuclari)