from flask import Blueprint, request, jsonify, current_app
from app import db
from app.models import User, Firma, FinansalVeri
from app.services import calculate_cari_oran, calculate_borc_ozkaynak_orani, calculate_altman_z_score_updated
from app.financial_statement_service import get_donem_sonu_bakiyeleri, get_donem_ici_hareketler, generate_bilanco_v3, generate_gelir_tablosu_v3

from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
import pandas as pd
import io

bp = Blueprint('main', __name__)

@bp.route('/')
def index():
    return "Finansal Risk Analizi Prototip API'sine Hoş Geldiniz!"

# === KULLANICI İŞLEMLERİ ===
@bp.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({"msg": "Kullanıcı adı ve şifre gerekli"}), 400
    if User.query.filter_by(username=data['username']).first():
        return jsonify({"msg": "Bu kullanıcı adı zaten mevcut"}), 400
    
    user = User(username=data['username'])
    user.set_password(data['password'])
    db.session.add(user)
    db.session.commit()
    current_app.logger.info(f"Yeni kullanıcı kaydedildi: {user.username}")
    return jsonify({"msg": "Kullanıcı başarıyla oluşturuldu", "user_id": user.id}), 201

@bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({"msg": "Kullanıcı adı ve şifre gerekli"}), 400
    
    user = User.query.filter_by(username=data['username']).first()
    if user is None or not user.check_password(data['password']):
        current_app.logger.warning(f"Başarısız giriş denemesi: Kullanıcı '{data.get('username')}'")
        return jsonify({"msg": "Geçersiz kullanıcı adı veya şifre"}), 401
    
    access_token = create_access_token(identity=str(user.id)) # user.id string'e çevrildi
    current_app.logger.info(f"Kullanıcı giriş yaptı: {user.username} (ID: {user.id})")
    return jsonify(access_token=access_token), 200

@bp.route('/me', methods=['GET'])
@jwt_required()
def get_me():
    current_user_id_str = get_jwt_identity() 
    # current_user_id = int(current_user_id_str) # get_jwt_identity string döndürür, DB'de int ise çevir.
                                                # SQLAlchemy'nin get() metodu genellikle string PK'ları da handle eder.
    user = User.query.get(current_user_id_str) # String ID ile de çalışabilir, test edin.
                                            # Eğer int gerekiyorsa User.query.get(int(current_user_id_str))
    if not user:
        return jsonify({"msg":"Token geçerli ancak kullanıcı bulunamadı"}), 404
    return jsonify({"id": user.id, "username": user.username}), 200

# === FİRMA İŞLEMLERİ ===
@bp.route('/firmalar', methods=['POST'])
@jwt_required()
def create_firma():
    current_user_id = int(get_jwt_identity()) # Token'dan gelen ID string olabilir, int'e çevir.
    data = request.get_json()

    if not data or not data.get('adi') or not data.get('vkn'):
        return jsonify({"msg": "Firma adı ve VKN zorunlu alanlardır."}), 400
    
    vkn_str = str(data['vkn']).strip()
    if not vkn_str:
        return jsonify({"msg": "VKN boş bırakılamaz."}), 400

    if Firma.query.filter_by(vkn=vkn_str).first():
        return jsonify({"msg": "Bu VKN zaten kayıtlı."}), 400

    firma = Firma(adi=data['adi'], vkn=vkn_str, user_id=current_user_id)
    db.session.add(firma)
    db.session.commit()
    current_app.logger.info(f"Kullanıcı {current_user_id} yeni firma ekledi: {firma.adi} (ID: {firma.id})")
    return jsonify({"id": firma.id, "adi": firma.adi, "vkn": firma.vkn, "user_id": firma.user_id}), 201

@bp.route('/firmalar', methods=['GET'])
@jwt_required()
def get_firmalar():
    current_user_id = int(get_jwt_identity())
    # Prototip: Şimdilik tüm firmaları listeleyelim, yetkilendirme eklenebilir.
    # firmalar_query = Firma.query.filter_by(user_id=current_user_id).all() 
    firmalar_query = Firma.query.order_by(Firma.adi).all()
    firmalar_list = [{"id": f.id, "adi": f.adi, "vkn": f.vkn, "user_id": f.user_id} for f in firmalar_query]
    return jsonify(firmalar_list), 200

@bp.route('/firmalar/<int:firma_id>', methods=['GET'])
@jwt_required()
def get_firma_detay(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # İsteğe bağlı: Sadece kendi firmasını görsün
    # if firma.user_id != current_user_id:
    #     return jsonify({"msg": "Bu firmayı görüntüleme yetkiniz yok"}), 403
    return jsonify({"id": firma.id, "adi": firma.adi, "vkn": firma.vkn, "user_id": firma.user_id}), 200

@bp.route('/firmalar/<int:firma_id>', methods=['PUT'])
@jwt_required()
def update_firma(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id: 
    #      return jsonify({"msg": "Bu firmayı güncelleme yetkiniz yok"}), 403

    data = request.get_json()
    if not data:
        return jsonify({"msg": "Güncellenecek veri yok"}), 400

    if 'adi' in data and data['adi'].strip():
        firma.adi = data['adi'].strip()
    if 'vkn' in data and str(data['vkn']).strip():
        new_vkn = str(data['vkn']).strip()
        existing_firma_vkn = Firma.query.filter(Firma.vkn == new_vkn, Firma.id != firma_id).first()
        if existing_firma_vkn:
            return jsonify({"msg": "Bu VKN başka bir firmaya ait"}), 400
        firma.vkn = new_vkn
    elif 'vkn' in data and not str(data['vkn']).strip(): # VKN silinmek isteniyorsa (model zorunlu yaptığı için bu senaryo engellenmeli)
         return jsonify({"msg": "VKN boş bırakılamaz."}), 400
    
    db.session.commit()
    current_app.logger.info(f"Kullanıcı {current_user_id} firma güncelledi: {firma.adi} (ID: {firma.id})")
    return jsonify({"id": firma.id, "adi": firma.adi, "vkn": firma.vkn}), 200

@bp.route('/firmalar/<int:firma_id>', methods=['DELETE'])
@jwt_required()
def delete_firma(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id: 
    #      return jsonify({"msg": "Bu firmayı silme yetkiniz yok"}), 403
    
    firma_adi_log = firma.adi # Silmeden önce adını alalım
    db.session.delete(firma)
    db.session.commit()
    current_app.logger.info(f"Kullanıcı {current_user_id} firma sildi: {firma_adi_log} (ID: {firma_id})")
    return jsonify({"msg": f"'{firma_adi_log}' adlı firma başarıyla silindi"}), 200

# === FİNANSAL VERİ İŞLEMLERİ ===
@bp.route('/firmalar/<int:firma_id>/upload_financials', methods=['POST'])
@jwt_required()
def upload_financials(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id:
    #      return jsonify({"msg": "Bu firmanın verisini yükleme yetkiniz yok"}), 403

    if 'file' not in request.files:
        return jsonify({"msg": "Dosya bulunamadı"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"msg": "Dosya seçilmedi"}), 400
    
    if file and file.filename.endswith('.csv'):
        try:
            df = pd.read_csv(io.StringIO(file.read().decode('utf-8-sig')))
            if df.empty: return jsonify({"msg": "CSV dosyası boş"}), 400
            
            processed_count = 0
            for index, data_row in df.iterrows():
                donem = data_row.get('Donem')
                if not donem or pd.isna(donem):
                    current_app.logger.warning(f"Firma {firma_id}, Satır {index+1}: 'Donem' bilgisi eksik, atlanıyor.")
                    continue

                FinansalVeri.query.filter_by(firma_id=firma.id, donem=str(donem)).delete()
                
                veri_dict = {'firma_id': firma.id, 'donem': str(donem)}
                model_columns = [column.key for column in FinansalVeri.__table__.columns]

                for csv_col_name in df.columns:
                    # CSV sütun adlarını modeldeki alan adlarına (snake_case) dönüştürmeye çalışalım
                    # Örnek: 'AktifToplami' -> 'aktif_toplami'
                    # Basit bir dönüşüm, daha karmaşık eşlemeler gerekebilir.
                    model_field_candidate = csv_col_name.lower() # Basitçe küçük harfe çevirelim
                    # Veya CSV'deki sütun adlarının modeldekiyle (snake_case) birebir aynı olduğunu varsayalım.
                    # Bu örnekte CSV'deki sütun adlarının modeldekiyle aynı olduğunu varsayıyorum (örn: aktif_toplami)
                    
                    if csv_col_name.lower() in model_columns: # Modelde böyle bir alan var mı? (küçük harf karşılaştırması)
                        try:
                            value = data_row.get(csv_col_name)
                            if pd.isna(value):
                                veri_dict[csv_col_name.lower()] = None
                            else:
                                veri_dict[csv_col_name.lower()] = float(value)
                        except ValueError:
                            current_app.logger.warning(f"Firma {firma_id}, Dönem {str(donem)}, Sütun {csv_col_name}: Geçersiz sayısal değer '{data_row.get(csv_col_name)}'. None olarak ayarlandı.")
                            veri_dict[csv_col_name.lower()] = None
                    elif csv_col_name != 'Donem':
                        current_app.logger.warning(f"Firma {firma_id}, CSV Sütunu '{csv_col_name}' modelde bulunamadı.")
                
                veri = FinansalVeri(**{k: v for k, v in veri_dict.items() if k in model_columns}) # Sadece modelde olanları ata
                db.session.add(veri)
                processed_count += 1
            
            if processed_count > 0:
                db.session.commit()
                current_app.logger.info(f"Kullanıcı {current_user_id}, Firma {firma_id} için {processed_count} döneme ait finansal veri yükledi/güncelledi.")
                return jsonify({"msg": f"{firma.adi} için {processed_count} döneme ait finansal veriler işlendi."}), 201
            else:
                return jsonify({"msg": "CSV'den işlenecek geçerli veri satırı bulunamadı."}), 400
        except pd.errors.EmptyDataError:
            return jsonify({"msg": "CSV dosyası veri içermiyor veya formatı bozuk."}), 400
        except Exception as e:
            db.session.rollback()
            current_app.logger.error(f"Finansal veri yükleme hatası (Firma ID: {firma_id}): {e}", exc_info=True)
            return jsonify({"msg": "Dosya işlenirken beklenmedik bir hata oluştu.", "error": str(e)}), 500
    else:
        return jsonify({"msg": "Geçersiz dosya formatı. Lütfen CSV kullanın."}), 400

@bp.route('/firmalar/<int:firma_id>/finansal_veriler', methods=['GET'])
@jwt_required()
def get_finansal_veriler(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id: 
    #      return jsonify({"msg": "Bu firmanın verilerini görüntüleme yetkiniz yok"}), 403

    donem_param = request.args.get('donem')
    query = FinansalVeri.query.filter_by(firma_id=firma.id)
    if donem_param:
        query = query.filter_by(donem=donem_param)
    veriler = query.order_by(FinansalVeri.donem.desc()).all()
    
    if not veriler:
        return jsonify({"msg": "Belirtilen kriterlere uygun finansal veri bulunamadı."}), 404
        
    result = [{col.name: getattr(v, col.name) for col in v.__table__.columns} for v in veriler]
    return jsonify(result), 200

# === FİNANSAL ANALİZ İŞLEMLERİ ===
@bp.route('/firmalar/<int:firma_id>/finansal_analiz', methods=['GET'])
@jwt_required()
def get_finansal_analiz(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id: 
    #      return jsonify({"msg": "Bu firmanın analizini görüntüleme yetkiniz yok"}), 403

    donem_param = request.args.get('donem')
    fin_veri_query = FinansalVeri.query.filter_by(firma_id=firma.id)
    if donem_param:
        fin_veri = fin_veri_query.filter_by(donem=donem_param).first()
        if not fin_veri: return jsonify({"msg": f"{donem_param} dönemi için finansal veri bulunamadı."}), 404
    else:
        fin_veri = fin_veri_query.order_by(FinansalVeri.donem.desc()).first()
        if not fin_veri: return jsonify({"msg": "Firma için analiz edilecek finansal veri bulunamadı."}), 404

    cari_oran_val = calculate_cari_oran(fin_veri.donen_varliklar, fin_veri.kisa_vadeli_yukumlulukler)
    borc_ozkaynak_orani_val = calculate_borc_ozkaynak_orani(fin_veri.toplam_yukumlulukler, fin_veri.oz_kaynaklar)
    altman_z_skoru_val = calculate_altman_z_score_updated(
        fin_veri.donen_varliklar, fin_veri.aktif_toplami, fin_veri.kisa_vadeli_yukumlulukler,
        fin_veri.dagitilmamis_karlar, fin_veri.vergi_oncesi_kar_zarar, fin_veri.oz_kaynaklar,
        fin_veri.toplam_yukumlulukler, fin_veri.net_satislar
    )

    fin_veri.cari_oran = cari_oran_val
    fin_veri.borc_ozkaynak_orani = borc_ozkaynak_orani_val
    fin_veri.altman_z_skoru = altman_z_skoru_val
    db.session.commit()
    
    altman_yorum = "Hesaplanamadı veya Veri Yetersiz"
    if altman_z_skoru_val is not None:
        if altman_z_skoru_val > 2.99: altman_yorum = "Güvenli Bölge (Düşük İflas Riski)"
        elif altman_z_skoru_val < 1.81: altman_yorum = "Riskli Bölge (Yüksek İflas Riski)"
        else: altman_yorum = "Belirsiz Bölge (Gri Alan)"
        
    return jsonify({
        "firma_adi": firma.adi,
        "analiz_donemi": fin_veri.donem,
        "hesaplanan_oranlar": {
            "cari_oran": cari_oran_val,
            "borc_bolu_ozkaynak_orani": borc_ozkaynak_orani_val,
        },
        "risk_skorlari": {
            "altman_z_skoru": altman_z_skoru_val,
            "altman_z_skoru_yorum": altman_yorum
        }
    }), 200
# app/routes.py dosyasının sonlarına doğru, diğer route'lardan sonra eklenebilir
import xml.etree.ElementTree as ET # XML işleme için Python'un standart kütüphanesi
# Daha gelişmiş XML işlemleri için lxml kütüphanesi de değerlendirilebilir: pip install lxml

@bp.route('/firmalar/<int:firma_id>/upload_edefter_xml', methods=['POST'])
@jwt_required()
def upload_edefter_xml(firma_id):
    current_user_id = int(get_jwt_identity())
    firma = Firma.query.get_or_404(firma_id)
    # if firma.user_id != current_user_id:
    #     return jsonify({"msg": "Bu firmanın verisini yükleme yetkiniz yok"}), 403

    if 'file' not in request.files:
        return jsonify({"msg": "Dosya bulunamadı"}), 400

    xml_file = request.files['file']

    if xml_file.filename == '':
        return jsonify({"msg": "Dosya seçilmedi"}), 400

    if xml_file and xml_file.filename.endswith('.xml'):
        try:
            # Dosyayı güvenli bir şekilde oku (içeriği doğrudan parse etmeden önce)
            xml_content_bytes = xml_file.read()
            xml_content_str = xml_content_bytes.decode('utf-8') # Genellikle UTF-8 olur

            # --------------------------------------------------------------------
            # XML PARSE ETME VE VERİ ÇIKARMA MANTIĞI BURAYA GELECEK
            # Bu kısım e-defterin yapısına göre çok detaylı olacaktır.
            # Aşağıda basit bir örnek verilecek.
            # --------------------------------------------------------------------

            # Örnek: XML'den basit bir veri çekme ve loglama
            # root = ET.fromstring(xml_content_str)
            # ornegin_bir_deger = root.findtext('.//OrnekBirAlan') # XPath ile bir alan bulma
            # current_app.logger.info(f"E-defterden okunan örnek değer: {ornegin_bir_deger}")

            # --------------------------------------------------------------------
            # ÇIKARILAN VERİLERİ FinansalVeri MODELİNE İŞLEME MANTIĞI
            # --------------------------------------------------------------------
            # Bu aşamada, parse edilen XML'den aldığınız toplamları,
            # FinansalVeri modelindeki alanlara (aktif_toplami, net_satislar vb.)
            # uygun bir dönem için kaydetmeniz gerekecek.
            # Bu, ciddi bir muhasebe ve veri eşleştirme mantığı gerektirir.
            # 
            # Örnek:
            # donem_bilgisi = "2024-YILSONU" # XML'den çıkarılmalı
            # aktif_toplami_xml = ... # XML'den hesaplanmalı
            # net_satislar_xml = ... # XML'den hesaplanmalı
            #
            # FinansalVeri.query.filter_by(firma_id=firma.id, donem=donem_bilgisi).delete() # Eski veriyi sil
            # yeni_veri = FinansalVeri(
            #     firma_id=firma.id,
            #     donem=donem_bilgisi,
            #     aktif_toplami=aktif_toplami_xml,
            #     net_satislar=net_satislar_xml,
            #     # ... diğer alanlar ...
            # )
            # db.session.add(yeni_veri)
            # db.session.commit()
            # --------------------------------------------------------------------

            current_app.logger.info(f"Kullanıcı {current_user_id}, Firma {firma_id} için e-defter dosyası '{xml_file.filename}' yüklendi ve işlenmeye çalışıldı.")
            return jsonify({"msg": f"E-defter dosyası '{xml_file.filename}' alındı. İşleme mantığı henüz tam olarak geliştirilmedi."}), 200

        except ET.ParseError as pe:
            current_app.logger.error(f"E-defter XML parse hatası (Firma ID: {firma_id}): {pe}", exc_info=True)
            return jsonify({"msg": "XML dosyası ayrıştırılırken hata oluştu. Lütfen dosya formatını kontrol edin.", "error": str(pe)}), 400
        except UnicodeDecodeError as ude:
            current_app.logger.error(f"E-defter dosya kodlama hatası (Firma ID: {firma_id}): {ude}", exc_info=True)
            return jsonify({"msg": "Dosya karakter kodlaması hatası. Dosyanın UTF-8 olduğundan emin olun.", "error": str(ude)}), 400
        except Exception as e:
            current_app.logger.error(f"E-defter yükleme sırasında beklenmedik hata (Firma ID: {firma_id}): {e}", exc_info=True)
            return jsonify({"msg": "E-defter yüklenirken beklenmedik bir hata oluştu.", "error": str(e)}), 500
    else:
        return jsonify({"msg": "Geçersiz dosya formatı. Lütfen .xml uzantılı bir e-defter dosyası yükleyin."}), 400
# app/routes.py içine eklenecek
# from app.financial_statement_service import get_hesap_bakiyeleri, generate_bilanco_from_hesap_bakiyeleri, generate_gelir_tablosu_from_hesap_bakiyeleri # Eğer ayrı bir dosyaya taşıdıysanız
# Veya services.py içindeyse oradan import edin. Şimdilik aynı dosyada olduğunu varsayalım.

@bp.route('/firmalar/<int:firma_id>/mali_tablolar', methods=['GET'])
@jwt_required()
def get_mali_tablolar(firma_id):
    current_app.logger.debug(f"/mali_tablolar endpoint'i çağrıldı. Firma ID: {firma_id}")
    # current_user_id = int(get_jwt_identity()) # Yetkilendirme için kullanılabilir
    firma = Firma.query.get_or_404(firma_id)
    # Yetkilendirme kontrolü eklenebilir

    donem_baslangic_str = request.args.get('donem_baslangic') # YYYY-AA-GG
    donem_bitis_str = request.args.get('donem_bitis')       # YYYY-AA-GG

    if not donem_baslangic_str or not donem_bitis_str:
        return jsonify({"msg": "Lütfen 'donem_baslangic' ve 'donem_bitis' parametrelerini YYYY-AA-GG formatında sağlayın."}), 400
    
    try:
        donem_baslangic_date = datetime.strptime(donem_baslangic_str, '%Y-%m-%d').date()
        donem_bitis_date = datetime.strptime(donem_bitis_str, '%Y-%m-%d').date()
        if donem_baslangic_date > donem_bitis_date:
            return jsonify({"msg": "Başlangıç tarihi bitiş tarihinden sonra olamaz."}), 400
    except ValueError:
        return jsonify({"msg": "Geçersiz tarih formatı. Lütfen YYYY-AA-GG kullanın."}), 400

    try:
        current_app.logger.info(f"Firma {firma_id} için {donem_baslangic_str} - {donem_bitis_str} dönemi hesap bakiyeleri çekiliyor...")
        hesap_bakiyeleri_donem = get_hesap_bakiyeleri_for_period(firma_id, donem_baslangic_date, donem_bitis_date)
        
        if not hesap_bakiyeleri_donem:
            current_app.logger.warning(f"Firma {firma_id}, Dönem {donem_baslangic_str}-{donem_bitis_str} için hesap özeti bulunamadı.")
            return jsonify({"msg": f"Belirtilen dönem için işlenmiş yevmiye verisi veya hesap özeti bulunamadı."}), 404

        current_app.logger.info(f"Firma {firma_id} için Bilanço oluşturuluyor...")
        bilanco = generate_bilanco(hesap_bakiyeleri_donem)
        
        current_app.logger.info(f"Firma {firma_id} için Gelir Tablosu oluşturuluyor...")
        gelir_tablosu = generate_gelir_tablosu(hesap_bakiyeleri_donem)
        
        # İsteğe bağlı: Bu türetilmiş özetleri FinansalVeri tablosuna kaydetme mantığı
        # (Bir önceki yanıtta bahsedilmişti, buraya eklenebilir)
        # Örneğin:
        # ozet_donem_adi = f"{donem_bitis_date.year}-{donem_bitis_date.month:02d}" # Veya daha uygun bir dönem adı
        # FinansalVeri.query.filter_by(firma_id=firma.id, donem=ozet_donem_adi).delete()
        # yeni_ozet_veri = FinansalVeri(
        #     firma_id=firma.id,
        #     donem=ozet_donem_adi,
        #     aktif_toplami = bilanco.get('AKTIFLER', {}).get('GENEL_TOPLAM', 0),
        #     donen_varliklar = bilanco.get('AKTIFLER', {}).get('I. DÖNEN VARLIKLAR', {}).get('TOPLAM', 0),
        #     # ... diğer bilanço kalemleri ...
        #     kisa_vadeli_yukumlulukler = bilanco.get('PASIFLER', {}).get('III. KISA VADELİ YABANCI KAYNAKLAR', {}).get('TOPLAM', 0),
        #     oz_kaynaklar = bilanco.get('PASIFLER', {}).get('V. ÖZKAYNAKLAR', {}).get('TOPLAM', 0),
        #     net_satislar = gelir_tablosu.get('NET SATIŞLAR', 0),
        #     # ... diğer gelir tablosu kalemleri ...
        # )
        # db.session.add(yeni_ozet_veri)
        # db.session.commit()
        # current_app.logger.info(f"Firma {firma_id}, Dönem {ozet_donem_adi} için özet FinansalVeri tablosu güncellendi/oluşturuldu.")


    except Exception as e:
        current_app.logger.error(f"Mali tablo API'sinde hata (Firma ID: {firma_id}, Dönem: {donem_baslangic_str}-{donem_bitis_str}): {e}", exc_info=True)
        return jsonify({"msg": "Mali tablolar alınırken sunucu içi bir hata oluştu."}), 500
        
    return jsonify({
        "firma_id": firma_id,
        "donem_baslangic": donem_baslangic_str,
        "donem_bitis": donem_bitis_str,
        "bilanco": bilanco,
        "gelir_tablosu": gelir_tablosu,
        # "debug_hesap_bakiyeleri": hesap_bakiyeleri_donem # Debug için
    }), 200
