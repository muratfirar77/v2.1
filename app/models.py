from app import db
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, date 
from decimal import Decimal

class User(db.Model):
    __tablename__ = 'user' # Tablo adını belirtmek iyi bir pratiktir
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), index=True, unique=True, nullable=False)
    password_hash = db.Column(db.String(256))

    firmalar = db.relationship('Firma', backref='sahibi_ref', lazy='dynamic') # backref adı düzeltildi

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f'<User {self.username}>'

# Firma Tipi için sabitler (Enum yerine basit bir liste)
FIRMA_TIPLERI = [
    "Anonim Şirket", "Limited Şirket", "Şahıs İşletmesi", 
    "Kollektif Şirket", "Komandit Şirket", "Kooperatif", "Diğer"
]

class Firma(db.Model):
    __tablename__ = 'firma'
    id = db.Column(db.Integer, primary_key=True)
    adi = db.Column(db.String(120), nullable=False, index=True)
    kurulus_tarihi = db.Column(db.Date, nullable=True)
    firma_tipi = db.Column(db.String(50), nullable=False)
    faaliyet_alani = db.Column(db.Text, nullable=True)
    vkn = db.Column(db.String(20), unique=True, nullable=False, index=True) # vkn için index eklendi
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    # İlişkiler daha önce tanımlanmıştı, backref isimleri kontrol edildi.
    # sahibi = db.relationship('User', backref='firmalar') -> User modelinde zaten var
    finansal_veriler = db.relationship('FinansalVeri', backref='firma_detay_ref', lazy='dynamic', cascade="all, delete-orphan") # backref adı düzeltildi
    yevmiye_maddeleri = db.relationship('YevmiyeMaddesiBasligi', backref='firma_baslik_ref', lazy='dynamic', cascade="all, delete-orphan") # backref adı düzeltildi


    def __repr__(self):
        return f'<Firma {self.id}: {self.adi} - Tipi: {self.firma_tipi}>'

class FinansalVeri(db.Model):
    __tablename__ = 'finansal_veri'
    id = db.Column(db.Integer, primary_key=True)
    firma_id = db.Column(db.Integer, db.ForeignKey('firma.id'), nullable=False, index=True) # index eklendi
    donem = db.Column(db.String(50), nullable=False, index=True) # index eklendi

    # Sayısal alanlar için Numeric kullanmak, özellikle parasal değerlerde Float'a göre daha iyidir.
    # Ancak mevcut yapıda Float ile devam ediyorsak tutarlı olalım.
    # Eğer Decimal/Numeric kullanılacaksa, default değerler de Decimal olmalı.
    donen_varliklar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    duran_varliklar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    aktif_toplami = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    kisa_vadeli_yukumlulukler = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    uzun_vadeli_yukumlulukler = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    toplam_yukumlulukler = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    oz_kaynaklar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    net_satislar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    satilan_malin_maliyeti = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    brut_kar_zarar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    faaliyet_giderleri = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    esas_faaliyet_kari_zarari = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    finansman_giderleri = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    vergi_oncesi_kar_zarar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    donem_net_kari_zarari = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    dagitilmamis_karlar = db.Column(db.Numeric(18, 2), default=Decimal('0.00'))
    
    cari_oran = db.Column(db.Float, nullable=True) # Bunlar hesaplanan değerler, Float kalabilir
    borc_ozkaynak_orani = db.Column(db.Float, nullable=True)
    altman_z_skoru = db.Column(db.Float, nullable=True)

    __table_args__ = (db.UniqueConstraint('firma_id', 'donem', name='_finansal_veri_firma_donem_uc'),)

    def __repr__(self):
        return f'<FinansalVeri ID: {self.id}, FirmaID: {self.firma_id}, Dönem: {self.donem}>'


class YevmiyeMaddesiBasligi(db.Model):
    __tablename__ = 'yevmiye_maddesi_basligi'
    id = db.Column(db.Integer, primary_key=True)
    firma_id = db.Column(db.Integer, db.ForeignKey('firma.id'), nullable=False, index=True)
    
    dosya_donemi_baslangic = db.Column(db.Date, nullable=True) # E-defterden gelen bilgi
    dosya_donemi_bitis = db.Column(db.Date, index=True, nullable=True) # E-defterden gelen bilgi
    orjinal_dosya_adi = db.Column(db.String(255), nullable=True)
    yuklenme_tarihi = db.Column(db.DateTime, default=datetime.utcnow)

    yevmiye_madde_no_counter = db.Column(db.String(50), index=True) # <gl-cor:entryNumberCounter>
    muhasebe_fis_no = db.Column(db.String(100), nullable=True, index=True) # <gl-cor:entryNumber>
    kayit_tarihi_giris = db.Column(db.Date, nullable=True) # <gl-cor:enteredDate>
    aciklama_baslik = db.Column(db.Text, nullable=True)     # <gl-cor:entryComment>
    toplam_borc = db.Column(db.Numeric(18, 2), nullable=False)       # <gl-bus:totalDebit>
    toplam_alacak = db.Column(db.Numeric(18, 2), nullable=False)     # <gl-bus:totalCredit>
    
    satirlar = db.relationship('YevmiyeFisiSatiri', backref='yevmiye_maddesi_ref', lazy='select', cascade="all, delete-orphan") # backref adı düzeltildi

    def __repr__(self):
        return f'<YevmiyeMaddesiBasligi ID: {self.id}, FirmaID: {self.firma_id}, MaddeNo: {self.yevmiye_madde_no_counter}>'


class YevmiyeFisiSatiri(db.Model):
    __tablename__ = 'yevmiye_fisi_satiri'
    id = db.Column(db.Integer, primary_key=True)
    yevmiye_maddesi_id = db.Column(db.Integer, db.ForeignKey('yevmiye_maddesi_basligi.id'), nullable=False, index=True)
    
    muhasebe_kayit_tarihi = db.Column(db.Date, nullable=False, index=True) # <gl-cor:postingDate>
    hesap_kodu = db.Column(db.String(50), nullable=False, index=True)    # <gl-cor:accountMainID>
    hesap_adi = db.Column(db.String(255), nullable=True)                # <gl-cor:accountMainDescription>
    alt_hesap_kodu = db.Column(db.String(50), nullable=True, index=True) # <gl-cor:accountSubID>
    alt_hesap_adi = db.Column(db.String(255), nullable=True)            # <gl-cor:accountSubDescription>
    
    borc_tutari = db.Column(db.Numeric(18, 2), default=Decimal('0.00'), nullable=False)
    alacak_tutari = db.Column(db.Numeric(18, 2), default=Decimal('0.00'), nullable=False)
    
    aciklama_satir = db.Column(db.Text, nullable=True)                  # <gl-cor:detailComment>
    belge_tipi = db.Column(db.String(50), nullable=True)                # <gl-cor:documentType>
    belge_tipi_aciklama = db.Column(db.String(255), nullable=True)     # <gl-cor:documentTypeDescription>
    belge_no = db.Column(db.String(100), nullable=True, index=True)     # <gl-cor:documentNumber>
    belge_tarihi = db.Column(db.Date, nullable=True, index=True)        # <gl-cor:documentDate>
    belge_referansi = db.Column(db.String(100), nullable=True)          # <gl-cor:documentReference>
    odeme_yontemi = db.Column(db.String(100), nullable=True)            # <gl-bus:paymentMethod>

    def __repr__(self):
        return f'<YevmiyeFisiSatiri ID: {self.id}, MaddeID: {self.yevmiye_maddesi_id}, Hesap: {self.hesap_kodu}, Borç: {self.borc_tutari}, Alacak: {self.alacak_tutari}>'
