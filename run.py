from app import create_app, db
from app.models import User, Firma, FinansalVeri, YevmiyeMaddesiBasligi, YevmiyeFisiSatiri

from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

app = create_app()


#DENEME 
@app.shell_context_processor
def make_shell_context():
    return {
        'db': db, 
        'User': User, 
        'Firma': Firma, 
        'FinansalVeri': FinansalVeri,
        'YevmiyeMaddesiBasligi': YevmiyeMaddesiBasligi,
        'YevmiyeFisiSatiri': YevmiyeFisiSatiri
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    # Gunicorn production'da bu dosyayı doğrudan çalıştırmaz, app objesini kullanır.
    # Lokal geliştirme için:
    app.run(debug=True, host='0.0.0.0', port=port)