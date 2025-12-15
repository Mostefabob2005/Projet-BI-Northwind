
##  Installation

### Prérequis
- Python 3.8+
- SQL Server Express
- Git

### Installation
```bash
# 1. Cloner le repository
git clone https://github.com/votre-username/projet-bi.git
cd projet-bi

# 2. Créer environnement virtuel
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Linux/Mac

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Configurer la base de données
# Copier config/config.example.py en config/config.py
# Modifier les paramètres de connexion
