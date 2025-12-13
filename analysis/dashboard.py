# ==================== DÉBUT DU FICHIER ====================
import os
import sys

# Obtenir le chemin du répertoire parent (Projet_BI)
# dashboard.py est dans analysis/, donc on remonte d'un niveau
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ajouter le chemin racine pour pouvoir importer config
sys.path.insert(0, PROJECT_ROOT)

print(f"🔍 Chemin du projet: {PROJECT_ROOT}")
print(f"📁 Emplacement dashboard: {os.path.dirname(os.path.abspath(__file__))}")

# Maintenant on peut importer depuis le dossier config
try:
    # Importer le module config depuis le dossier config
    from config import config
    print("✅ Import depuis config/config.py réussi!")
except ImportError as e:
    print(f"❌ Échec initial: {e}")
    
    # Essayer une autre méthode
    try:
        # Ajouter explicitement le chemin du dossier config
        config_path = os.path.join(PROJECT_ROOT, "config")
        sys.path.insert(0, config_path)
        import config
        print("✅ Import réussi via ajout explicite du chemin!")
    except ImportError as e2:
        print(f"❌ Échec secondaire: {e2}")
        
        # Dernière tentative : importer directement
        try:
            config_path = os.path.join(PROJECT_ROOT, "config", "config.py")
            if os.path.exists(config_path):
                # Méthode d'importation manuelle
                import importlib.util
                spec = importlib.util.spec_from_file_location("config_module", config_path)
                config = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(config)
                print("✅ Import réussi via importlib!")
            else:
                print(f"❌ Fichier non trouvé: {config_path}")
                sys.exit(1)
        except Exception as e3:
            print(f"❌ Échec final: {e3}")
            print("📁 Structure des dossiers:")
            for root, dirs, files in os.walk(PROJECT_ROOT):
                level = root.replace(PROJECT_ROOT, '').count(os.sep)
                indent = ' ' * 2 * level
                print(f'{indent}{os.path.basename(root)}/')
                subindent = ' ' * 2 * (level + 1)
                for file in files:
                    print(f'{subindent}{file}')
            sys.exit(1)

# ==================== FIN DU HACK ====================

import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
import pyodbc
from datetime import datetime

# Connexion DWH
try:
    # Utiliser la fonction get_engine de config
    engine = config.get_engine('dwh')
    print("✅ Moteur de base de données créé avec succès")
except AttributeError:
    # Fallback : créer une connexion pyodbc directe
    print("⚠️ Utilisation de pyodbc comme fallback...")
    connection_string = config.get_connection_string('dwh')
    engine = pyodbc.connect(connection_string)
    print("✅ Connexion pyodbc établie")

# Test de connexion
try:
    # Vérifier quelles tables existent
    test_query = """
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    tables = pd.read_sql(test_query, engine)
    print(f"✅ Connexion réussie! Tables disponibles dans DWH_Northwind:")
    for table in tables['TABLE_NAME'].values:
        print(f"   - {table}")
    
    # Vérifier spécifiquement les tables de dimensions
    required_tables = ['Dim_Client', 'Dim_Produit', 'Fact_Ventes']
    for table in required_tables:
        check_query = f"SELECT COUNT(*) as count FROM {table}"
        try:
            result = pd.read_sql(check_query, engine)
            print(f"   ✓ {table}: {result['count'][0]} lignes")
        except:
            print(f"   ✗ {table}: Non disponible")
            
except Exception as e:
    print(f"❌ Erreur de connexion: {e}")
    # Créer un dataframe de test pour le développement
    print("⚠️ Utilisation de données de test pour le développement")
    df = pd.DataFrame({
        'Annee': [2024, 2024, 2023, 2023],
        'Mois': [1, 2, 12, 11],
        'NomMois': ['Janvier', 'Février', 'Décembre', 'Novembre'],
        'Client': ['Client A', 'Client B', 'Client A', 'Client C'],
        'Pays': ['France', 'USA', 'France', 'Germany'],
        'Produit': ['Produit 1', 'Produit 2', 'Produit 1', 'Produit 3'],
        'Categorie': ['Catégorie 1', 'Catégorie 2', 'Catégorie 1', 'Catégorie 3'],
        'ChiffreAffaires': [1000.50, 2500.75, 1500.00, 800.25],
        'QuantiteVendue': [10, 25, 15, 8],
        'NombreCommandes': [2, 3, 1, 1]
    })
    
    # Dashboard avec données de test
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.H1("📊 Dashboard Northwind - Mode Démo"),
        html.H3("⚠️ Données de test - Exécutez main_etl.py d'abord"),
        html.P("Les tables DWH ne sont pas encore créées."),
        html.P("Exécutez 'python main_etl.py' pour créer la base de données."),
        dcc.Graph(
            figure=px.bar(df, x='Client', y='ChiffreAffaires', 
                         title='Données de démonstration')
        )
    ])
    
    if __name__ == '__main__':
        print("\n🚀 Lancement du dashboard en mode démo...")
        print("🌐 Ouvrez http://localhost:8050 dans votre navigateur")
        print("💡 Conseil: Exécutez d'abord main_etl.py pour avoir les vraies données")
        app.run_server(debug=True, port=8050)
    sys.exit(0)

# ==================== REQUÊTE PRINCIPALE ====================
# Version adaptée à votre schéma DWH
query = """
SELECT TOP 1000  -- Limiter pour les tests
    YEAR(fv.DateChargement) as Annee,
    MONTH(fv.DateChargement) as Mois,
    FORMAT(fv.DateChargement, 'MMMM', 'fr-FR') as NomMois,
    dc.CompanyName as Client,
    dc.Country as Pays,
    dp.ProductName as Produit,
    dp.CategoryName as Categorie,
    SUM(fv.MontantVente) as ChiffreAffaires,
    SUM(fv.Quantite) as QuantiteVendue,
    COUNT(DISTINCT fv.OrderID) as NombreCommandes
FROM Fact_Ventes fv
JOIN Dim_Client dc ON fv.CustomerID = dc.CustomerID
JOIN Dim_Produit dp ON fv.ProductID = dp.ProductID
GROUP BY 
    YEAR(fv.DateChargement),
    MONTH(fv.DateChargement),
    FORMAT(fv.DateChargement, 'MMMM', 'fr-FR'),
    dc.CompanyName,
    dc.Country,
    dp.ProductName,
    dp.CategoryName
ORDER BY Annee DESC, Mois DESC
"""

# Alternative si DateChargement n'existe pas
query_alternative = """
SELECT 
    c.CompanyName as Client,
    c.Country as Pays,
    p.ProductName as Produit,
    p.CategoryName as Categorie,
    SUM(f.MontantVente) as ChiffreAffaires,
    SUM(f.Quantite) as QuantiteVendue,
    COUNT(DISTINCT f.OrderID) as NombreCommandes
FROM Fact_Ventes f
JOIN Dim_Client c ON f.CustomerID = c.CustomerID
JOIN Dim_Produit p ON f.ProductID = p.ProductID
GROUP BY 
    c.CompanyName,
    c.Country,
    p.ProductName,
    p.CategoryName
ORDER BY ChiffreAffaires DESC
"""

# Charger les données
try:
    print("\n📥 Chargement des données depuis DWH_Northwind...")
    df = pd.read_sql(query, engine)
    
    if len(df) == 0:
        print("⚠️ Aucune donnée avec la première requête, essai alternative...")
        df = pd.read_sql(query_alternative, engine)
    
    print(f"✅ Données chargées: {len(df)} lignes")
    
    # Afficher un aperçu
    print(f"\n📊 Aperçu des données (5 premières lignes):")
    print(df.head())
    
    # Statistiques basiques
    print(f"\n📈 Statistiques:")
    print(f"   - Période: {df['Annee'].min() if 'Annee' in df.columns else 'N/A'} - {df['Annee'].max() if 'Annee' in df.columns else 'N/A'}")
    print(f"   - Nombre de clients: {df['Client'].nunique()}")
    print(f"   - Nombre de produits: {df['Produit'].nunique()}")
    print(f"   - CA total: {df['ChiffreAffaires'].sum():,.2f} €")
    
except Exception as e:
    print(f"❌ Erreur lors du chargement des données: {e}")
    # Utiliser des données de base si nécessaire
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==================== DASHBOARD ====================
app = dash.Dash(__name__)

# Styles CSS
styles = {
    'container': {
        'padding': '20px',
        'fontFamily': 'Arial, sans-serif'
    },
    'header': {
        'backgroundColor': '#2c3e50',
        'color': 'white',
        'padding': '20px',
        'borderRadius': '10px',
        'marginBottom': '20px'
    },
    'card': {
        'backgroundColor': 'white',
        'padding': '15px',
        'borderRadius': '8px',
        'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
        'marginBottom': '15px'
    },
    'stats': {
        'display': 'flex',
        'justifyContent': 'space-around',
        'marginBottom': '20px'
    },
    'statBox': {
        'textAlign': 'center',
        'padding': '15px',
        'backgroundColor': '#f8f9fa',
        'borderRadius': '8px',
        'flex': '1',
        'margin': '0 10px'
    }
}

# Calculer les KPI
total_ca = df['ChiffreAffaires'].sum()
total_commandes = df['NombreCommandes'].sum()
moyenne_panier = total_ca / total_commandes if total_commandes > 0 else 0
top_client = df.groupby('Client')['ChiffreAffaires'].sum().idxmax()
top_pays = df.groupby('Pays')['ChiffreAffaires'].sum().idxmax()

app.layout = html.Div(style=styles['container'], children=[
    # En-tête
    html.Div(style=styles['header'], children=[
        html.H1("📊 Dashboard Northwind - Business Intelligence"),
        html.P("Analyse des ventes et performance commerciale"),
        html.P(f"Dernière mise à jour: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    ]),
    
    # KPI
    html.Div(style=styles['stats'], children=[
        html.Div(style=styles['statBox'], children=[
            html.H3(f"{total_ca:,.2f} €"),
            html.P("Chiffre d'affaires total", style={'color': '#7f8c8d'})
        ]),
        html.Div(style=styles['statBox'], children=[
            html.H3(f"{total_commandes:,}"),
            html.P("Nombre de commandes", style={'color': '#7f8c8d'})
        ]),
        html.Div(style=styles['statBox'], children=[
            html.H3(f"{moyenne_panier:,.2f} €"),
            html.P("Panier moyen", style={'color': '#7f8c8d'})
        ]),
        html.Div(style=styles['statBox'], children=[
            html.H3(top_client[:15] + "..."),
            html.P("Meilleur client", style={'color': '#7f8c8d'})
        ]),
    ]),
    
    # Première ligne de graphiques
    html.Div([
        # Graphique 1: CA annuel
        html.Div(style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}, children=[
            html.Div(style=styles['card'], children=[
                html.H4("📈 Chiffre d'affaires annuel"),
                dcc.Graph(
                    id='ca-annuel',
                    figure=px.bar(
                        df.groupby('Annee')['ChiffreAffaires'].sum().reset_index(),
                        x='Annee',
                        y='ChiffreAffaires',
                        title='',
                        color='ChiffreAffaires',
                        color_continuous_scale='Viridis',
                        labels={'ChiffreAffaires': 'CA (€)', 'Annee': 'Année'}
                    ).update_layout(height=400)
                )
            ])
        ]),
        
        # Graphique 2: Top clients
        html.Div(style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '4%'}, children=[
            html.Div(style=styles['card'], children=[
                html.H4("👥 Top 10 clients"),
                dcc.Graph(
                    id='top-clients',
                    figure=px.pie(
                        df.groupby('Client')['ChiffreAffaires'].sum()
                        .nlargest(10).reset_index(),
                        values='ChiffreAffaires',
                        names='Client',
                        title='',
                        hole=0.4,
                        color_discrete_sequence=px.colors.qualitative.Set3
                    ).update_layout(height=400)
                )
            ])
        ]),
    ]),
    
    # Deuxième ligne de graphiques
    html.Div([
        # Graphique 3: Ventes par catégorie
        html.Div(style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}, children=[
            html.Div(style=styles['card'], children=[
                html.H4("📦 Ventes par catégorie"),
                dcc.Graph(
                    id='ventes-par-categorie',
                    figure=px.bar(
                        df.groupby('Categorie')['ChiffreAffaires'].sum()
                        .reset_index().sort_values('ChiffreAffaires', ascending=False),
                        x='Categorie',
                        y='ChiffreAffaires',
                        title='',
                        color='ChiffreAffaires',
                        color_continuous_scale='Blues',
                        labels={'ChiffreAffaires': 'CA (€)', 'Categorie': 'Catégorie'}
                    ).update_layout(height=400)
                )
            ])
        ]),
        
        # Graphique 4: Répartition géographique
        html.Div(style={'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '4%'}, children=[
            html.Div(style=styles['card'], children=[
                html.H4("🌍 Répartition géographique"),
                dcc.Graph(
                    id='ventes-par-pays',
                    figure=px.treemap(
                        df.groupby(['Pays', 'Client'])['ChiffreAffaires'].sum().reset_index(),
                        path=['Pays', 'Client'],
                        values='ChiffreAffaires',
                        color='ChiffreAffaires',
                        color_continuous_scale='Greens',
                        title=''
                    ).update_layout(height=400)
                )
            ])
        ]),
    ]),
    
    # Tableau de données (optionnel)
    html.Div(style=styles['card'], children=[
        html.H4("📋 Aperçu des données"),
        html.Div([
            html.P(f"Affichage de {min(10, len(df))} lignes sur {len(df)} totales"),
            html.Table(
                # En-tête
                [html.Tr([html.Th(col) for col in df.columns[:6]])] +
                # Lignes de données
                [html.Tr([html.Td(df.iloc[i][col]) for col in df.columns[:6]]) 
                 for i in range(min(10, len(df)))],
                style={'width': '100%', 'borderCollapse': 'collapse'}
            )
        ], style={'overflowX': 'auto'})
    ]),
    
    # Pied de page
    html.Div(style={'marginTop': '30px', 'textAlign': 'center', 'color': '#7f8c8d'}, children=[
        html.Hr(),
        html.P("Dashboard Northwind BI - Powered by Python, SQL Server & Plotly Dash"),
        html.P(f"Données extraites de DWH_Northwind • {len(df)} enregistrements analysés")
    ])
])

# ==================== LANCEMENT ====================
if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 LANCEMENT DU DASHBOARD NORTHWIND BI")
    print("="*60)
    print(f"📊 Données chargées: {len(df)} lignes")
    print(f"💰 Chiffre d'affaires total: {total_ca:,.2f} €")
    print(f"📦 Nombre de produits: {df['Produit'].nunique()}")
    print(f"👥 Nombre de clients: {df['Client'].nunique()}")
    
    # Déterminer le port
    port = 8050
    print(f"🌐 Tentative de lancement sur le port {port}...")
    print("="*60)
    print("💡 Appuyez sur Ctrl+C pour arrêter le serveur")
    print("="*60)
    
    # Lancer le serveur avec gestion des erreurs de port
    try:
        # Méthode moderne (Dash 2.0+)
        app.run(debug=True, port=port, host='127.0.0.1')
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"⚠️ Le port {port} est occupé, tentative sur le port {port+1}...")
            app.run(debug=True, port=port+1, host='127.0.0.1')
        else:
            raise e