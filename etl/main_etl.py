"""
ETL COMPLET pour Northwind -> DWH_Northwind
Exécute dans cet ordre :
1. Dimensions
2. Table de faits
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.config import get_engine, get_connection_string
import pyodbc
import pandas as pd
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class NorthwindETL:
    def __init__(self):
        print("=" * 60)
        print("🚀 ETL NORTHWIND - BUSINESS INTELLIGENCE")
        print("=" * 60)
        
        # Connexions
        self.conn_source = pyodbc.connect(get_connection_string('source'))
        self.engine_dwh = get_engine('dwh')
        
        # Connexion pyodbc directe vers DWH (pour contourner le problème)
        self.conn_dwh_pyodbc = pyodbc.connect(get_connection_string('dwh'))
        
        # Statistiques
        self.stats = {
            'start_time': datetime.now(),
            'rows_loaded': {}
        }
    
    # ====================
    # DIMENSION : CLIENTS
    # ====================
    def etl_dim_client(self):
        print("\n📊 ETL Dim_Client...")
        
        # EXTRACT
        query = """
        SELECT CustomerID, CompanyName, ContactName, ContactTitle, Address, 
              City, Region, PostalCode, Country, Phone, Fax
        FROM Customers
        """
        df = pd.read_sql(query, self.conn_source)
        print(f"  ➤ {len(df)} clients extraits")
        
        # TRANSFORM
        df = df.fillna({'Region': 'Non spécifié', 'Fax': 'Non disponible'})
        df['DateDebut'] = pd.to_datetime('today').date()
        df['Actif'] = 1
        df['SourceSystem'] = 'Python_ETL'
        
        # LOAD avec pyodbc direct (solution fiable)
        print("  📤 Chargement via pyodbc direct...")
        
        cursor = self.conn_dwh_pyodbc.cursor()
        
        try:
            print("  🔧 Vérification des contraintes FOREIGN KEY...")
            find_fk_sql = """
            SELECT 
                fk.name AS ForeignKeyName,
                OBJECT_NAME(fk.parent_object_id) AS ReferencingTable
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables t ON fkc.referenced_object_id = t.object_id
            WHERE t.name = 'Dim_Client'
            """
            
            cursor.execute(find_fk_sql)
            foreign_keys = cursor.fetchall()
            
            # Supprimer chaque contrainte FOREIGN KEY trouvée
            for fk in foreign_keys:
                fk_name = fk[0]
                table_name = fk[1]
                drop_fk_sql = f"ALTER TABLE [{table_name}] DROP CONSTRAINT [{fk_name}]"
                cursor.execute(drop_fk_sql)
                print(f"  ✓ Contrainte '{fk_name}' supprimée de '{table_name}'")
            
            if foreign_keys:
                print(f"  ✓ {len(foreign_keys)} contrainte(s) FOREIGN KEY supprimée(s)")
            
            # Supprimer la table si elle existe
            cursor.execute("IF OBJECT_ID('Dim_Client', 'U') IS NOT NULL DROP TABLE Dim_Client")
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Client supprimée")
            
            # Créer la table
            create_sql = """
            CREATE TABLE Dim_Client (
                ClientID INT IDENTITY(1,1) PRIMARY KEY,
                CustomerID NVARCHAR(5),
                CompanyName NVARCHAR(40),
                ContactName NVARCHAR(30),
                ContactTitle NVARCHAR(30),
                Address NVARCHAR(60),
                City NVARCHAR(15),
                Region NVARCHAR(15),
                PostalCode NVARCHAR(10),
                Country NVARCHAR(15),
                Phone NVARCHAR(24),
                Fax NVARCHAR(24),
                DateDebut DATE,
                Actif BIT,
                SourceSystem NVARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Client recréée")
            
            # Insérer les données
            insert_sql = """
            INSERT INTO Dim_Client 
            (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, 
            Region, PostalCode, Country, Phone, Fax, DateDebut, Actif, SourceSystem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Préparer les données
            for _, row in df.iterrows():
                cursor.execute(insert_sql, 
                              str(row['CustomerID']), str(row['CompanyName']), 
                              str(row['ContactName']), str(row['ContactTitle']), 
                              str(row['Address']), str(row['City']),
                              str(row['Region']), str(row['PostalCode']), 
                              str(row['Country']), str(row['Phone']), 
                              str(row['Fax']), row['DateDebut'], 
                              int(row['Actif']), str(row['SourceSystem']))
            
            self.conn_dwh_pyodbc.commit()
            print(f"  ✓ {len(df)} clients insérés")
            
        except Exception as e:
            self.conn_dwh_pyodbc.rollback()
            raise e
        finally:
            cursor.close()
        
        self.stats['rows_loaded']['Dim_Client'] = len(df)
        print(f"  ✅ {len(df)} clients chargés")
    
    # ====================
    # DIMENSION : PRODUITS
    # ====================
    def etl_dim_produit(self):
        print("\n📦 ETL Dim_Produit...")
        
        query = """
        SELECT 
            p.ProductID,
            p.ProductName,
            p.SupplierID,
            s.CompanyName as SupplierName,
            p.CategoryID,
            c.CategoryName,
            p.QuantityPerUnit,
            p.UnitPrice,
            p.UnitsInStock,
            p.UnitsOnOrder,
            p.ReorderLevel,
            p.Discontinued
        FROM Products p
        LEFT JOIN Categories c ON p.CategoryID = c.CategoryID
        LEFT JOIN Suppliers s ON p.SupplierID = s.SupplierID
        """
        df = pd.read_sql(query, self.conn_source)
        print(f"  ➤ {len(df)} produits extraits")
        
        # Transformation
        df = df.fillna({
            'SupplierName': 'Fournisseur inconnu',
            'CategoryName': 'Catégorie non définie'
        })
        
        df['DateDebut'] = pd.to_datetime('today').date()
        df['Actif'] = 1
        df['SourceSystem'] = 'Python_ETL_v1.0'
        
        # LOAD avec pyodbc direct
        print("  📤 Chargement via pyodbc direct...")
        
        cursor = self.conn_dwh_pyodbc.cursor()
        
        try:
            # ===== GESTION DES FOREIGN KEY =====
            print("  🔧 Vérification des contraintes FOREIGN KEY...")
            
            # Chercher toutes les contraintes FOREIGN KEY qui référencent Dim_Produit
            find_fk_sql = """
            SELECT 
                fk.name AS ForeignKeyName,
                OBJECT_NAME(fk.parent_object_id) AS ReferencingTable
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables t ON fkc.referenced_object_id = t.object_id
            WHERE t.name = 'Dim_Produit'
            """
            
            cursor.execute(find_fk_sql)
            foreign_keys = cursor.fetchall()
            
            # Supprimer chaque contrainte FOREIGN KEY trouvée
            for fk in foreign_keys:
                fk_name = fk[0]
                table_name = fk[1]
                drop_fk_sql = f"ALTER TABLE [{table_name}] DROP CONSTRAINT [{fk_name}]"
                cursor.execute(drop_fk_sql)
                print(f"  ✓ Contrainte '{fk_name}' supprimée de '{table_name}'")
            
            if foreign_keys:
                print(f"  ✓ {len(foreign_keys)} contrainte(s) FOREIGN KEY supprimée(s)")
            
            # Maintenant on peut supprimer la table
            cursor.execute("IF OBJECT_ID('Dim_Produit', 'U') IS NOT NULL DROP TABLE Dim_Produit")
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Produit supprimée")
            
            # Créer la table
            create_sql = """
            CREATE TABLE Dim_Produit (
                ProduitID INT IDENTITY(1,1) PRIMARY KEY,
                ProductID INT,
                ProductName NVARCHAR(40),
                SupplierID INT,
                SupplierName NVARCHAR(40),
                CategoryID INT,
                CategoryName NVARCHAR(15),
                QuantityPerUnit NVARCHAR(20),
                UnitPrice MONEY,
                UnitsInStock SMALLINT,
                UnitsOnOrder SMALLINT,
                ReorderLevel SMALLINT,
                Discontinued BIT,
                DateDebut DATE,
                Actif BIT,
                SourceSystem NVARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Produit recréée")
            
            # Insérer les données
            insert_sql = """
            INSERT INTO Dim_Produit 
            (ProductID, ProductName, SupplierID, SupplierName, CategoryID, CategoryName,
             QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, 
             Discontinued, DateDebut, Actif, SourceSystem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for _, row in df.iterrows():
                cursor.execute(insert_sql,
                              int(row['ProductID']), str(row['ProductName']),
                              int(row['SupplierID']) if pd.notna(row['SupplierID']) else None,
                              str(row['SupplierName']),
                              int(row['CategoryID']) if pd.notna(row['CategoryID']) else None,
                              str(row['CategoryName']),
                              str(row['QuantityPerUnit']) if pd.notna(row['QuantityPerUnit']) else None,
                              float(row['UnitPrice']),
                              int(row['UnitsInStock']) if pd.notna(row['UnitsInStock']) else 0,
                              int(row['UnitsOnOrder']) if pd.notna(row['UnitsOnOrder']) else 0,
                              int(row['ReorderLevel']) if pd.notna(row['ReorderLevel']) else 0,
                              1 if row['Discontinued'] else 0,
                              row['DateDebut'], 1, str(row['SourceSystem']))
            
            self.conn_dwh_pyodbc.commit()
            print(f"  ✓ {len(df)} produits insérés")
            
        except Exception as e:
            self.conn_dwh_pyodbc.rollback()
            raise e
        finally:
            cursor.close()
        
        self.stats['rows_loaded']['Dim_Produit'] = len(df)
        print(f"  ✅ {len(df)} produits chargés")
    
    # ====================
    # DIMENSION : EMPLOYÉS
    # ====================
    def etl_dim_employe(self):
        print("\n👥 ETL Dim_Employe...")
        
        query = """
        SELECT 
            EmployeeID,
            LastName,
            FirstName,
            Title,
            TitleOfCourtesy,
            BirthDate,
            HireDate,
            Address,
            City,
            Region,
            PostalCode,
            Country,
            HomePhone,
            Extension,
            ReportsTo
        FROM Employees
        """
        df = pd.read_sql(query, self.conn_source)
        print(f"  ➤ {len(df)} employés extraits")
        
        # Transformation
        df = df.fillna({
            'Region': 'Non spécifié',
            'ReportsTo': -1
        })
        
        df['DateDebut'] = pd.to_datetime('today').date()
        df['Actif'] = 1
        df['SourceSystem'] = 'Python_ETL_v1.0'
        
        # LOAD avec pyodbc direct
        print("  📤 Chargement via pyodbc direct...")
        
        cursor = self.conn_dwh_pyodbc.cursor()
        
        try:
            # ===== GESTION DES FOREIGN KEY =====
            print("  🔧 Vérification des contraintes FOREIGN KEY...")
            
            # Chercher toutes les contraintes FOREIGN KEY qui référencent Dim_Employe
            find_fk_sql = """
            SELECT 
                fk.name AS ForeignKeyName,
                OBJECT_NAME(fk.parent_object_id) AS ReferencingTable
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables t ON fkc.referenced_object_id = t.object_id
            WHERE t.name = 'Dim_Employe'
            """
            
            cursor.execute(find_fk_sql)
            foreign_keys = cursor.fetchall()
            
            # Supprimer chaque contrainte FOREIGN KEY trouvée
            for fk in foreign_keys:
                fk_name = fk[0]
                table_name = fk[1]
                drop_fk_sql = f"ALTER TABLE [{table_name}] DROP CONSTRAINT [{fk_name}]"
                cursor.execute(drop_fk_sql)
                print(f"  ✓ Contrainte '{fk_name}' supprimée de '{table_name}'")
            
            if foreign_keys:
                print(f"  ✓ {len(foreign_keys)} contrainte(s) FOREIGN KEY supprimée(s)")
            
            # Supprimer la table si elle existe
            cursor.execute("IF OBJECT_ID('Dim_Employe', 'U') IS NOT NULL DROP TABLE Dim_Employe")
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Employe supprimée")
            
            # Créer la table
            create_sql = """
            CREATE TABLE Dim_Employe (
                EmployeID INT IDENTITY(1,1) PRIMARY KEY,
                EmployeeID INT,
                LastName NVARCHAR(20),
                FirstName NVARCHAR(10),
                Title NVARCHAR(30),
                TitleOfCourtesy NVARCHAR(25),
                BirthDate DATE,
                HireDate DATE,
                Address NVARCHAR(60),
                City NVARCHAR(15),
                Region NVARCHAR(15),
                PostalCode NVARCHAR(10),
                Country NVARCHAR(15),
                HomePhone NVARCHAR(24),
                Extension NVARCHAR(4),
                ReportsTo INT,
                DateDebut DATE,
                Actif BIT,
                SourceSystem NVARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Employe recréée")
            
            # Insérer les données
            insert_sql = """
            INSERT INTO Dim_Employe 
            (EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate,
             HireDate, Address, City, Region, PostalCode, Country, HomePhone,
             Extension, ReportsTo, DateDebut, Actif, SourceSystem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for _, row in df.iterrows():
                cursor.execute(insert_sql,
                              int(row['EmployeeID']), str(row['LastName']), str(row['FirstName']),
                              str(row['Title']), str(row['TitleOfCourtesy']),
                              row['BirthDate'], row['HireDate'],
                              str(row['Address']), str(row['City']), str(row['Region']),
                              str(row['PostalCode']), str(row['Country']),
                              str(row['HomePhone']), str(row['Extension']),
                              int(row['ReportsTo']),
                              row['DateDebut'], 1, str(row['SourceSystem']))
            
            self.conn_dwh_pyodbc.commit()
            print(f"  ✓ {len(df)} employés insérés")
            
        except Exception as e:
            self.conn_dwh_pyodbc.rollback()
            raise e
        finally:
            cursor.close()
        
        self.stats['rows_loaded']['Dim_Employe'] = len(df)
        print(f"  ✅ {len(df)} employés chargés")
    
    # ====================
    # DIMENSION : TRANSPORTEURS
    # ====================
    def etl_dim_transporteur(self):
        print("\n🚚 ETL Dim_Transporteur...")
        
        query = """
        SELECT 
            ShipperID,
            CompanyName,
            Phone
        FROM Shippers
        """
        df = pd.read_sql(query, self.conn_source)
        print(f"  ➤ {len(df)} transporteurs extraits")
        
        # Transformation
        df['DateDebut'] = pd.to_datetime('today').date()
        df['Actif'] = 1
        df['SourceSystem'] = 'Python_ETL_v1.0'
        
        # LOAD avec pyodbc direct
        print("  📤 Chargement via pyodbc direct...")
        
        cursor = self.conn_dwh_pyodbc.cursor()
        
        try:
            # ===== GESTION DES FOREIGN KEY =====
            print("  🔧 Vérification des contraintes FOREIGN KEY...")
            
            # Chercher toutes les contraintes FOREIGN KEY qui référencent Dim_Transporteur
            find_fk_sql = """
            SELECT 
                fk.name AS ForeignKeyName,
                OBJECT_NAME(fk.parent_object_id) AS ReferencingTable
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables t ON fkc.referenced_object_id = t.object_id
            WHERE t.name = 'Dim_Transporteur'
            """
            
            cursor.execute(find_fk_sql)
            foreign_keys = cursor.fetchall()
            
            # Supprimer chaque contrainte FOREIGN KEY trouvée
            for fk in foreign_keys:
                fk_name = fk[0]
                table_name = fk[1]
                drop_fk_sql = f"ALTER TABLE [{table_name}] DROP CONSTRAINT [{fk_name}]"
                cursor.execute(drop_fk_sql)
                print(f"  ✓ Contrainte '{fk_name}' supprimée de '{table_name}'")
            
            if foreign_keys:
                print(f"  ✓ {len(foreign_keys)} contrainte(s) FOREIGN KEY supprimée(s)")
            
            # Supprimer la table si elle existe
            cursor.execute("IF OBJECT_ID('Dim_Transporteur', 'U') IS NOT NULL DROP TABLE Dim_Transporteur")
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Transporteur supprimée")
            
            # Créer la table
            create_sql = """
            CREATE TABLE Dim_Transporteur (
                TransporteurID INT IDENTITY(1,1) PRIMARY KEY,
                ShipperID INT,
                CompanyName NVARCHAR(40),
                Phone NVARCHAR(24),
                DateDebut DATE,
                Actif BIT,
                SourceSystem NVARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            self.conn_dwh_pyodbc.commit()
            print("  ✓ Table Dim_Transporteur recréée")
            
            # Insérer les données
            insert_sql = """
            INSERT INTO Dim_Transporteur 
            (ShipperID, CompanyName, Phone, DateDebut, Actif, SourceSystem)
            VALUES (?, ?, ?, ?, ?, ?)
            """
            
            for _, row in df.iterrows():
                cursor.execute(insert_sql,
                              int(row['ShipperID']), str(row['CompanyName']),
                              str(row['Phone']), row['DateDebut'], 1,
                              str(row['SourceSystem']))
            
            self.conn_dwh_pyodbc.commit()
            print(f"  ✓ {len(df)} transporteurs insérés")
            
        except Exception as e:
            self.conn_dwh_pyodbc.rollback()
            raise e
        finally:
            cursor.close()
        
        self.stats['rows_loaded']['Dim_Transporteur'] = len(df)
        print(f"  ✅ {len(df)} transporteurs chargés")
    
    # ====================
    # TABLE DE FAITS : VENTES
    # ====================
    def etl_fact_ventes(self):
        print("\n💰 ETL Fact_Ventes...")
        
        query = """
        SELECT 
            od.OrderID,
            od.ProductID,
            o.CustomerID,
            o.EmployeeID,
            o.OrderDate,
            o.RequiredDate,
            o.ShippedDate,
            o.ShipVia as ShipperID,
            od.UnitPrice,
            od.Quantity,
            od.Discount,
            o.Freight
        FROM [Order Details] od
        JOIN Orders o ON od.OrderID = o.OrderID
        """
        df = pd.read_sql(query, self.conn_source)
        print(f"  ➤ {len(df)} lignes de vente extraites")
        
        # ========== TRANSFORMATIONS ==========
        
        # 1. Convertir les dates en TempsID (YYYYMMDD)
        df['OrderDate'] = pd.to_datetime(df['OrderDate'])
        df['TempsID'] = (
            df['OrderDate'].dt.year.astype(str) +
            df['OrderDate'].dt.month.astype(str).str.zfill(2) +
            df['OrderDate'].dt.day.astype(str).str.zfill(2)
        ).astype(int)
        
        # 2. Calculer le montant de vente
        df['MontantVente'] = df['Quantity'] * df['UnitPrice'] * (1 - df['Discount'])
        
        # 3. Taxe de transport (10% si >= 500)
        df['TaxeTransport'] = df['Freight'].apply(
            lambda x: x * 0.10 if x >= 500 else 0
        )
        
        # 4. EstLivree (1 si livrée, 0 sinon)
        df['EstLivree'] = df['ShippedDate'].notna().astype(int)
        
        # 5. Délai de livraison (en jours)
        df['DelaiLivraison'] = 0
        mask = df['ShippedDate'].notna() & df['RequiredDate'].notna()
        df.loc[mask, 'DelaiLivraison'] = (
            pd.to_datetime(df.loc[mask, 'ShippedDate']) - 
            pd.to_datetime(df.loc[mask, 'RequiredDate'])
        ).dt.days
        
        # 6. Date de chargement
        df['DateChargement'] = datetime.now()
        df['SourceSystem'] = 'Python_ETL_v1.0'
        
        # ========== CHARGEMENT ==========
        print("  📤 Chargement via pyodbc direct...")
        
        cursor = self.conn_dwh_pyodbc.cursor()
        
        try:
            # Supprimer la table si elle existe
            cursor.execute("IF OBJECT_ID('Fact_Ventes', 'U') IS NOT NULL DROP TABLE Fact_Ventes")
            self.conn_dwh_pyodbc.commit()
            
            # Créer la table
            create_sql = """
            CREATE TABLE Fact_Ventes (
                VenteID INT IDENTITY(1,1) PRIMARY KEY,
                CustomerID NVARCHAR(5),
                ProductID INT,
                TempsID INT,
                EmployeeID INT,
                ShipperID INT,
                Quantite SMALLINT,
                PrixUnitaire MONEY,
                Remise FLOAT,
                MontantVente MONEY,
                FraisTransport MONEY,
                TaxeTransport MONEY,
                EstLivree BIT,
                DelaiLivraison INT,
                OrderID INT,
                DateChargement DATETIME,
                SourceSystem NVARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            self.conn_dwh_pyodbc.commit()
            
            # Insérer les données par lots
            insert_sql = """
            INSERT INTO Fact_Ventes 
            (CustomerID, ProductID, TempsID, EmployeeID, ShipperID,
             Quantite, PrixUnitaire, Remise, MontantVente, FraisTransport,
             TaxeTransport, EstLivree, DelaiLivraison, OrderID,
             DateChargement, SourceSystem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            batch_size = 1000
            total_rows = len(df)
            
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    cursor.execute(insert_sql,
                                  str(row['CustomerID']), int(row['ProductID']),
                                  int(row['TempsID']), int(row['EmployeeID']),
                                  int(row['ShipperID']), int(row['Quantity']),
                                  float(row['UnitPrice']), float(row['Discount']),
                                  float(row['MontantVente']), float(row['Freight']),
                                  float(row['TaxeTransport']), int(row['EstLivree']),
                                  int(row['DelaiLivraison']), int(row['OrderID']),
                                  row['DateChargement'], str(row['SourceSystem']))
                
                self.conn_dwh_pyodbc.commit()
                print(f"  ↳ Lot {i//batch_size + 1}/{(total_rows//batch_size)+1} chargé")
            
            print(f"  ✓ {total_rows} ventes insérées")
            
        except Exception as e:
            self.conn_dwh_pyodbc.rollback()
            raise e
        finally:
            cursor.close()
        
        self.stats['rows_loaded']['Fact_Ventes'] = total_rows
        print(f"  ✅ {total_rows} ventes chargées")
    
    # ====================
    # EXÉCUTION COMPLÈTE
    # ====================
    def run_complete_etl(self):
        try:
            print("\n" + "=" * 60)
            print("🔄 DÉMARRAGE DE L'ETL COMPLET")
            print("=" * 60)
            
            # Ordre IMPORTANT : dimensions d'abord !
            self.etl_dim_client()
            self.etl_dim_produit()
            self.etl_dim_employe()
            self.etl_dim_transporteur()
            
            # Puis la table de faits
            self.etl_fact_ventes()
            
            # Statistiques finales
            self.print_statistics()
            
        except Exception as e:
            print(f"\n❌ ERREUR : {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.conn_source.close()
            self.conn_dwh_pyodbc.close()
            print("\n🔌 Connexions fermées")
    
    def print_statistics(self):
        print("\n" + "=" * 60)
        print("📈 STATISTIQUES DE L'ETL")
        print("=" * 60)
        
        total_time = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print(f"⏱️  Temps total : {total_time:.2f} secondes")
        print(f"📊 Tables chargées :")
        
        for table, rows in self.stats['rows_loaded'].items():
            print(f"   • {table} : {rows:,} lignes")
        
        total_rows = sum(self.stats['rows_loaded'].values())
        print(f"\n📦 TOTAL : {total_rows:,} lignes chargées")
        print("=" * 60)
        print("✅ ETL TERMINÉ AVEC SUCCÈS !")
        print("=" * 60)

# ====================
# EXÉCUTION
# ====================
if __name__ == "__main__":
    etl = NorthwindETL()
    etl.run_complete_etl()