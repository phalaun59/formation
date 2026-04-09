import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog,filedialog
import json
import pandas as pd
from datetime import datetime
import webbrowser
import sys
import csv


def resource_path(relative_path):
    """ Gestion des chemins pour PyInstaller et le développement """
    try:
        # PyInstaller crée un dossier temporaire _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)
# --- CONFIGURATION ---
DB_PATH = resource_path("multirequetes.db")
BASE_DIR = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.abspath(".")
REQUETE_DIR = os.path.join(BASE_DIR, "requete")
RESULT_DIR = os.path.join(BASE_DIR, "result")

# Création automatique des dossiers s'ils manquent
for d in [REQUETE_DIR, RESULT_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)
# --- INITIALISATION ---
def setup_environment():
    for folder in ["requete", "result"]:
        if not os.path.exists(folder):
            os.makedirs(folder)

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialisation avec gestion de la colonne 'ordre'"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Sécurité structure initiale
    try:
        cur.execute("PRAGMA table_info(sondes)")
        columns = [column[1] for column in cur.fetchall()]
        if len(columns) > 0 and "nom_sonde" not in columns:
            cur.execute("DROP TABLE sondes")
            conn.commit()
    except:
        pass
    
    cur.execute("""CREATE TABLE IF NOT EXISTS DB_conn (
        id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT, service TEXT, sid TEXT, 
        grappe_id INTEGER, type_db TEXT DEFAULT 'Oracle', port TEXT DEFAULT '1521',
        FOREIGN KEY(grappe_id) REFERENCES grappes(id))""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS schemas (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, nom TEXT, schema TEXT, 
        grappe_id INTEGER, DB_conn_id INTEGER,
        FOREIGN KEY(grappe_id) REFERENCES grappes(id),
        FOREIGN KEY(DB_conn_id) REFERENCES DB_conn(id))""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS sondes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom_sonde TEXT,
        type_sonde TEXT,
        fonction_id INTEGER,
        type_alerte TEXT,
        type_db TEXT,
        serveur_id INTEGER,
        requete TEXT,
        lien_MOP TEXT,
        ordre INTEGER DEFAULT 0)""")

    cur.execute("CREATE TABLE IF NOT EXISTS sonde_fonctions (id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT UNIQUE)")


    cur.execute("""CREATE TABLE IF NOT EXISTS sonde_cibles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sonde_id INTEGER,
        type_cible TEXT, -- 'GRAPPE' ou 'SCHEMA'
        cible_id INTEGER,
        FOREIGN KEY(sonde_id) REFERENCES sondes(id))""")

    cur.execute("CREATE TABLE IF NOT EXISTS sonde_fonctions (id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT UNIQUE)")
    
    conn.commit()
    conn.close()
def ask_credentials(parent):
    """Ouvre une popup pour saisir uniquement le mot de passe"""
    dialog = tk.Toplevel(parent)
    dialog.title("Authentification BDD")
    dialog.geometry("300x130")
    dialog.resizable(False, False)
    dialog.transient(parent)
    dialog.grab_set()

    # Centrage
    x = parent.winfo_x() + (parent.winfo_width() // 2) - 150
    y = parent.winfo_y() + (parent.winfo_height() // 2) - 65
    dialog.geometry(f"+{x}+{y}")

    result = {"pass": None}

    tk.Label(dialog, text="Entrez le mot de passe pour les schémas :", font=('Helvetica', 9, 'bold')).pack(pady=10)
    
    pass_ent = ttk.Entry(dialog, show="*")
    pass_ent.pack(padx=20, fill="x")
    pass_ent.focus_set()

    def on_confirm(event=None):
        result["pass"] = pass_ent.get()
        dialog.destroy()

    btn = ttk.Button(dialog, text="Valider", command=on_confirm)
    btn.pack(pady=10)
    
    # Validation avec la touche Entrée
    dialog.bind('<Return>', on_confirm)
    
    parent.wait_window(dialog)
    return result["pass"]

    def on_confirm():
        result["user"] = user_ent.get()
        result["pass"] = pass_ent.get()
        dialog.destroy()

    ttk.Button(dialog, text="Valider", command=on_confirm).pack(pady=10)
    
    parent.wait_window(dialog) # Attend la fermeture de la popup
    return result["user"], result["pass"]


class ProbeEngine:
    def __init__(self, sqlite_conn_func):
        """
        :param sqlite_conn_func: Fonction qui retourne une connexion à ta base SQLite locale
        """
        self.get_sqlite_conn = sqlite_conn_func
        self.report_path = "rapport_sondes.html"

    def run_all(self, db_filter=None, user=None, pwd=None):
        """Récupère le mapping et lance toutes les sondes configurées"""
        targets = self._get_probe_mapping(db_filter)
        if not targets:
            print("Aucune sonde configurée dans sonde_cible.")
            return

        all_results = []
        for target in targets:
            print(f"Exécution de '{target['libelle']}' sur {target['host']}...")
            result = self._execute_probe(target,user,pwd)
            all_results.append(result)

        self._generate_html_report(all_results)

    def _get_probe_mapping(self, db_filter):
        conn = get_db_connection()
        query = """
            SELECT 
                s.nom_sonde as libelle, 
                s.type_alerte,
                s.requete as requete_sql, 
                o.host, o.port, o.service, o.type_db, 
                s.lien_MOP,
                COALESCE(sch.schema, o.libelle) as schema_name,
                s.ordre,
                f.nom as fonction_nom  
            FROM sondes s
            JOIN sonde_fonctions f ON s.fonction_id = f.id 
            JOIN sonde_cibles sc ON s.id = sc.sonde_id
            JOIN DB_conn o ON (sc.type_cible = 'GRAPPE' AND sc.cible_id = o.id) 
                 OR (sc.type_cible = 'SQLITE' AND sc.cible_id = o.id)
            LEFT JOIN schemas sch ON (sc.type_cible = 'SCHEMA' AND sc.cible_id = sch.id)
        """
        try:
            # On trie d'abord par l'ID de la fonction ou son nom pour grouper
            # puis par l'ordre défini dans l'interface d'organisation
            order_clause = " ORDER BY s.fonction_id, s.ordre ASC"
            
            if db_filter:
                query += " WHERE s.type_db = ?" + order_clause
                df_mapping = pd.read_sql_query(query, conn, params=[db_filter])
            else:
                query += order_clause
                df_mapping = pd.read_sql_query(query, conn)
        finally:
            conn.close()
        return df_mapping.to_dict(orient='records')

    def _execute_probe(self, target, user, pwd):
        """Gère la connexion distante et l'exécution du SQL"""
        data = None
        error = None
        
        try:
            if target['type_db'] == 'Oracle':
                data = self._run_oracle(target, user, pwd)
            elif target['type_db'] == 'SQLite':
                data = self._run_sqlite(target)
            elif target['type_db'] == 'MySQL':
                error = "Driver MySQL non configuré."
            else:
                error = f"SGBD {target['type_db']} non supporté."
        except Exception as e:
            error = str(e)

        #  On transmet fonction_nom au dictionnaire de résultat ---
        return {
            "libelle": target['libelle'],
            "fonction_nom": target.get('fonction_nom', 'AUTRE'),
            "alerte": target.get('type_alerte', 'Mineur'),
            "lien_mop": target.get('lien_MOP', ''),
            "host": target['host'],
            "service": target['service'],
            "schema": target['schema_name'],
            "data": data,
            "error": error
        }
    def _run_oracle(self, t, user, pwd):
        """Exécution réelle sur Oracle avec changement de schéma"""
        import oracledb
        
        # 1. Construction du DSN (Data Source Name)
        dsn = f"{t['host']}:{t['port']}/{t['service']}"
        
        # 2. Connexion à la base
       
        conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
        try:
            cursor = conn.cursor()
            
            # 3. On se positionne sur le bon schéma avant de lancer la requête
            if t['schema_name']:
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {t['schema_name']}")
            
            # 4. On exécute la requête SQL de la sonde et on récupère un DataFrame Pandas
            df = pd.read_sql(t['requete_sql'], conn)
            return df
        finally:
            conn.close()
    def _run_sqlite(self, t):
        """Exécution sur une base SQLite locale"""
        # Pour SQLite, le chemin du fichier est stocké dans la colonne 'host'
        db_path = t['host']
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Fichier base de données introuvable : {db_path}")
            
        conn = sqlite3.connect(db_path)
        try:
            # On utilise pandas pour lire directement le résultat en DataFrame
            df = pd.read_sql_query(t['requete_sql'], conn)
            return df
        finally:
            conn.close()
            
    def _generate_html_report(self, results):
        """Génère le rapport HTML avec couleurs dynamiques et liens MOP"""
        from collections import defaultdict
        import os
        import webbrowser
        from datetime import datetime
        
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        # 1. Regroupement par fonction
        groups = defaultdict(list)
        for r in results:
            f_nom = r.get('fonction_nom', 'AUTRE')
            groups[f_nom].append(r)

        # 2. Dictionnaire des couleurs pour les alertes
        color_map = {
            "Critique": "#e74c3c",  # Rouge
            "Majeur": "#e67e22",    # Orange
            "Mineur": "#3498db"     # Bleu
        }

        html = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 30px; background-color: #f4f7f6; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                .date {{ text-align: center; color: #7f8c8d; margin-bottom: 30px; }}
                
                .report-grid {{
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 25px;
                    align-items: start;
                }}

                .function-block {{
                    background: #fff;
                    border: 1px solid #d1d8e0;
                    border-radius: 8px;
                    padding: 15px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                    margin-bottom: 20px;
                }}

                .function-title {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 10px;
                    margin: -15px -15px 15px -15px;
                    border-radius: 8px 8px 0 0;
                    font-size: 1.1em;
                    text-transform: uppercase;
                }}

                .sonde-card {{
                    margin-bottom: 20px;
                    padding-bottom: 10px;
                    border-bottom: 1px dashed #ccc;
                }}
                .sonde-card:last-child {{ border-bottom: none; }}

                .sonde-header {{ 
                    font-weight: bold; 
                    font-size: 1.1em;
                    margin-bottom: 5px;
                    display: flex;
                    align-items: center;
                }}

                .mop-link {{
                    text-decoration: none;
                    background-color: #f8f9fa;
                    border: 1px solid #d1d8e0;
                    color: #2c3e50;
                    font-size: 0.7em;
                    padding: 2px 8px;
                    border-radius: 4px;
                    margin-left: 10px;
                    transition: background 0.2s;
                }}
                .mop-link:hover {{ background-color: #e9ecef; }}

                .meta {{ font-size: 0.75em; color: #95a5a6; margin-bottom: 8px; }}
                
                table {{ border-collapse: collapse; width: 100%; font-size: 0.85em; margin-top: 10px; }}
                th {{ background-color: #f8f9fa; color: #333; padding: 6px; text-align: left; border-bottom: 2px solid #ddd; }}
                td {{ padding: 5px; border-bottom: 1px solid #eee; }}
                
                .error {{ color: #e74c3c; font-size: 0.85em; padding: 5px; background: #fdf2f2; border-radius: 4px; }}
                
                @media (max-width: 1100px) {{ .report-grid {{ grid-template-columns: 1fr; }} }}
            </style>
        </head>
        <body>
            <h1>📊 Rapport d'Exploitation</h1>
            <div class="date">Généré le {now}</div>
            
            <div class="report-grid">
        """

        for f_nom, s_list in groups.items():
            html += f"""
            <div class="function-block">
                <div class="function-title">{f_nom}</div>
            """
            
            for r in s_list:
                # 3. Normalisation de l'alerte pour la couleur (sécurité str)
                alerte_raw = str(r.get('alerte', 'Mineur')).strip().capitalize()
                titre_couleur = color_map.get(alerte_raw, "#2c3e50")

                # 4. Gestion sécurisée du lien MOP (Correction de l'erreur Float)
                mop_url = r.get('lien_mop')
                html_mop = ""
                
                if mop_url and str(mop_url).strip() != "" and str(mop_url).lower() != "nan":
                    mop_str = str(mop_url).strip()
                    
                    if not mop_str.lower().startswith('http'):
                        # Formatage du chemin Windows pour le navigateur
                        target_link = "file:///" + mop_str.replace("\\", "/")
                    else:
                        target_link = mop_str
                    
                    html_mop = f'<a href="{target_link}" target="_blank" class="mop-link" title="Ouvrir le Mode Opératoire">📖 MOP</a>'

                html += f"""
                <div class="sonde-card">
                    <div class="sonde-header" style="color: {titre_couleur};">
                        ● {r['libelle']} {html_mop}
                    </div>
                    <div class="meta">📍 {r['host']} | {r['schema']} <span style="float:right;">[{alerte_raw}]</span></div>
                """
                
                if r.get('error'):
                    html += f"<div class='error'>❌ {r['error']}</div>"
                elif r.get('data') is not None and not r['data'].empty:
                    html += r['data'].to_html(index=False, border=0, classes='sonde-table')
                else:
                    html += "<p style='font-size:0.8em; color:gray; font-style: italic;'>Aucune donnée retournée par la requête.</p>"
                
                html += "</div>"
            
            html += "</div>"

        html += "</div></body></html>"

        # 5. Écriture et ouverture du fichier
        try:
            with open(self.report_path, "w", encoding="utf-8") as f:
                f.write(html)
            webbrowser.open("file://" + os.path.realpath(self.report_path))
        except Exception as e:
            print(f"Erreur lors de la génération du rapport : {e}")

class TypeDbSelector(tk.Toplevel):
    def __init__(self, master, current_val):
        super().__init__(master)
        self.master = master
        self.title("Sélection SGBD")
        self.result = None
        
        # --- REMPLACEMENT DE self.eval POUR ÉVITER L'ERREUR ---
        w, h = 350, 180
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        # -----------------------------------------------------
        
        self.grab_set() 
        self.resizable(False, False)

        container = ttk.Frame(self, padding=20)
        container.pack(fill="both", expand=True)

        ttk.Label(container, text="Quel type de BDD organiser ?", font=("Arial", 10)).pack(pady=(0, 10))

        # On définit les valeurs explicitement ici
        self.cb = ttk.Combobox(container, values=("Oracle", "MySQL", "PostgreSQL",'SQLite'), state="readonly")
        
        # On essaie de mettre la valeur actuelle, sinon la première par défaut
        if current_val in ("Oracle", "MySQL", "PostgreSQL","SQLite"):
            self.cb.set(current_val)
        else:
            self.cb.current(0)
            
        self.cb.pack(pady=10, fill="x")
        
        ttk.Button(container, text="Valider", command=self._on_validate).pack(pady=10, fill="x")

    def _on_validate(self):
        self.result = self.cb.get()
        if self.result:
            self.destroy()

    def _on_validate(self):
        self.result = self.cb.get()
        self.destroy()
               
# --- FENÊTRE : selection grappe SCHEMA pour les sondes---#
class CiblePickerWindow(tk.Toplevel):
    def __init__(self, master, current_targets, db_type):
        super().__init__(master)
        self.title(f"Sélection des cibles - {db_type}")
        
        # Configuration de la fenêtre
        w, h = 700, 600 
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.grab_set() # Rend la fenêtre modale
        
        self.db_type = db_type
        self.result = current_targets 
        
        # Dictionnaires pour stocker les variables de contrôle (Checkbuttons)
        self.vars_g = {}      # Pour les Grappes (Oracle)
        self.vars_m = {}      # Pour les Schémas (Oracle)
        self.vars_sqlite = {} # Pour les fichiers SQLite

        self._build_ui()

    def _build_ui(self):
        container = ttk.Frame(self, padding=15)
        container.pack(fill="both", expand=True)
        
        conn = get_db_connection()
        db_type_upper = self.db_type.upper()

        if db_type_upper == "SQLITE":
            # --- VUE SQLITE : Liste simple des bases paramétrées ---
            ttk.Label(container, text="📂 Sélection des fichiers SQLite :", 
                      font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 10))

            scroll_frame = ttk.Frame(container)
            scroll_frame.pack(fill="both", expand=True)

            canvas = tk.Canvas(scroll_frame)
            scrollbar = ttk.Scrollbar(scroll_frame, orient="vertical", command=canvas.yview)
            self.scrollable_inner = ttk.Frame(canvas)

            self.scrollable_inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
            canvas.create_window((0, 0), window=self.scrollable_inner, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)

            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")

            # On récupère les bases SQLite (on affiche le host qui contient le chemin)
            res = conn.execute("SELECT id, host FROM DB_conn WHERE UPPER(type_db)='SQLITE'").fetchall()
            
            if not res:
                ttk.Label(self.scrollable_inner, text="Aucune base SQLite trouvée dans le paramétrage.", 
                          foreground="gray").pack(pady=20)
            else:
                for r in res:
                    # On vérifie si l'ID est déjà dans la sélection actuelle
                    is_sel = r['id'] in self.result.get('SQLITE', [])
                    var = tk.BooleanVar(value=is_sel)
                    self.vars_sqlite[r['id']] = var
                    ttk.Checkbutton(self.scrollable_inner, text=r['host'], variable=var).pack(anchor="w", pady=2)

        else:
            # --- VUE ORACLE / AUTRES : Système de colonnes ---
            body = ttk.Frame(container)
            body.pack(fill="both", expand=True)
            body.columnconfigure(0, weight=1)
            body.columnconfigure(1, weight=1)

            # 1. Colonne Grappes
            f_g = ttk.LabelFrame(body, text=" Grappes (Groupes) ", padding=5)
            f_g.grid(row=0, column=0, sticky="nsew", padx=5)
            
            # Correction de la requête avec l'alias 'o' bien défini
            query_g = """
                SELECT o.id, o.libelle FROM DB_conn o WHERE UPPER(o.type_db)=UPPER(?)
            """
            grappes = conn.execute(query_g, (db_type_upper,)).fetchall()
            for g in grappes:
                is_sel = g['id'] in self.result.get('GRAPPE', [])
                var = tk.BooleanVar(value=is_sel)
                self.vars_g[g['id']] = var
                ttk.Checkbutton(f_g, text=g['libelle'], variable=var).pack(anchor="w")

            # 2. Colonne Schémas / Instances
            f_m = ttk.LabelFrame(body, text=" Schémas Individuels ", padding=5)
            f_m.grid(row=0, column=1, sticky="nsew", padx=5)
            
            query_m = """
                SELECT s.id, s.code 
                FROM schemas s
                JOIN DB_conn o ON s.DB_conn_id = o.id
                WHERE UPPER(o.type_db) = ?
            """
            schemas = conn.execute(query_m, (db_type_upper,)).fetchall()
            for s in schemas:
                is_sel = s['id'] in self.result.get('SCHEMA', [])
                var = tk.BooleanVar(value=is_sel)
                self.vars_m[s['id']] = var
                ttk.Checkbutton(f_m, text=s['code'], variable=var).pack(anchor="w")

        conn.close()

        # Pied de page avec bouton Valider
        footer = ttk.Frame(container)
        footer.pack(fill="x", pady=(10, 0))
        ttk.Button(footer, text="✅ VALIDER LA SÉLECTION", width=30, 
                   command=self._on_validate).pack(anchor="center")

    def _on_validate(self):
        """Enregistre les modifications dans le dictionnaire result"""
        if self.db_type.upper() == "SQLITE":
            # On vide et on remplit la liste des IDs SQLite sélectionnés
            self.result['SQLITE'] = [db_id for db_id, var in self.vars_sqlite.items() if var.get()]
        else:
            # On remplit les listes Grappes et Schémas
            self.result['GRAPPE'] = [g_id for g_id, var in self.vars_g.items() if var.get()]
            self.result['SCHEMA'] = [m_id for m_id, var in self.vars_m.items() if var.get()]
        
        self.destroy() # Ferme la fenêtre
        
        
# --- FENÊTRE : ORGANISER --- #
class OrganiserWindow(tk.Toplevel):
    def __init__(self, master, db_type): # <-- Ajoute db_type ici
        super().__init__(master)
        self.master = master
        self.db_type = db_type # <-- On le stocke
        self.title(f"Organisation des Sondes - {self.db_type}")
        
        # 1. Ajustement dynamique à l'écran (Plein écran)
        try:
            self.state('zoomed')
        except:
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()
            self.geometry(f"{int(sw*0.9)}x{int(sh*0.9)}")

        self.configure(bg="#444") # Fond sombre pour faire ressortir la "page"
        self.grab_set()

        self.floating_label = None
        self.drag_widget = None

        # Configuration de la grille principale (Ligne 0 = Contenu, Ligne 1 = Bouton fixe)
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

        # --- ZONE DE CONTENU (SCROLLABLE) ---
        self.main_container = tk.Frame(self, bg="#444")
        self.main_container.grid(row=0, column=0, sticky="nsew")
        self.main_container.grid_rowconfigure(0, weight=1)
        self.main_container.grid_columnconfigure(0, weight=1)

        self.canvas = tk.Canvas(self.main_container, bg="#444", highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.main_container, orient="vertical", command=self.canvas.yview)
        
        self.page_a4 = tk.Frame(self.canvas, bg="white", padx=40, pady=40)
        
        self.page_a4.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas_window = self.canvas.create_window((self.winfo_screenwidth()/2, 0), 
                                                       window=self.page_a4, anchor="n")
        
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.scrollbar.grid(row=0, column=1, sticky="ns")

        # Layout des colonnes internes à la page
        self.page_a4.grid_columnconfigure(0, weight=1)
        self.page_a4.grid_columnconfigure(1, weight=1)

        self.col_left = tk.Frame(self.page_a4, bg="white")
        self.col_left.grid(row=0, column=0, sticky="nsew", padx=15)
        
        self.col_right = tk.Frame(self.page_a4, bg="white")
        self.col_right.grid(row=0, column=1, sticky="nsew", padx=15)

        # --- ZONE BASSE FIXE (BOUTON) ---
        footer = tk.Frame(self, bg="#222", pady=15, bd=1, relief="raised")
        footer.grid(row=1, column=0, sticky="ew")
        
        btn_save = tk.Button(footer, text="💾  ENREGISTRER L'ORGANISATION", 
                             bg="#28a745", fg="white", font=("Arial", 12, "bold"), 
                             padx=40, pady=10, relief="flat", cursor="hand2",
                             command=self._save_order)
        btn_save.pack()

        self.sections = {} 
        self._build_sections()
        
        # Responsive : Ajuste la largeur de la page blanche lors du redimensionnement
        self.bind("<Configure>", self._on_resize)

    def _on_resize(self, event):
        # On centre la "page" et on ajuste sa largeur (max 1200px)
        canvas_width = self.canvas.winfo_width()
        new_width = min(1200, canvas_width * 0.9)
        self.canvas.itemconfig(self.canvas_window, width=new_width)

    def _build_sections(self):
        """Récupère les sondes filtrées et les place dans les colonnes gauche/droite"""
        conn = get_db_connection()
        # On récupère les catégories de sondes
        fonctions = conn.execute("SELECT * FROM sonde_fonctions ORDER BY nom").fetchall()
        
        # --- CORRECTION : Compteur manuel pour l'alternance gauche/droite ---
        display_count = 0
        
        for func in fonctions:
            # Filtre par fonction et par type de BDD
            sondes = conn.execute("""
                SELECT * FROM sondes 
                WHERE fonction_id = ? AND type_db = ? 
                ORDER BY ordre
            """, (func['id'], self.db_type)).fetchall()
            
            # On ne crée le cadre que s'il y a des sondes à l'intérieur
            if sondes:
                # Si display_count est pair (0, 2, 4...) -> Colonne Gauche
                # Si display_count est impair (1, 3, 5...) -> Colonne Droite
                parent_col = self.col_left if display_count % 2 == 0 else self.col_right
                
                # Le LabelFrame est l'enfant de la COLONNE (parent_col)
                frame = tk.LabelFrame(parent_col, text=f"  {func['nom'].upper()}  ", 
                                      bg="white", font=("Arial", 10, "bold"), padx=10, pady=10)
                
                # On utilise pack() pour empiler les groupes dans la colonne choisie
                # fill="x" permet au cadre de prendre toute la largeur de sa colonne
                frame.pack(fill="x", pady=15, padx=5, anchor="n")
                
                frame.fonction_id = func['id']
                self.sections[func['id']] = frame
                
                for s in sondes:
                    self._create_sonde_widget(frame, s['id'], s['nom_sonde'])
                
                # ON INCRÉMENTE SEULEMENT SI LE GROUPE A ÉTÉ AFFICHÉ
                display_count += 1
        
        conn.close()

    def _create_sonde_widget(self, parent, sonde_id, nom_sonde):
        sw = tk.Frame(parent, bg="#f8f9fa", bd=1, relief="solid", cursor="fleur")
        sw.pack(fill="x", pady=3)
        sw.sonde_id = sonde_id
        sw.nom_sonde = nom_sonde
        
        lbl = tk.Label(sw, text=f" ☰   {nom_sonde}", bg="#f8f9fa", anchor="w", pady=8, font=("Segoe UI", 10))
        lbl.pack(fill="x")
        
        lbl.bind("<Button-1>", self._on_start)
        lbl.bind("<B1-Motion>", self._on_drag)
        lbl.bind("<ButtonRelease-1>", self._on_drop)
        return sw

    def _on_start(self, event):
        self.drag_widget = event.widget.master
        self.drag_widget.config(bg="#e9ecef")
        
        # Widget flottant (Fantôme)
        self.floating_label = tk.Toplevel(self)
        self.floating_label.overrideredirect(True)
        self.floating_label.attributes("-alpha", 0.8)
        self.floating_label.attributes("-topmost", True)
        
        f = tk.Frame(self.floating_label, bg="#d1ecf1", bd=2, relief="ridge")
        f.pack()
        tk.Label(f, text=event.widget.cget("text"), bg="#d1ecf1", padx=15, pady=8, font=("Segoe UI", 10, "bold")).pack()
        self.floating_label.geometry(f"+{event.x_root+10}+{event.y_root+10}")

    def _on_drag(self, event):
        if self.floating_label:
            self.floating_label.geometry(f"+{event.x_root+15}+{event.y_root+15}")

    def _on_drop(self, event):
        if self.floating_label:
            self.floating_label.destroy()
            self.floating_label = None

        x, y = event.x_root, event.y_root
        target_section = None
        
        # Détection de la section cible
        for fid, frame in self.sections.items():
            x1, y1 = frame.winfo_rootx(), frame.winfo_rooty()
            x2, y2 = x1 + frame.winfo_width(), y1 + frame.winfo_height()
            if x1 <= x <= x2 and y1 <= y <= y2:
                target_section = frame
                break
        
        if target_section:
            s_id = self.drag_widget.sonde_id
            s_nom = self.drag_widget.nom_sonde
            self.drag_widget.destroy()
            
            # Recréation dans la nouvelle section
            new_w = self._create_sonde_widget(target_section, s_id, s_nom)
            
            # Positionnement précis par rapport aux autres sondes
            y_in_frame = y - target_section.winfo_rooty()
            children = [c for c in target_section.winfo_children() if hasattr(c, 'sonde_id') and c != new_w]
            
            inserted = False
            for child in children:
                if y_in_frame < child.winfo_y() + (child.winfo_height() / 2):
                    new_w.pack_forget()
                    new_w.pack(in_=target_section, fill="x", pady=3, before=child)
                    inserted = True
                    break
            if not inserted:
                new_w.pack(in_=target_section, fill="x", pady=3)
        else:
            if self.drag_widget:
                self.drag_widget.config(bg="#f8f9fa")

    def _save_order(self):
        conn = get_db_connection()
        try:
            for fid, frame in self.sections.items():
                sondes = [c for c in frame.winfo_children() if hasattr(c, 'sonde_id')]
                for idx, w in enumerate(sondes):
                    conn.execute("UPDATE sondes SET ordre=?, fonction_id=? WHERE id=?", 
                                 (idx, fid, w.sonde_id))
            conn.commit()
            messagebox.showinfo("Succès", "Organisation enregistrée avec succès !")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors de la sauvegarde : {e}")
        finally:
            conn.close()

# --- FENÊTRE : PARAMÉTRAGE SONDE 
class SondeWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.title("Paramétrage des Sondes")
        
        # Centrage de la fenêtre
        w, h = 800, 700  # Augmenté légèrement pour le nouveau champ
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.current_sonde_id = None
        
        # Stockage temporaire des cibles choisies
        self.selected_targets = {'GRAPPE': [], 'SCHEMA': [], 'SQLITE': []}

        # Variables
        self.nom_sonde_var = tk.StringVar()
        self.type_sonde_var = tk.StringVar(value="Fonctionnelle")
        self.fonction_var = tk.StringVar()
        self.db_type_var = tk.StringVar(value="Oracle")
        self.alerte_var = tk.StringVar(value="Majeur") 
        self.search_sonde_var = tk.StringVar()
        self.alerte_var = tk.StringVar(value="")
        self.lien_mop_var = tk.StringVar()

        self._build_ui()
        self._load_fonctions()
        self._load_liste_sondes()
        self.grab_set()

    def _on_close(self):
        self.master.deiconify()
        self.destroy()
    def _browse_mop_file(self):
        """Ouvre une boîte de dialogue pour sélectionner un fichier local"""
        from tkinter import filedialog
        import os

        # Ouvre l'explorateur de fichiers
        chemin_fichier = filedialog.askopenfilename(
            title="Sélectionner le Mode Opératoire",
            filetypes=[
                ("Tous les fichiers", "*.*"),
                ("Documents PDF", "*.pdf"),
                ("Images", "*.png;*.jpg;*.jpeg"),
                ("Documents Word", "*.docx")
            ]
        )

        if chemin_fichier:
            # On normalise le chemin pour éviter les problèmes de slashs Windows
            chemin_propre = os.path.normpath(chemin_fichier)
            self.lien_mop_var.set(chemin_propre)
            
            
    def _build_ui(self):
        container = ttk.Frame(self, padding=15)
        container.pack(fill="both", expand=True)

        # Recherche
        f_search = ttk.LabelFrame(container, text="Rechercher une sonde", padding=5)
        f_search.pack(fill="x", pady=(0, 10))
        self.cb_search = ttk.Combobox(f_search, textvariable=self.search_sonde_var, state="readonly")
        self.cb_search.pack(side="left", fill="x", expand=True, padx=5)
        self.cb_search.bind("<<ComboboxSelected>>", lambda e: self._load_selected_sonde())
        ttk.Button(f_search, text="Nouvelle Sonde", command=self._reset_form).pack(side="left", padx=2)

        # Formulaire
        f_form = ttk.LabelFrame(container, text="Détails de la Sonde", padding=10)
        f_form.pack(fill="x")
        grid = ttk.Frame(f_form)
        grid.pack(fill="x")

        # Ligne 0 : Nom
        ttk.Label(grid, text="Nom Sonde * :").grid(row=0, column=0, sticky="w")
        ttk.Entry(grid, textvariable=self.nom_sonde_var).grid(row=0, column=1, sticky="ew", pady=5)

        # Ligne 1 : Type
        ttk.Label(grid, text="Type :").grid(row=1, column=0, sticky="w")
        ttk.Combobox(grid, textvariable=self.type_sonde_var, values=("Fonctionnelle", "Technique"), state="readonly").grid(row=1, column=1, sticky="ew", pady=2)

        # Ligne 2 : Fonction
        ttk.Label(grid, text="Fonction * :").grid(row=2, column=0, sticky="w")
        f_func = ttk.Frame(grid)
        f_func.grid(row=2, column=1, sticky="ew")
        self.cb_fonctions = ttk.Combobox(f_func, textvariable=self.fonction_var, state="readonly")
        self.cb_fonctions.pack(side="left", fill="x", expand=True)
        ttk.Button(f_func, text="+", width=3, command=self._add_fonction_popup).pack(side="left", padx=2)

        # Ligne 3 : Type BDD
        ttk.Label(grid, text="Type BDD :").grid(row=3, column=0, sticky="w")
        cb_db = ttk.Combobox(grid, textvariable=self.db_type_var, values=("Oracle", "MySQL", "PostgreSQL","SQLite"), state="readonly")
        cb_db.grid(row=3, column=1, sticky="ew", pady=2)
        cb_db.bind("<<ComboboxSelected>>", lambda e: self._reset_targets())

        # Ligne 4 : Type Alerte 
        ttk.Label(grid, text="Niveau Alerte :").grid(row=4, column=0, sticky="w")
        ttk.Combobox(grid, textvariable=self.alerte_var, values=("Mineur", "Majeur", "Critique"), state="readonly").grid(row=4, column=1, sticky="ew", pady=2)
        
        # Ligne 5 : Lien MOP (Saisie URL + Bouton Parcourir)
        ttk.Label(grid, text="Lien MOP / URL :").grid(row=5, column=0, sticky="w")
        
        f_mop = ttk.Frame(grid)
        f_mop.grid(row=5, column=1, sticky="ew", pady=2)
        
        ttk.Entry(f_mop, textvariable=self.lien_mop_var).pack(side="left", fill="x", expand=True)
        ttk.Button(f_mop, text="...", width=3, command=self._browse_mop_file).pack(side="left", padx=2)
        
        
        # Ligne 6 : Cibles - ON PASSE SUR LA LIGNE 6
        ttk.Label(grid, text="Cibles :").grid(row=6, column=0, sticky="w")
        self.btn_cible = ttk.Button(grid, text="🎯 Définir les cibles (0 sélectionnées)", 
                                   command=self._open_cible_picker)
        self.btn_cible.grid(row=6, column=1, sticky="ew", pady=5)

        grid.columnconfigure(1, weight=1)

        ttk.Label(container, text="Requête SQL * :").pack(anchor="w", pady=(10, 0))
        self.txt_req = tk.Text(container, height=10)
        self.txt_req.pack(fill="x", pady=5)

        f_btns = ttk.Frame(container)
        f_btns.pack(fill="x", pady=10)
        ttk.Button(f_btns, text="ENREGISTRER", command=self._save_sonde).pack(side="left", fill="x", expand=True, padx=2)
        ttk.Button(f_btns, text="ORGANISER", command=self._prepare_organiser).pack(side="left", fill="x", expand=True)
        tk.Button(f_btns, text="SUPPRIMER", bg="#ffcccc", fg="red", command=self._delete_sonde_active).pack(side="left", fill="x", expand=True, padx=2)

    def _load_selected_sonde(self):
        sel = self.search_sonde_var.get()
        if not sel: return
        sid = sel.split(" | ")[0]
        conn = get_db_connection()
        
        s = conn.execute("SELECT * FROM sondes WHERE id=?", (sid,)).fetchone()
        if s:
            self.current_sonde_id = s['id']
            self.nom_sonde_var.set(s['nom_sonde'])
            self.db_type_var.set(s['type_db'])
            self.type_sonde_var.set(s['type_sonde'] if s['type_sonde'] else "Fonctionnelle")
            self.alerte_var.set(s['type_alerte'] if s['type_alerte'] else "")
            self.lien_mop_var.set(s['lien_MOP'] if s['lien_MOP'] else "")

            # Chargement de la fonction
            f = conn.execute("SELECT nom FROM sonde_fonctions WHERE id=?", (s['fonction_id'],)).fetchone()
            if f: self.fonction_var.set(f['nom'])
            
            # Chargement de la requête
            self.txt_req.delete("1.0", "end")
            self.txt_req.insert("1.0", s['requete'])

            self.selected_targets = {'GRAPPE': [], 'SCHEMA': [], 'SQLITE': []}
            cibles = conn.execute("SELECT type_cible, cible_id FROM sonde_cibles WHERE sonde_id=?", (sid,)).fetchall()
            for c in cibles:
                t_type = c['type_cible'].upper()
                if t_type in self.selected_targets:
                    self.selected_targets[t_type].append(c['cible_id'])
            
            self._update_target_button_text()
        conn.close()

    def _save_sonde(self):
        nom = self.nom_sonde_var.get().strip()
        f_nom = self.fonction_var.get()
        req = self.txt_req.get("1.0", "end-1c").strip()
        db_type = self.db_type_var.get()
        t_sonde = self.type_sonde_var.get()
        alerte = self.alerte_var.get()
        mop = self.lien_mop_var.get().strip()

        if not all([nom, f_nom, req,alerte]): 
            return messagebox.showwarning("Erreur", "Veuillez remplir le nom, la fonction ,la requête et le niveau d 'alerte.")
        
        conn = get_db_connection()
        try:
            f_res = conn.execute("SELECT id FROM sonde_fonctions WHERE nom=?", (f_nom,)).fetchone()
            if not f_res: return messagebox.showerror("Erreur", "Fonction inconnue.")
            f_id = f_res['id']

            if self.current_sonde_id:
                # Ajout de type_alerte et type_sonde dans l'UPDATE
                conn.execute("""UPDATE sondes SET nom_sonde=?, fonction_id=?, requete=?, type_db=?, type_sonde=?, type_alerte=?, lien_MOP=?
                             WHERE id=?""", (nom, f_id, req, db_type, t_sonde, alerte, mop ,self.current_sonde_id))
                sid = self.current_sonde_id
                conn.execute("DELETE FROM sonde_cibles WHERE sonde_id=?", (sid,))
            else:
                # Ajout de type_alerte et type_sonde dans l'INSERT
                cur = conn.cursor()
                cur.execute("""INSERT INTO sondes (nom_sonde, fonction_id, requete, type_db, type_sonde, type_alerte, lien_MOP=?) 
                            VALUES (?,?,?,?,?,?)""", (nom, f_id, req, db_type, t_sonde, alerte, mop))
                sid = cur.lastrowid

            # Enregistrement des cibles
            for t_type, ids in self.selected_targets.items():
                for cid in ids:
                    conn.execute("INSERT INTO sonde_cibles (sonde_id, type_cible, cible_id) VALUES (?, ?, ?)", 
                                (sid, t_type, cid))

            conn.commit()
            messagebox.showinfo("Succès", "Sonde enregistrée avec succès.")
            self._reset_form()
            self._load_liste_sondes()
        except Exception as e:
            conn.rollback()
            messagebox.showerror("Erreur", str(e))
        finally:
            conn.close()

    # --- Méthodes de support inchangées ---
    def _open_cible_picker(self):
        picker = CiblePickerWindow(self, self.selected_targets, self.db_type_var.get())
        self.wait_window(picker) 
        self.selected_targets = picker.result
        self._update_target_button_text()

    def _update_target_button_text(self):
        count = sum(len(v) for v in self.selected_targets.values())
        self.btn_cible.config(text=f"🎯 Définir les cibles ({count} sélectionnées)")

    def _reset_targets(self):
        self.selected_targets = {'GRAPPE': [], 'SCHEMA': [], 'SQLITE': []}
        self._update_target_button_text()

    def _reset_form(self):
        self.current_sonde_id = None
        self.nom_sonde_var.set("")
        self.fonction_var.set("")
        self.search_sonde_var.set("")
        self.alerte_var.set("Majeur")
        self.cb_search.set("")
        self.txt_req.delete("1.0", "end")
        self._reset_targets()
        self.alerte_var.set("")

    def _load_liste_sondes(self):
        conn = get_db_connection()
        res = conn.execute("SELECT id, nom_sonde FROM sondes ORDER BY id DESC").fetchall()
        self.cb_search["values"] = [f"{r['id']} | {r['nom_sonde']}" for r in res]
        conn.close()

    def _load_fonctions(self):
        conn = get_db_connection()
        res = conn.execute("SELECT nom FROM sonde_fonctions ORDER BY nom").fetchall()
        self.cb_fonctions["values"] = [r["nom"] for r in res]
        conn.close()

    def _add_fonction_popup(self):
        n = simpledialog.askstring("Fonction", "Nom de la nouvelle fonction :")
        if n:
            conn = get_db_connection()
            conn.execute("INSERT OR IGNORE INTO sonde_fonctions (nom) VALUES (?)", (n.strip(),))
            conn.commit()
            conn.close()
            self._load_fonctions()
            self.fonction_var.set(n.strip())

    def _prepare_organiser(self):
        selector = TypeDbSelector(self, self.db_type_var.get())
        self.wait_window(selector)
        if selector.result:
            OrganiserWindow(self, selector.result)

    def _delete_sonde_active(self):
        if self.current_sonde_id and messagebox.askyesno("Confirmation", "Supprimer cette sonde ?"):
            conn = get_db_connection()
            conn.execute("DELETE FROM sonde_cibles WHERE sonde_id=?", (self.current_sonde_id,))
            conn.execute("DELETE FROM sondes WHERE id=?", (self.current_sonde_id,))
            conn.commit()
            conn.close()
            self._reset_form()
            self._load_liste_sondes()

# --- FENÊTRE : PARAMÉTRAGE BDD ---#
class ParamWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.title("Paramétrage BDD")
        
        # Centrage de la fenêtre
        w, h = 1200, 800
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Variables
        self.db_type_var = tk.StringVar(value="Oracle")
        self.host_var = tk.StringVar()
        self.port_var = tk.StringVar(value="1521")
        self.service_var = tk.StringVar()
        self.libelle_var = tk.StringVar()
        
        self.schema_code_var = tk.StringVar()
        self.nom_schema_var = tk.StringVar()
        self.Schema_oracle_var = tk.StringVar() # Utilisé par la Combobox cb_o
        
        self._current_oracle_id = None
        self._current_SCHEMA_id = None

        self._build_ui()
        self._update_ui_visibility()

    def _on_close(self):
        self.master.deiconify()
        if hasattr(self.master, '_load_data'):
            self.master._load_data()
        self.destroy()

    def _update_ui_visibility(self, event=None):
        db_type = self.db_type_var.get()
        
        if db_type == "SQLite":
            self.f_mag.pack_forget()
            self.f_net.configure(text=" 2. Configuration des Fichiers SQLite ")
            self.lbl_host.configure(text="Chemin du fichier (.db) :")
            self.btn_browse.pack(side="right", padx=2)
            self.lbl_port.pack_forget(); self.ent_port.pack_forget()
            self.lbl_service.pack_forget(); self.ent_service.pack_forget()
            self.tree_o.heading("h", text="Chemin Complet")
        else:
            self.f_mag.pack(fill="both", expand=True, pady=5)
            self.f_net.configure(text=" 2. Configuration des Serveurs ")
            self.lbl_host.configure(text="Hôte :")
            self.btn_browse.pack_forget()
            self.lbl_port.pack(anchor="w"); self.ent_port.pack(fill="x", pady=2)
            self.lbl_service.pack(anchor="w"); self.ent_service.pack(fill="x", pady=2)
            self.tree_o.heading("h", text="Hôte")

            ports = {"Oracle": "1521", "MySQL": "3306", "PostgreSQL": "5432"}
            self.port_var.set(ports.get(db_type, ""))

        self._current_oracle_id = None
        self._load_server()
        if db_type != "SQLite":
            self._load_schema()

    def _browse_db_file(self):
        path = filedialog.askopenfilename(title="Sélectionner une base SQLite", 
                                         filetypes=[("SQLite DB", "*.db;*.sqlite"), ("Tous", "*.*")])
        if path:
            self.host_var.set(path)
            if not self.libelle_var.get():
                self.libelle_var.set(os.path.basename(path))

    def _build_ui(self):
        container = ttk.Frame(self, padding=10)
        container.pack(fill="both", expand=True)

        # 1. SGBD
        f_type = ttk.LabelFrame(container, text=" 1. Type de SGBD ", padding=5)
        f_type.pack(fill="x", pady=5)
        cb = ttk.Combobox(f_type, textvariable=self.db_type_var, values=("Oracle",  "SQLite"), state="readonly")
        cb.pack(side="left", fill="x", expand=True, padx=5)
        cb.bind("<<ComboboxSelected>>", self._update_ui_visibility)

        # 2. SERVEURS
        self.f_net = ttk.LabelFrame(container, text=" 2. Configuration des Serveurs ", padding=5)
        self.f_net.pack(fill="both", expand=True, pady=5)
        
        f_table_o = ttk.Frame(self.f_net)
        f_table_o.pack(side="left", fill="both", expand=True)

        self.tree_o = ttk.Treeview(f_table_o, columns=("h", "p", "s", "g"), show="headings", height=8)
        for c, t in [("h", "Hôte"), ("p", "Port"), ("s", "Service/SID"), ("g", "Libellé")]:
            self.tree_o.heading(c, text=t)
        self.tree_o.column("h", width=150, minwidth=100) # Hôte 
        self.tree_o.column("p", width=20, anchor="center") # Port 
        self.tree_o.column("s", width=120)                # Service
        self.tree_o.column("g", width=200)                # Libellé
        
        scroll_o = ttk.Scrollbar(f_table_o, orient="vertical", command=self.tree_o.yview)
        self.tree_o.configure(yscrollcommand=scroll_o.set)
        self.tree_o.pack(side="left", fill="both", expand=True)
        scroll_o.pack(side="right", fill="y")
        self.tree_o.bind("<<TreeviewSelect>>", self._on_ora_sel)
        
        self.form_n = ttk.Frame(self.f_net, padding=5, width=300) 
        self.form_n.pack(side="right", fill="y")
        self.form_n.pack_propagate(False) 
        
        self.lbl_host = ttk.Label(self.form_n, text="Hôte :")
        self.lbl_host.pack(anchor="w")
        f_h = ttk.Frame(self.form_n)
        f_h.pack(fill="x", pady=2)
        ttk.Entry(f_h, textvariable=self.host_var).pack(side="left", fill="x", expand=True)
        self.btn_browse = ttk.Button(f_h, text="...", width=3, command=self._browse_db_file)
        
        self.lbl_port = ttk.Label(self.form_n, text="Port :")
        self.ent_port = ttk.Entry(self.form_n, textvariable=self.port_var)
        self.lbl_service = ttk.Label(self.form_n, text="Service / SID :")
        self.ent_service = ttk.Entry(self.form_n, textvariable=self.service_var)
        
        ttk.Label(self.form_n, text="Libellé :").pack(anchor="w")
        ttk.Entry(self.form_n, textvariable=self.libelle_var).pack(fill="x", pady=2)

        btn_grid = ttk.Frame(self.form_n, padding=5)
        btn_grid.pack(fill="x", pady=5)
        ttk.Button(btn_grid, text="➕ Nouveau", command=self._add_connection).grid(row=0, column=0, sticky="ew", padx=2)
        ttk.Button(btn_grid, text="💾 Sauver", command=self._save_o).grid(row=0, column=1, sticky="ew", padx=2)
        ttk.Button(btn_grid, text="🗑️ Supprimer", command=self._delete_connection).grid(row=1, column=0, columnspan=2, sticky="ew", pady=5)
        ttk.Button(btn_grid, text="📥 Importer JSON", command=self._import_json).grid(row=2, column=0, columnspan=2, sticky="ew", pady=2)
        # 3. SCHÉMAS
        self.f_mag = ttk.LabelFrame(container, text=" 3. Configuration des Schémas ", padding=5)
        self.f_mag.pack(fill="both", expand=True, pady=5)
        
        f_table_m = ttk.Frame(self.f_mag)
        f_table_m.pack(side="left", fill="both", expand=True)

        self.tree_m = ttk.Treeview(f_table_m, columns=("c", "s", "g"), show="headings", height=8)
        for c, t in [("c", "Code"), ("s", "Schéma SQL"), ("g", "Serveur lié")]:
            self.tree_m.heading(c, text=t)
        self.tree_m.column("c", width=50)  # Code
        self.tree_m.column("s", width=50)  # Schéma SQL
        self.tree_m.column("g", width=300)  # Serveur lié
        
        scroll_m = ttk.Scrollbar(f_table_m, orient="vertical", command=self.tree_m.yview)
        self.tree_m.configure(yscrollcommand=scroll_m.set)
        self.tree_m.pack(side="left", fill="both", expand=True)
        scroll_m.pack(side="right", fill="y")
        self.tree_m.bind("<<TreeviewSelect>>", self._on_schema_sel)

        form_m = ttk.Frame(self.f_mag, padding=5, width=300)
        form_m.pack(side="right", fill="y")
        form_m.pack_propagate(False)
        
        ttk.Label(form_m, text="Code (Sonde) :").pack(anchor="w")
        ttk.Entry(form_m, textvariable=self.schema_code_var).pack(fill="x", pady=2)
        ttk.Label(form_m, text="Schéma SQL :").pack(anchor="w")
        ttk.Entry(form_m, textvariable=self.nom_schema_var).pack(fill="x", pady=2)
        ttk.Label(form_m, text="Serveur lié :").pack(anchor="w")
        self.cb_o = ttk.Combobox(form_m, textvariable=self.Schema_oracle_var, state="readonly")
        self.cb_o.pack(fill="x", pady=2)
        
        btn_m_grid = ttk.Frame(form_m)
        btn_m_grid.pack(fill="x", pady=10)
        ttk.Button(btn_m_grid, text="💾 Sauver", command=self._save_m).pack(fill="x", pady=2)
        ttk.Button(btn_m_grid, text="✨ Nouveau", command=self._reset_schema_fields).pack(fill="x", pady=2)
        ttk.Button(btn_m_grid, text="🗑️ Supprimer", command=self._delete_m).pack(fill="x", pady=2)

    def _load_server(self):
        conn = get_db_connection()
        t = self.db_type_var.get()
        self.tree_o.delete(*self.tree_o.get_children())
        res = conn.execute("SELECT * FROM DB_conn WHERE type_db=?", (t,)).fetchall()
        for r in res:
            self.tree_o.insert("", "end", iid=r["id"], values=(r["host"], r["port"], r["service"], r["libelle"]))
        self.cb_o["values"] = [f"{r['id']} | {r['libelle']}" for r in res]
        conn.close()

    def _load_schema(self):
        conn = get_db_connection()
        t = self.db_type_var.get()
        self.tree_m.delete(*self.tree_m.get_children())
        query = """SELECT s.id, s.code, s.schema, s.DB_conn_id, o.libelle FROM schemas s 
                   JOIN DB_conn o ON s.DB_conn_id = o.id 
                   WHERE o.type_db=?"""
        res = conn.execute(query, (t,)).fetchall()
        for r in res:
            display_server = f"{r['DB_conn_id']} | {r['libelle']}"
            self.tree_m.insert("", "end", iid=r["id"], values=(r["code"], r["schema"], display_server))
        conn.close()
        self._current_SCHEMA_id = None

    def _save_o(self):
        conn = get_db_connection(); cur = conn.cursor()
        db_type = self.db_type_var.get()
        try:
            if self._current_oracle_id:
                cur.execute("UPDATE DB_conn SET host=?, port=?, service=?, libelle=? WHERE id=?",
                            (self.host_var.get(), self.port_var.get(), self.service_var.get(), self.libelle_var.get(), self._current_oracle_id))
            else:
                cur.execute("INSERT INTO DB_conn (type_db, host, port, service, libelle) VALUES (?,?,?,?,?)", 
                            (db_type, self.host_var.get(), self.port_var.get(), self.service_var.get(), self.libelle_var.get()))
                if db_type == "SQLite":
                    new_id = cur.lastrowid
                    cur.execute("INSERT INTO schemas (code, schema, DB_conn_id) VALUES (?, 'main', ?)",
                                (self.libelle_var.get(), new_id))
            conn.commit()
            messagebox.showinfo("Succès", "Serveur enregistré.")
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
        finally:
            conn.close()
            self._load_server()
            if db_type != "SQLite": self._load_schema()

    def _save_m(self):
        if not self.cb_o.get(): 
            messagebox.showwarning("Attention", "Veuillez sélectionner un serveur.")
            return
        
        oid = self.cb_o.get().split(" | ")[0]
        conn = get_db_connection(); cur = conn.cursor()
        
        try:
            if self._current_SCHEMA_id: 
                cur.execute("UPDATE schemas SET code=?, schema=?, DB_conn_id=? WHERE id=?", 
                            (self.schema_code_var.get(), self.nom_schema_var.get(), oid, self._current_SCHEMA_id))
            else: 
                cur.execute("INSERT INTO schemas (code, schema, DB_conn_id) VALUES (?,?,?)", 
                            (self.schema_code_var.get(), self.nom_schema_var.get(), oid))
            conn.commit()
            messagebox.showinfo("Succès", "Schéma enregistré.")
            self._reset_schema_fields()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
        finally:
            conn.close()
            self._load_schema()

    def _on_ora_sel(self, event):
        sel = self.tree_o.selection()
        if sel:
            self._current_oracle_id = sel[0]
            v = self.tree_o.item(sel[0], 'values')
            self.host_var.set(v[0]); self.port_var.set(v[1])
            self.service_var.set(v[2]); self.libelle_var.set(v[3])

    def _on_schema_sel(self, e):
        s = self.tree_m.selection()
        if s: 
            self._current_SCHEMA_id = s[0] # L'iid est l'ID SQL
            v = self.tree_m.item(s[0], "values")
            self.schema_code_var.set(v[0]); self.nom_schema_var.set(v[1])
            self.cb_o.set(v[2])

    def _reset_schema_fields(self):
        self._current_SCHEMA_id = None
        self.schema_code_var.set("")
        self.nom_schema_var.set("")
        self.cb_o.set("")

    def _add_connection(self):
        self.host_var.set(""); self.service_var.set(""); self.libelle_var.set("")
        self._current_oracle_id = None
        self.port_var.set("1521" if self.db_type_var.get() != "SQLite" else "")

    def _delete_m(self):
        if not self._current_SCHEMA_id: return
        if messagebox.askyesno("Confirm", "Supprimer ce schéma ?"):
            conn = get_db_connection()
            conn.execute("DELETE FROM schemas WHERE id=?", (self._current_SCHEMA_id,))
            conn.commit(); conn.close()
            self._reset_schema_fields()
            self._load_schema()

    def _delete_connection(self):
        sel_o = self.tree_o.selection()
        if sel_o and messagebox.askyesno("Confirm", "Supprimer ce serveur et ses schémas ?"):
            conn = get_db_connection()
            for i in sel_o:
                conn.execute("DELETE FROM schemas WHERE DB_conn_id=?", (i,))
                conn.execute("DELETE FROM DB_conn WHERE id=?", (i,))
            conn.commit(); conn.close()
            self._load_server(); self._load_schema()
            
    def _import_json(self):
        import json
        from tkinter import filedialog, messagebox
        
        file_path = filedialog.askopenfilename(
            title="Importer des connexions (JSON)",
            filetypes=[("Fichiers JSON", "*.json")]
        )
        
        if not file_path:
            return

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            connections = data.get('connections', [])
            conn = get_db_connection()
            cur = conn.cursor()
            
            s_count = 0 
            m_count = 0 
            
            for item in connections:
                info = item.get('info', {})
                
                # --- RECTIFICATION : On récupère le NAV_FOLDER pour le libellé ---
                # On nettoie les "+" qui remplacent les espaces dans le JSON SQL Developer
                nav_folder = info.get('NAV_FOLDER')
                if nav_folder:
                    name_conn = nav_folder.replace('+', ' ')
                else:
                    name_conn = item.get('name', 'Imported_Conn')
                
                host = info.get('hostname')
                port = info.get('port', '1521')
                service = info.get('serviceName')
                user_schema = info.get('user')
                
                if host and service:
                    # 1. On cherche si le serveur (Hôte + Service) existe déjà
                    cur.execute("""SELECT id FROM DB_conn 
                                   WHERE host = ? AND service = ? AND type_db = 'Oracle'""", 
                                (host, service))
                    row = cur.fetchone()
                    
                    if row:
                        db_id = row['id']
                    else:
                        # Nouveau serveur avec le nom de la Grappe (NAV_FOLDER)
                        cur.execute("""INSERT INTO DB_conn (type_db, host, port, service, libelle) 
                                       VALUES (?, ?, ?, ?, ?)""",
                                    ("Oracle", host, port, service, name_conn))
                        db_id = cur.lastrowid
                        s_count += 1
                    
                    # 2. Insertion du schéma
                    if user_schema:
                        # On vérifie si ce schéma existe déjà pour ce serveur
                        cur.execute("SELECT id FROM schemas WHERE code = ? AND DB_conn_id = ?", 
                                   (user_schema, db_id))
                        if not cur.fetchone():
                            cur.execute("""INSERT INTO schemas (code, schema, DB_conn_id) 
                                           VALUES (?, ?, ?)""",
                                        (user_schema, user_schema, db_id))
                            m_count += 1
            
            conn.commit()
            conn.close()
            
            self._load_server()
            self._load_schema()
            
            messagebox.showinfo("Import terminé", 
                                f"Analyse terminée :\n- {s_count} nouvelles grappes créées\n- {m_count} nouveaux schémas ajoutés")
            
        except Exception as e:
            messagebox.showerror("Erreur Import", f"Erreur lors de l'import : {e}")
            
# --- APPLICATION PRINCIPALE ---
class MultiRequetesApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Multirequete")
        w, h = 900, 700
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        setup_environment()
        init_db()
        
        self.type_v = tk.StringVar(value="Oracle")
        self.file_v = tk.StringVar() 
        
        self._build_ui()
        self._on_type_change()

    def _build_ui(self):
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=3)
        self.grid_rowconfigure(2, weight=1)

        top = ttk.Frame(self, padding=10)
        top.grid(row=0, column=0, sticky="ew")
        
        ttk.Label(top, text="SGBD :").pack(side="left")
        # Correction : On utilise "SQLite" (un seul L) pour matcher avec le reste du code
        cb = ttk.Combobox(top, textvariable=self.type_v, values=("Oracle",  "SQLite"), state="readonly")
        cb.pack(side="left", padx=5)
        cb.bind("<<ComboboxSelected>>", self._on_type_change)

        ttk.Label(top, text="Fichier SQL :").pack(side="left", padx=(15, 0))
        self.cb_files = ttk.Combobox(top, textvariable=self.file_v, state="readonly", width=35)
        self.cb_files.bind("<Button-1>", self._load_files)
        self.cb_files.pack(side="left", padx=5)
        
        
        ttk.Button(top, text="Paramétrage BDD", command=self._open_bdd).pack(side="right", padx=2)
        ttk.Button(top, text="Paramétrage Sondes", command=self._open_sondes).pack(side="right", padx=2)

        self.body_container = ttk.Frame(self, padding=10)
        self.body_container.grid(row=1, column=0, sticky="nsew")
        self.body_container.grid_columnconfigure(0, weight=1)
        self.body_container.grid_rowconfigure(0, weight=1)

        # Vue Standard (Oracle/etc)
        self.f_oracle_view = ttk.Frame(self.body_container)
        self.f_oracle_view.grid_columnconfigure(0, weight=1)
        self.f_oracle_view.grid_columnconfigure(1, weight=1)
        self.f_oracle_view.grid_rowconfigure(0, weight=1)

        m_col = ttk.LabelFrame(self.f_oracle_view, text=" Schémas / Serveurs ", padding=5)
        m_col.grid(row=0, column=0, sticky="nsew", padx=5)
        self.l_m = tk.Listbox(m_col, font=("Segoe UI", 10), selectmode="multiple", exportselection=0)
        self.l_m.pack(fill="both", expand=True)
        
        g_col = ttk.LabelFrame(self.f_oracle_view, text=" Grappes ", padding=5)
        g_col.grid(row=0, column=1, sticky="nsew", padx=5)
        self.l_g = tk.Listbox(g_col, font=("Segoe UI", 10), selectmode="multiple", exportselection=0)
        self.l_g.pack(fill="both", expand=True)

        # Vue SQLite
        self.f_sqlite_view = ttk.LabelFrame(self.body_container, text=" Fichiers SQLite disponibles ", padding=5)
        self.l_sqlite = tk.Listbox(self.f_sqlite_view, font=("Segoe UI", 10), selectmode="multiple", exportselection=0)
        self.l_sqlite.pack(fill="both", expand=True)

        log_frame = ttk.LabelFrame(self, text=" Console d'exécution ", padding=5)
        log_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
        self.log_text = tk.Text(log_frame, height=8, bg="#d1f7ff", fg="black", font=("Consolas", 10))
        self.log_text.pack(side="left", fill="both", expand=True)
        scroll_log = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll_log.pack(side="right", fill="y")
        self.log_text.config(yscrollcommand=scroll_log.set)

        bottom = ttk.Frame(self, padding=10)
        bottom.grid(row=3, column=0, sticky="ew")
        btn_center = ttk.Frame(bottom)
        btn_center.pack(anchor="center")
        ttk.Button(btn_center, text="🚀 Lancer les requêtes", width=25, command=self._run_queries).pack(side="left", padx=5)
        ttk.Button(btn_center, text="🚀 Lancer les Sondes", width=25, command=self._on_launch_probes).pack(side="left", padx=5)

    def _on_type_change(self, event=None):
        db_type = self.type_v.get()
        if db_type == "SQLite":
            self.f_oracle_view.grid_forget()
            self.f_sqlite_view.grid(row=0, column=0, sticky="nsew")
        else:
            self.f_sqlite_view.grid_forget()
            self.f_oracle_view.grid(row=0, column=0, sticky="nsew")
        self._load_data()

    def _load_data(self):
        conn = get_db_connection()
        t = self.type_v.get()
        if t == "SQLite":
            self.l_sqlite.delete(0, "end")
            res = conn.execute("SELECT libelle FROM DB_conn WHERE type_db='SQLite'").fetchall()
            for r in res: self.l_sqlite.insert("end", r['libelle'])
        else:
            self.l_m.delete(0, "end")
            res = conn.execute("SELECT m.code FROM schemas m JOIN DB_conn o ON m.DB_conn_id = o.id WHERE o.type_db=?", (t,)).fetchall()
            for r in res: self.l_m.insert("end", r['code'])
            self.l_g.delete(0, "end")
            res_g = conn.execute("SELECT libelle from DB_conn WHERE type_db=?", (t,)).fetchall()
            for r in res_g: self.l_g.insert("end", r['libelle'])
        conn.close()
        self._load_files()

    def _run_queries(self):
        sql_file = self.file_v.get()
        db_type = self.type_v.get()
        self.log_text.delete("1.0", "end")
        
        if not sql_file:
            self._log("Erreur : Aucun fichier SQL sélectionné.", "ERROR")
            return

        conn_sqlite = get_db_connection()
        targets = []
        
        if db_type == "SQLite":
            selected = self.l_sqlite.curselection()
            for i in selected:
                name = self.l_sqlite.get(i)
                res = conn_sqlite.execute("SELECT host, libelle FROM DB_conn WHERE libelle=?", (name,)).fetchone()
                if res:
                    targets.append({'host': res['host'], 'schema': res['libelle'], 'port': '', 'service': ''})
        else:
            # --- 1. Gestion des SCHÉMAS individuels ---
            selected_m = self.l_m.curselection()
            for i in selected_m:
                name = self.l_m.get(i)
                res = conn_sqlite.execute("""
                    SELECT s.schema, o.host, o.port, o.service 
                    FROM schemas s 
                    JOIN DB_conn o ON s.DB_conn_id = o.id 
                    WHERE s.code = ?""", (name,)).fetchone()
                if res: 
                    targets.append(dict(res))

            # --- 2. Gestion des GRAPPES / SERVEURS ---
            selected_g = self.l_g.curselection()
            for i in selected_g:
                server_libelle = self.l_g.get(i)
                res_g = conn_sqlite.execute("""
                    SELECT s.schema, o.host, o.port, o.service 
                    FROM schemas s
                    JOIN DB_conn o ON s.DB_conn_id = o.id
                    WHERE o.libelle = ?""", (server_libelle,)).fetchall()
                
                for r in res_g:
                    target_dict = dict(r)
                    if target_dict not in targets:
                        targets.append(target_dict)

        conn_sqlite.close()

        if not targets:
            self._log("Aucune cible sélectionnée (Schéma ou Grappe).", "WARN")
            return

        # --- Authentification : On ne demande que le MOT DE PASSE ---
        pwd = None
        if db_type != "SQLite":
            # On utilise ta nouvelle fonction simplifiée (uniquement password)
            pwd = ask_credentials(self) 
            if not pwd: 
                self._log("Lancement annulé : mot de passe manquant.", "WARN")
                return

        try:
            req_path = os.path.join("requete", sql_file)
            with open(req_path, "r", encoding="utf-8") as f:
                sql_content = f.read().strip()
            
           # On détecte si c'est du PL/SQL
            is_plsql = sql_content.upper().startswith(("BEGIN", "DECLARE"))
            
            # On ne retire le ; final que si ce n'est PAS du PL/SQL
            if not is_plsql and sql_content.endswith(';'):
                sql_content = sql_content[:-1].strip()
                
            # Lancement de l'exécution
            self._execute_on_engine(db_type, targets, sql_content,  pwd)
            
        except Exception as e:
            self._log(f"Erreur lecture fichier : {e}", "ERROR")

    def _execute_on_engine(self, db_type, targets, sql, pwd=None):
        """Exécute SQL/PLSQL avec confirmation, gestion du COMMIT et export des LOGS Oracle"""
        from tkinter import messagebox
        import os
        import csv
        from datetime import datetime

        # 1. ANALYSE DU TYPE DE REQUÊTE
        sql_raw = sql.strip()
        sql_upper = sql_raw.upper()
        
        # Détection si c'est une action sensible
        is_write_op = sql_upper.startswith(("UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "BEGIN", "DECLARE"))

        # 2. DEMANDE DE CONFIRMATION
        if is_write_op:
            msg = f"Attention : La requête semble contenir des instructions de modification ou du PL/SQL.\n\n"
            msg += f"Elle va être exécutée sur {len(targets)} cible(s).\n\n"
            msg += "Voulez-vous vraiment continuer ?"
            
            confirm = messagebox.askyesno("Confirmation d'écriture", msg, icon='warning')
            if not confirm:
                self._log("❌ Opération annulée par l'utilisateur.", "WARN")
                return

        # 3. PRÉPARATION DU DOSSIER RÉSULTAT
        if not os.path.exists("result"):
            os.makedirs("result")

        self._log(f"🚀 Lancement de l'exécution sur {len(targets)} cible(s)...")
        
        for t in targets:
            try:
                schema_name = t.get('schema', 'Base')
                self._log(f"Connexion à {schema_name}...")
                
                if db_type == "Oracle":
                    import oracledb
                    dsn = oracledb.makedsn(t['host'], t['port'], service_name=t['service'])
                    conn = oracledb.connect(user=schema_name, password=pwd, dsn=dsn)
                    cursor = conn.cursor()
                    
                    # --- ACTIVATION DES LOGS ORACLE ---
                    cursor.callproc("dbms_output.enable", (None,))
                    
                    # Détection PL/SQL vs SQL
                    is_plsql = sql_upper.startswith(("BEGIN", "DECLARE"))

                    if is_plsql:
                        # On garde le bloc entier pour le PL/SQL
                        if sql_raw.endswith('/'): sql_raw = sql_raw[:-1].strip()
                        queries = [sql_raw]
                    else:
                        # On découpe pour le SQL classique
                        queries = [q.strip() for q in sql_raw.split(';') if q.strip()]

                    # Exécution des requêtes
                    for query in queries:
                        cursor.execute(query)
                    
                    # --- RÉCUPÉRATION DES LOGS (DBMS_OUTPUT) ---
                    dbms_logs = []
                    status_var = cursor.var(oracledb.NUMBER)
                    line_var = cursor.var(oracledb.STRING)
                    while True:
                        cursor.callproc("dbms_output.get_line", (line_var, status_var))
                        if status_var.getvalue() != 0: break
                        dbms_logs.append(line_var.getvalue())

                    # --- ÉCRITURE DU FICHIER LOG (si logs présents) ---
                    if dbms_logs:
                        timestamp_log = datetime.now().strftime("%Y%m%d_%H%M%S")
                        log_filename = f"result/LOG_{schema_name}_{timestamp_log}.log"
                        with open(log_filename, 'w', encoding='utf-8') as f_log:
                            f_log.write("\n".join(dbms_logs))
                        self._log(f"📄 Logs Oracle extraits dans {log_filename}", "OK")

                    # --- TRAITEMENT DES RÉSULTATS (CSV) ---
                    if cursor.description:
                        colnames = [d[0] for d in cursor.description]
                        rows = cursor.fetchall()
                        if rows:
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"result/{schema_name}_{timestamp}.csv"
                            with open(filename, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.writer(f, delimiter=';')
                                writer.writerow(colnames)
                                writer.writerows(rows)
                            self._log(f"SUCCÈS : {len(rows)} lignes dans {filename}", "OK")
                        else:
                            self._log(f"INFO : Aucun résultat pour {schema_name}.", "INFO")
                    else:
                        # Commit pour les opérations sans retour de lignes
                        conn.commit()
                        self._log(f"✅ Succès : Commande validée (COMMIT) sur {schema_name}.", "OK")
                
                    conn.close()

                elif db_type == "SQLite":
                    import sqlite3
                    conn = sqlite3.connect(t['host'])
                    cursor = conn.cursor()
                    cursor.execute(sql)
                    conn.commit()
                    conn.close()

            except Exception as e:
                self._log(f"ÉCHEC sur {t.get('schema', 'Base')}: {e}", "ERROR")

        self._log("🏁 Toutes les exécutions sont terminées.")

    def _on_launch_probes(self):
        """Lance le moteur de sondes"""
        db_type = self.type_v.get()
        user, pwd = (None, None)
        if db_type != "SQLite":
            user, pwd = ask_credentials(self)
            if not user or not pwd: return

        try:

            engine = ProbeEngine(get_db_connection)
            engine.run_all(db_filter=db_type, user=user, pwd=pwd)
        except Exception as e:
            messagebox.showerror("Erreur Moteur", f"Impossible de lancer les sondes : {e}")

    def _log(self, message, level="INFO"):
        self.log_text.insert("end", f"[{level}] {message}\n")
        self.log_text.see("end")
        self.update_idletasks()

    def _open_bdd(self): self.withdraw(); ParamWindow(self)
    def _open_sondes(self): self.withdraw(); SondeWindow(self)
    
    def _load_files(self, event=None): # Ajout de event=None
        if os.path.exists("requete"):
            files = [f for f in os.listdir("requete") if f.endswith(".sql")]
            self.cb_files["values"] = sorted(files)
            # On ne change la sélection que si rien n'est sélectionné
            if files and not self.file_v.get(): 
                self.cb_files.current(0)

if __name__ == "__main__":
    app = MultiRequetesApp(); app.mainloop()