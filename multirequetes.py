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
import cryptography
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import oracledb


def resource_path(relative_path):
    """ Gestion des chemins pour PyInstaller et le développement """
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)
# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.abspath(".")
DB_PATH = os.path.join(BASE_DIR, "multirequetes.db")
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
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Table des connexions (C'est ici qu'il te manquait 'libelle')
    cur.execute("""
        CREATE TABLE IF NOT EXISTS DB_conn (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            libelle TEXT NOT NULL,
            host TEXT,
            port TEXT,
            service TEXT,
            type_db TEXT
        )
    """)

    # 2. Table des schémas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schemas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            DB_conn_id INTEGER,
            code TEXT,
            schema TEXT,
            FOREIGN KEY(DB_conn_id) REFERENCES DB_conn(id)
        )
    """)

    # 3. Table des fonctions (catégories de sondes)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sonde_fonctions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT UNIQUE
        )
    """)

    # 4. Table des sondes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sondes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom_sonde TEXT,
            fonction_id INTEGER,
            type_alerte TEXT,
            requete TEXT,
            lien_MOP TEXT,
            type_db TEXT,
            FOREIGN KEY(fonction_id) REFERENCES sonde_fonctions(id)
        )
    """)

    # 5. Table des packs de reporting
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reporting_packs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT UNIQUE
        )
    """)

    # 6. Table de liaison Pack <-> Sonde (avec l'ordre)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reporting_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pack_id INTEGER,
            sonde_id INTEGER,
            ordre INTEGER,
            FOREIGN KEY(pack_id) REFERENCES reporting_packs(id),
            FOREIGN KEY(sonde_id) REFERENCES sondes(id)
        )
    """)

    # 7. Table des cibles de sondes (Lien sonde <-> schéma ou grappe)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sonde_cibles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sonde_id INTEGER,
            type_cible TEXT, -- 'SCHEMA' ou 'GRAPPE'
            cible_id INTEGER,
            FOREIGN KEY(sonde_id) REFERENCES sondes(id)
        )
    """)

    conn.commit()
    conn.close()
    
    
def init_db_compare():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. Définition du comparatif
    cur.execute("""
        CREATE TABLE IF NOT EXISTS comparatifs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            libelle TEXT NOT NULL,
            requete_sql TEXT NOT NULL,
            ref_schema_id INTEGER, -- ID du schéma qui sert de base
            FOREIGN KEY(ref_schema_id) REFERENCES schemas(id)
        )
    """)

    # 2. Cibles du comparatif (sur quels autres schémas on compare)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS comparatif_cibles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            comp_id INTEGER,
            schema_id INTEGER,
            FOREIGN KEY(comp_id) REFERENCES comparatifs(id),
            FOREIGN KEY(schema_id) REFERENCES schemas(id)
        )
    """)
    
    conn.commit()
    conn.close()

def _is_readonly_query( sql_query):
  
        forbidden_keywords = ["UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
    
        # On met tout en majuscules et on nettoie les espaces/retours à la ligne
        clean_query = sql_query.upper().strip()
        
        for word in forbidden_keywords:
            # On cherche le mot avec des espaces autour pour éviter de bloquer 
            # un mot légitime (ex: 'DESCRIPTION' qui contient 'DROP' est ok)
            if word in clean_query:
                # Vérification plus fine pour éviter les faux positifs
                # On vérifie si le mot interdit est un mot entier
                import re
                if re.search(rf'\b{word}\b', clean_query):
                    return False, word
                    
        return True, None
    
def ask_credentials(parent):
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
class ProgressDialog(tk.Toplevel):
    def __init__(self, parent, title="Exécution en cours"):
        super().__init__(parent)
        self.title(title)
        self.geometry("400x150")
        self.resizable(False, False)
        self.grab_set()  # Rend la fenêtre modale (bloque l'accès au parent)
        
        # Centrer la fenêtre
        self.center_window()

        self.lbl_main = ttk.Label(self, text="Démarrage...", font=("Segoe UI", 10, "bold"))
        self.lbl_main.pack(pady=(20, 5))

        self.progress = ttk.Progressbar(self, orient="horizontal", length=300, mode="determinate")
        self.progress.pack(pady=10)

        self.lbl_sub = ttk.Label(self, text="", font=("Segoe UI", 9))
        self.lbl_sub.pack()

    def center_window(self):
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')

    def update(self, main_text, sub_text, value):
        self.lbl_main.config(text=main_text)
        self.lbl_sub.config(text=sub_text)
        self.progress["value"] = value
        self.update_idletasks()

class CompareWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("🔍 Gestion des Comparatifs d'Environnements")
        self.geometry("1100x750")
        self.parent = parent
        self.selected_comp_id = None
        self.target_vars = {}  # Pour stocker les variables des Checkbuttons 🔘
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._setup_ui()
        self._load_ref_schemas()
        self._load_comparatifs()
        self.cb_ref.bind("<<ComboboxSelected>>", self._update_target_list)

    def _setup_ui(self):
        """Planifie l'interface avec trois zones : Liste, Édition, et Cibles."""
        # --- PANNEAU GAUCHE : LISTE DES COMPARATIFS ---
        frame_list = ttk.LabelFrame(self, text="Comparatifs enregistrés")
        frame_list.pack(side="left", fill="y", padx=10, pady=10)

        self.list_comp = tk.Listbox(frame_list, width=35, font=("Segoe UI", 10))
        self.list_comp.pack(fill="both", expand=True, padx=5, pady=5)
        self.list_comp.bind("<<ListboxSelect>>", self._on_select_comp)

        # --- PANNEAU CENTRAL : FORMULAIRE D'ÉDITION ---
        frame_edit = ttk.LabelFrame(self, text="Configuration du comparatif")
        frame_edit.pack(side="left", fill="both", expand=True, padx=10, pady=10)

        ttk.Label(frame_edit, text="Nom du comparatif :").pack(anchor="w", padx=5)
        self.ent_libelle = ttk.Entry(frame_edit, font=("Segoe UI", 10))
        self.ent_libelle.pack(fill="x", padx=5, pady=5)

        ttk.Label(frame_edit, text="Schéma de Référence (Pivot) :").pack(anchor="w", padx=5)
        self.cb_ref = ttk.Combobox(frame_edit, state="readonly")
        self.cb_ref.pack(fill="x", padx=5, pady=5)

        ttk.Label(frame_edit, text="Requête SQL (Extraction uniquement) :").pack(anchor="w", padx=5)
        self.txt_sql = tk.Text(frame_edit, height=12, font=("Consolas", 10))
        self.txt_sql.pack(fill="both", expand=True, padx=5, pady=5)

        # Boutons d'action
        btn_frame = ttk.Frame(frame_edit)
        btn_frame.pack(fill="x", pady=10)
        
        ttk.Button(btn_frame, text="🆕 Nouveau", command=self._clear_form).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="💾 Enregistrer", command=self._save_comp).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="🗑️ Supprimer", command=self._delete_comp).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="🚀 LANCER LA COMPARAISON", command=self._run_comparison).pack(side="right", padx=5)

        # --- PANNEAU DROIT : SÉLECTION DES CIBLES ---
        self.frame_targets = ttk.LabelFrame(self, text="Environnements à comparer")
        self.frame_targets.pack(side="right", fill="y", padx=10, pady=10)
        
        self.canvas_targets = tk.Canvas(self.frame_targets, width=250)
        self.scroll_y = ttk.Scrollbar(self.frame_targets, orient="vertical", command=self.canvas_targets.yview)
        self.scroll_frame = ttk.Frame(self.canvas_targets)

        self.canvas_targets.create_window((0, 0), window=self.scroll_frame, anchor="nw")
        self.canvas_targets.configure(yscrollcommand=self.scroll_y.set)
        
        self.canvas_targets.pack(side="left", fill="both", expand=True)
        self.scroll_y.pack(side="right", fill="y")

    def _on_close(self):
        self.parent.deiconify()
        self.destroy()
    def _update_target_list(self, event=None):

        pivot_display = self.cb_ref.get()
        if not pivot_display:
            return

        ref_id = self.schema_mapping.get(pivot_display)

        # On parcourt tous les widgets créés précédemment
        for s_id, widget in self.target_widgets.items():
            if s_id == ref_id:
                # C'est le pivot : on le décoche et on le cache
                self.target_vars[s_id].set(False)
                widget.pack_forget() 
            else:
                # Ce n'est pas le pivot : on s'assure qu'il est visible
                widget.pack(anchor="w", padx=5)
        
        # Rafraîchir le scroll du canvas
        self.scroll_frame.update_idletasks()
        self.canvas_targets.configure(scrollregion=self.canvas_targets.bbox("all"))
        
    def _load_ref_schemas(self):
        """Récupère les schémas disponibles pour le pivot et les cibles."""
        conn = get_db_connection()
        schemas = conn.execute("""
            SELECT s.id, c.libelle || ' - ' || s.schema 
            FROM schemas s 
            JOIN DB_conn c ON s.DB_conn_id = c.id
        """).fetchall()
        conn.close()

        display_names = [s[1] for s in schemas]
        self.cb_ref['values'] = display_names
        self.schema_mapping = {s[1]: s[0] for s in schemas} # Nom -> ID
        
        # Initialisation des dictionnaires de stockage
        self.target_vars = {}    # Stocke les BooleanVar (pour la valeur True/False)
        self.target_widgets = {} # Stocke les objets Checkbutton (pour l'affichage)

        # Création dynamique des Checkbuttons pour les cibles
        for name in display_names:
            s_id = self.schema_mapping[name]
            var = tk.BooleanVar()
            chk = ttk.Checkbutton(self.scroll_frame, text=name, variable=var)
            chk.pack(anchor="w", padx=5)
            
            # On mémorise la variable ET le widget
            self.target_vars[s_id] = var
            self.target_widgets[s_id] = chk

    def _save_comp(self):
        """Enregistre le comparatif avec vérifications strictes"""
        # 1. Récupération des données de base
        libelle = self.ent_libelle.get().strip()
        sql = self.txt_sql.get("1.0", tk.END).strip()
        pivot_display = self.cb_ref.get()

        # 2. Vérification des champs textuels
        if not libelle:
            messagebox.showwarning("Données manquantes", "Veuillez saisir un nom pour ce comparatif.")
            return
        if not pivot_display:
            messagebox.showwarning("Données manquantes", "Veuillez sélectionner un schéma de référence (Pivot).")
            return
        if not sql or len(sql) < 10:
            messagebox.showwarning("Données manquantes", "La requête SQL semble vide ou trop courte.")
            return

        # 3. Vérification de la sécurité SQL (fonction globale)
        is_safe, forbidden_word = _is_readonly_query(sql)
        if not is_safe:
            messagebox.showerror("Sécurité", f"Action interdite détectée : '{forbidden_word}'\nSeuls les SELECT sont autorisés.")
            return

        # 4. Vérification si au moins une cible est cochée
        # On récupère les IDs des schémas dont la case est à True
        selected_target_ids = [s_id for s_id, var in self.target_vars.items() if var.get()]
        
        if not selected_target_ids:
            messagebox.showwarning("Sélection manquante", "Veuillez cocher au moins un environnement à comparer dans la liste de droite.")
            return
        
        # 5. Enregistrement en base de données
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        try:
            ref_id = self.schema_mapping[pivot_display]

            if self.selected_comp_id:
                # MISE À JOUR
                cur.execute("""
                    UPDATE comparatifs SET libelle=?, requete_sql=?, ref_schema_id=? 
                    WHERE id=?""", (libelle, sql, ref_id, self.selected_comp_id))
                # On vide les anciennes cibles pour réinsérer les nouvelles
                cur.execute("DELETE FROM comparatif_cibles WHERE comp_id=?", (self.selected_comp_id,))
            else:
                # CRÉATION
                cur.execute("""
                    INSERT INTO comparatifs (libelle, requete_sql, ref_schema_id) 
                    VALUES (?, ?, ?)""", (libelle, sql, ref_id))
                self.selected_comp_id = cur.lastrowid

            # Insertion des nouvelles cibles cochées
            for target_id in selected_target_ids:
                cur.execute("""
                    INSERT INTO comparatif_cibles (comp_id, schema_id) 
                    VALUES (?, ?)""", (self.selected_comp_id, target_id))

            conn.commit()
            messagebox.showinfo("Succès", f"Le comparatif '{libelle}' a été enregistré avec {len(selected_target_ids)} cible(s).")
            
            self._load_comparatifs() # Rafraîchir la liste de gauche
            
        except Exception as e:
            messagebox.showerror("Erreur Base de données", f"Impossible d'enregistrer : {e}")
        finally:
            conn.close()
            
            
    def _save_log_file(self, schema_name, df):
        """Sauvegarde le résultat brut d'une extraction dans le dossier result."""
        try:
            import os
            from datetime import datetime
            
            if not os.path.exists("result"):
                os.makedirs("result")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"result/Resultat_{schema_name}_{timestamp}.txt"
            
            with open(filename, "w", encoding="utf-8") as f:
                f.write(f"SERVEUR : {schema_name}\n")
                f.write("-" * 30 + "\n")
                # to_string() permet de garder l'aspect tableau dans le fichier texte
                f.write(df.to_string(index=True))
            
            print(f"Log généré : {filename}")
        except Exception as e:
            print(f"Erreur sauvegarde log : {e}")
            
    def _compare_dataframes(self, df_ref, df_tgt):
        """Identifie les écarts même si les données contiennent des doublons d'index."""
        common_cols = list(df_ref.columns.intersection(df_tgt.columns))
        
        # 1. Conversion en listes de dictionnaires (orient='records')
        # On garde l'index (la clé unique) comme une colonne normale
        ref_list = df_ref[common_cols].reset_index().to_dict(orient='records')
        tgt_list = df_tgt[common_cols].reset_index().to_dict(orient='records')

        # 2. Reconstruction de dictionnaires propres en gérant les doublons d'ID
        # On utilise le nom de la première colonne (la clé)
        pk_name = df_ref.index.name if df_ref.index.name else df_ref.index.names[0]
        
        def list_to_map(data_list):
            d_map = {}
            for row in data_list:
                key = str(row.get(pk_name, ""))
                # Si la clé existe déjà (doublon), on l'ignore ou on la suffixe
                # Ici on garde la première occurrence pour rester stable
                if key not in d_map:
                    d_map[key] = row
            return d_map

        dict_ref = list_to_map(ref_list)
        dict_tgt = list_to_map(tgt_list)

        # 3. Comparaison des clés
        keys_ref = set(dict_ref.keys())
        keys_tgt = set(dict_tgt.keys())
        
        val_diffs = []
        missing = list(keys_ref - keys_tgt)
        extra = list(keys_tgt - keys_ref)
        common_keys = keys_ref.intersection(keys_tgt)

        # 4. Comparaison colonne par colonne
        for key in common_keys:
            row_ref = dict_ref[key]
            row_tgt = dict_tgt[key]
            
            for col in common_cols:
                v_ref = str(row_ref.get(col, "")).strip()
                v_tgt = str(row_tgt.get(col, "")).strip()
                
                # Nettoyage des valeurs 'nulles'
                if v_ref.lower() in ['none', 'nan', 'nat', 'null']: v_ref = ""
                if v_tgt.lower() in ['none', 'nan', 'nat', 'null']: v_tgt = ""
                
                if v_ref != v_tgt:
                    val_diffs.append({
                        'key': key,
                        'col': col,
                        'ref': v_ref,
                        'tgt': v_tgt
                    })

        return {
            "missing": missing,
            "extra": extra,
            "values": val_diffs
        }
    def _generate_html_report(self, title, pivot_name, all_results):
        """Génère un rapport strictement limité aux écarts de données."""
        html = f"""
        <html>
        <head>
            <meta charset="utf-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .schema-block {{ border: 1px solid #ddd; padding: 15px; margin-bottom: 25px; border-radius: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
                th, td {{ border: 1px solid #ccc; padding: 10px; text-align: left; }}
                th {{ background-color: #f8f9fa; }}
                .key-cell {{ font-weight: bold; color: #2c3e50; }}
                .col-cell {{ color: #e67e22; font-weight: bold; font-family: monospace; }}
                .val-ref {{ background-color: #fdedec; color: #c0392b; }}
                .val-tgt {{ background-color: #eafaf1; color: #1e8449; }}
            </style>
        </head>
        <body>
            <h1>Rapport d'écarts : {title}</h1>
            <p><b>Référence (Pivot) :</b> {pivot_name}</p>
        """

        for s_name, res in all_results.items():
            html += f"<div class='schema-block'><h2>Cible : {s_name}</h2>"

            # 1. Manquants (Simple liste d'IDs)
            if res['missing']:
                html += f"<p style='color:red;'><b>❌ IDs manquants :</b> {', '.join(map(str, res['missing']))}</p>"

            # 2. Écarts de données (Tableau 4 colonnes)
            if res['values']:
                html += """
                <table>
                    <thead>
                        <tr>
                            <th>ID (Clé)</th>
                            <th>Colonne en écart</th>
                            <th>Valeur Pivot</th>
                            <th>Valeur Cible</th>
                        </tr>
                    </thead>
                    <tbody>
                """
                for v in res['values']:
                    html += f"""
                        <tr>
                            <td class="key-cell">{v['key']}</td>
                            <td class="col-cell">{v['col']}</td>
                            <td class="val-ref">{v['ref']}</td>
                            <td class="val-tgt">{v['tgt']}</td>
                        </tr>
                    """
                html += "</tbody></table>"
            
            if not res['missing'] and not res['values']:
                html += "<p style='color:green;'>✅ Aucune différence détectée.</p>"

            html += "</div>"

        html += "</body></html>"

        with open("Rapport_Ecarts.html", "w", encoding="utf-8") as f:
            f.write(html)
        
        import webbrowser, os
        webbrowser.open('file://' + os.path.abspath("Rapport_Ecarts.html")) 

    def _get_dataframe(self, schema_id, sql, pwd):
        """Se connecte à Oracle et retourne un DataFrame Pandas."""
        import oracledb
        import pandas as pd
        
        conn_sqlite = sqlite3.connect(DB_PATH)
        # On récupère les infos de connexion via l'ID du schéma
        s_info = conn_sqlite.execute("""
            SELECT c.host, c.port, c.service, s.schema, c.type_db 
            FROM schemas s 
            JOIN DB_conn c ON s.DB_conn_id = c.id 
            WHERE s.id = ?
        """, (schema_id,)).fetchone()
        conn_sqlite.close()

        if not s_info:
            return None

        host, port, service, schema_user, type_db = s_info

        try:
            dsn = f"{host}:{port}/{service}"
            conn_oracle = oracledb.connect(user=schema_user, password=pwd, dsn=dsn)
            
            cursor = conn_oracle.cursor()
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            # ------------------------
            
            cursor.close()
            conn_oracle.close()
            return df
            
        except Exception as e:
            print(f"❌ Erreur sur le schéma {schema_user} ({host}): {e}")
            messagebox.showwarning("Erreur Connexion", f"Impossible de joindre {schema_user} :\n{e}")
            return None
    
    def _update_status(self, text, color="black"):
        """Met à jour le texte de statut et force l'affichage"""
        self.lbl_status.config(text=text, foreground=color)
        self.update_idletasks() # Force Tkinter à rafraîchir l'écran immédiatement

    def _run_comparison(self):
        """Lance l'exécution de la requête et gère les logs et la comparaison."""
        libelle = self.ent_libelle.get().strip()
        sql = self.txt_sql.get("1.0", tk.END).strip()
        pivot_name = self.cb_ref.get()
        
        # Nettoyage du SQL pour Oracle (Point-virgule et espaces)
        sql = sql.rstrip(';').strip()
        
        target_ids = [s_id for s_id, var in self.target_vars.items() if var.get()]
        if not sql or not target_ids:
            messagebox.showwarning("Attention", "Veuillez renseigner le SQL et cocher au moins une cible.")
            return

        pwd = simpledialog.askstring("Connexion Oracle", f"Entrez le mot de passe :", show='*')
        if not pwd: return

        dialog = ProgressDialog(self, title="Exécution en cours...")
        total_steps = len(target_ids) + (1 if pivot_name else 0)
        dialog.progress["maximum"] = total_steps

        all_diffs = {}
        df_pivot_idx = None
        current_step = 0

        try:
            # 1. Traitement du Pivot (si sélectionné)
            if pivot_name:
                current_step += 1
                dialog.update(f"Phase {current_step}/{total_steps}", f"Extraction Pivot : {pivot_name}", current_step)
                ref_id = self.schema_mapping[pivot_name]
                df_pivot = self._get_dataframe(ref_id, sql, pwd)
                
                if df_pivot is not None and not df_pivot.empty:
                    pk_col = df_pivot.columns[0]
                    df_pivot_idx = df_pivot.set_index(pk_col)
                    # Sauvegarde log pour le pivot
                    self._save_log_file(pivot_name, df_pivot)
                else:
                    messagebox.showerror("Erreur", f"Le pivot {pivot_name} est vide.")
                    dialog.destroy()
                    return

            # 2. Boucle sur les Cibles
            for s_id in target_ids:
                target_name = [name for name, idx in self.schema_mapping.items() if idx == s_id][0]
                if target_name == pivot_name: continue # On ne compare pas le pivot avec lui-même
                
                current_step += 1
                dialog.update(f"Phase {current_step}/{total_steps}", f"Extraction : {target_name}", current_step)
                
                df_target = self._get_dataframe(s_id, sql, pwd)
                
                if df_target is not None:
                    self._save_log_file(target_name, df_target)
                    
                    # Logique de comparaison si le pivot existe
                    if df_pivot_idx is not None:
                        pk_col = df_target.columns[0]
                        df_target_idx = df_target.set_index(pk_col)
                        all_diffs[target_name] = self._compare_dataframes(df_pivot_idx, df_target_idx)
                else:
                    all_diffs[target_name] = {"missing": [], "extra": [], "values": [], "error": "Inaccessible"}

            # 3. Rapport final
            if all_diffs:
                dialog.update("Finalisation", "Génération du rapport HTML...", total_steps)
                self._generate_html_report(libelle, pivot_name, all_diffs)
            
            dialog.destroy()
            messagebox.showinfo("Succès", "Traitement terminé. Les logs sont dans 'result' et le rapport est ouvert.")

        except Exception as e:
            if dialog.winfo_exists(): dialog.destroy()
            messagebox.showerror("Erreur", f"Erreur lors de l'exécution : {str(e)}")

    def _update_status(self, text, color="black"):
        """Met à jour le label de statut et force le rafraîchissement UI."""
        self.lbl_status.config(text=text, foreground=color)
        self.update_idletasks() # Indispensable pour voir l'évolution en temps réel

    def _load_comparatifs(self):
        """Remplit la Listbox de gauche avec les comparatifs de la base"""
        self.list_comp.delete(0, tk.END) # On vide la liste
        self.comp_ids_map = [] # Pour stocker l'ID réel de chaque ligne

        conn = get_db_connection()
        comps = conn.execute("SELECT id, libelle FROM comparatifs ORDER BY libelle").fetchall()
        conn.close()

        for c in comps:
            self.list_comp.insert(tk.END, c[1]) # On insère le libellé
            self.comp_ids_map.append(c[0])     # On garde l'ID en mémoire

    
    def _update_target_list(self, event=None):
        """Masque le schéma de référence de la liste des cibles et le décoche"""
        pivot_display = self.cb_ref.get()
        if not pivot_display:
            return

        ref_id = self.schema_mapping.get(pivot_display)

        # On parcourt tous les widgets de la zone de droite
        # (On suppose que chaque checkbutton est un enfant de self.scroll_frame)
        for widget in self.scroll_frame.winfo_children():
            # On récupère l'ID associé au texte du checkbutton
            # (ou on utilise un dictionnaire de widgets si tu en as créé un)
            schema_name = widget.cget("text")
            schema_id = self.schema_mapping.get(schema_name)

            if schema_id == ref_id:
                # C'est le pivot : on le décoche, on le désactive et on le cache
                self.target_vars[schema_id].set(False)
                widget.pack_forget() # On le retire de l'affichage
            else:
                # Ce n'est pas le pivot : on s'assure qu'il est visible
                widget.pack(anchor="w", padx=5)
    def _on_select_comp(self, event):
        """Charge les détails du comparatif sélectionné"""
        selection = self.list_comp.curselection()
        if not selection:
            return

        idx = selection[0]
        self.selected_comp_id = self.comp_ids_map[idx]

        conn = get_db_connection()
        # 1. Charger les infos principales
        row = conn.execute("""
            SELECT c.libelle, c.requete_sql, s.schema, db.libelle 
            FROM comparatifs c
            JOIN schemas s ON c.ref_schema_id = s.id
            JOIN DB_conn db ON s.DB_conn_id = db.id
            WHERE c.id = ?
        """, (self.selected_comp_id,)).fetchone()

        if row:
            self.ent_libelle.delete(0, tk.END)
            self.ent_libelle.insert(0, row[0])
            self.txt_sql.delete("1.0", tk.END)
            self.txt_sql.insert("1.0", row[1])
            # On sélectionne le pivot dans la combo
            pivot_display = f"{row[3]} - {row[2]}"
            self.cb_ref.set(pivot_display)
            self._update_target_list()
            self._update_target_list()
        # 2. Cocher les cibles enregistrées
        # On commence par tout décocher
        for var in self.target_vars.values():
            var.set(False)

        cibles = conn.execute("SELECT schema_id FROM comparatif_cibles WHERE comp_id = ?", 
                             (self.selected_comp_id,)).fetchall()
        conn.close()

        for c in cibles:
            if c[0] in self.target_vars:
                self.target_vars[c[0]].set(True)

    def _clear_form(self):
        """Réinitialise tout l'écran"""
        self.selected_comp_id = None
        self.ent_libelle.delete(0, tk.END)
        self.txt_sql.delete("1.0", tk.END)
        self.cb_ref.set('')
        for var in self.target_vars.values():
            var.set(False)
        self.list_comp.selection_clear(0, tk.END)

    def _delete_comp(self):
        """Supprime le comparatif sélectionné après confirmation"""
        # 1. Vérifier si un comparatif est sélectionné
        if not self.selected_comp_id:
            messagebox.showwarning("Attention", "Veuillez sélectionner un comparatif à supprimer dans la liste.")
            return

        # 2. Demander confirmation à l'utilisateur
        nom_comp = self.ent_libelle.get()
        if not messagebox.askyesno("Confirmation", f"Êtes-vous sûr de vouloir supprimer le comparatif '{nom_comp}' ?"):
            return

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        try:
            # 3. Supprimer les cibles associées (intégrité référentielle)
            cur.execute("DELETE FROM comparatif_cibles WHERE comp_id = ?", (self.selected_comp_id,))
            
            # 4. Supprimer le comparatif
            cur.execute("DELETE FROM comparatifs WHERE id = ?", (self.selected_comp_id,))
            
            conn.commit()
            messagebox.showinfo("Succès", "Le comparatif a été supprimé.")
            
            # 5. Nettoyer l'interface et rafraîchir la liste
            self._clear_form()
            self._load_comparatifs()
            
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur lors de la suppression : {e}")
        finally:
            conn.close()
            
    def _on_close(self):
        self.parent.deiconify()
        self.destroy()

class ProbeEngine:
    def __init__(self, sqlite_conn_func, logger=None):
        """
        :param logger: Instance de la fenêtre principale pour appeler _log
        """
        self.get_sqlite_conn = sqlite_conn_func
        self.report_path = "rapport_sondes.html"
        self.logger = logger # On stocke la fenêtre principale

    def _internal_log(self, message, level="INFO"):
        """Envoie le message vers la fenêtre principale ou la console"""
        if self.logger and hasattr(self.logger, '_log'):
            self.logger._log(message, level)
        else:
            print(f"[{level}] {message}")

    def run_all(self, db_filter=None, pack_id=None, user=None, pwd=None):
        self._internal_log(f"🔍 Recherche des sondes pour le pack ID: {pack_id}...")
        # 1. On va chercher le nom du pack en base
        conn = self.get_sqlite_conn()
        pack_name = "Rapport"
        try:
            res = conn.execute("SELECT nom FROM reporting_packs WHERE id=?", (pack_id,)).fetchone()
            if res:
                pack_name = res[0]
        except Exception as e:
            self._internal_log(f"⚠️ Impossible de récupérer le nom du pack : {e}", "WARNING")
        finally:
            conn.close()
            
        targets = self._get_probe_mapping(db_filter, pack_id)
        
        if not targets:
            self._internal_log(f"⚠️ Aucune sonde trouvée pour le pack {pack_id} et le type {db_filter}", "WARNING")
            return

        self._internal_log(f"✅ {len(targets)} sonde(s) à exécuter.")
        all_results = []
        
        for target in targets:
            self._internal_log(f"🚀 Exécution : {target['libelle']} sur {target['host']}...")
            result = self._execute_probe(target, pwd)
            
            if result.get('error'):
                self._internal_log(f"❌ Erreur sur {target['libelle']}: {result['error']}", "ERROR")
            else:
                self._internal_log(f"✔️ {target['libelle']} terminé avec succès.")
                
            all_results.append(result)

        self._internal_log("📊 Génération du rapport HTML...")
        self._generate_html_report(all_results, pack_name)
        self._internal_log(f"🏁 Rapport disponible : {self.report_path}")

    def _get_probe_mapping(self, db_filter, pack_id):
        """Récupère les cibles et démultiplie les sondes par schéma (Correctif Colonne)"""
        conn = self.get_sqlite_conn()
        
        query = """
            SELECT 
                s.nom_sonde as libelle, 
                s.type_alerte,
                s.requete as requete_sql, 
                o.host, o.port, o.service, o.type_db, 
                s.lien_MOP,
                sch.schema as schema_technique,
                f.nom as fonction_nom  
            FROM reporting_items ri
            JOIN sondes s ON ri.sonde_id = s.id
            JOIN reporting_packs rp ON ri.pack_id = rp.id
            JOIN sonde_fonctions f ON s.fonction_id = f.id 
            JOIN sonde_cibles sc ON s.id = sc.sonde_id
            
            -- ON UTILISE DB_conn_id AU LIEU DE db_id --
            LEFT JOIN schemas sch ON (
                (sc.type_cible = 'SCHEMA' AND sch.id = sc.cible_id) OR
                (sc.type_cible = 'GRAPPE' AND sch.DB_conn_id = sc.cible_id)
            )
            -- On récupère les infos de connexion via le schéma trouvé
            JOIN DB_conn o ON sch.DB_conn_id = o.id
            
            WHERE rp.id = ?
        """
        
        params = [pack_id]
        if db_filter:
            query += " AND s.type_db = ?"
            params.append(db_filter)

        query += " ORDER BY ri.ordre ASC, sch.schema ASC"

        try:
            df = pd.read_sql_query(query, conn, params=params)
            
            if df.empty:
                self._internal_log(f"⚠️ Aucune donnée trouvée (Vérifiez les jointures dans la BDD)", "WARNING")
            else:
                self._internal_log(f"✅ Mapping chargé : {len(df)} exécutions prévues.")
                for _, row in df.iterrows():
                    self._internal_log(f"   -> Sonde: {row['libelle']} | Schéma: {row['schema_technique']}", "DEBUG")
            
            return df.to_dict(orient='records')
        except Exception as e:
            self._internal_log(f"❌ Erreur SQL Mapping : {str(e)}", "ERROR")
            raise e
        finally:
            conn.close()

    def _execute_probe(self, target, pwd):
        """DÉFINITION : Reçoit target et pwd (2 arguments au total)"""
        data = None
        error = None
        
        # Le schéma récupéré en BDD devient notre utilisateur Oracle
        schema_as_user = target.get('schema_technique')

        try:
            if target['type_db'] == 'Oracle':
                # Appel de la connexion Oracle
                data = self._run_oracle(target, schema_as_user, pwd)
            elif target['type_db'] == 'SQLite':
                data = self._run_sqlite(target)
            else:
                error = f"SGBD {target['type_db']} non supporté."
        except Exception as e:
            error = str(e)

        return {
            "libelle": target['libelle'],
            "fonction_nom": target.get('fonction_nom', 'AUTRE'),
            "alerte": target.get('type_alerte', 'Mineur'),
            "lien_mop": target.get('lien_MOP', ''),
            "host": target['host'],
            "service": target['service'],
            "schema": schema_as_user,
            "type_db": target['type_db'],
            "data": data,
            "error": error,
            "requete_sql": target['requete_sql']
        }

    def _run_oracle(self, t, schema_as_user, pwd):
        import oracledb
        
        # Vérifions les valeurs extraites de la BDD
        host = t['host']
        port = t['port']
        service = t['service']
        
        # Log de contrôle pour être sûr de ce qu'on envoie
        self._internal_log(f"   - DEBUG DSN : Host={host}, Port={port}, Service={service}", "DEBUG")

        # Construction du DSN
        dsn = oracledb.makedsn(host, port, service_name=service)
        
        try:
            conn = oracledb.connect(user=schema_as_user, password=pwd, dsn=dsn)
            df = pd.read_sql(t['requete_sql'], conn)
            return df
        except Exception as e:
            self._internal_log(f"❌ Erreur de connexion : {str(e)}", "ERROR")
            raise e
        finally:
            if 'conn' in locals():
                conn.close()

    def _run_sqlite(self, t):
        db_path = t['host']
        conn = sqlite3.connect(db_path)
        try:
            df = pd.read_sql_query(t['requete_sql'], conn)
            return df
        finally:
            conn.close()

    def _generate_html_report(self, results, pack_name):
        """Génère le rapport HTML avec regroupement par Fonction > Sonde > Schémas"""
        from collections import defaultdict
        import os
        import webbrowser
        from datetime import datetime
        
        now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        
        # 1. Double regroupement : [Fonction][Nom_Sonde] = liste des résultats
        structured_data = defaultdict(lambda: defaultdict(list))
        for r in results:
            f_nom = r.get('fonction_nom', 'AUTRE')
            s_nom = r.get('libelle', 'Sonde sans nom')
            structured_data[f_nom][s_nom].append(r)

        color_map = {"Critique": "#e74c3c", "Majeur": "#e67e22", "Mineur": "#3498db"}

        html = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', sans-serif; margin: 0; padding: 40px 0; background-color: #444; }}
                .page-a4 {{ background: white; width: 95%; max-width: 1400px; margin: 0 auto; padding: 30px; box-shadow: 0 0 20px rgba(0,0,0,0.5); min-height: 100vh; }}
                header {{ border-bottom: 2px solid #2c3e50; margin-bottom: 30px; text-align: center; padding-bottom: 10px; }}
                h1 {{ margin: 0; color: #2c3e50; }}
                .date {{ color: #7f8c8d; font-style: italic; font-size: 0.9em; }}
                
                .report-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 25px; align-items: start; }}
                .function-block {{ border: 1px solid #ddd; border-radius: 8px; padding: 15px; background: #fff; margin-bottom: 20px; }}
                .function-title {{ font-weight: bold; text-transform: uppercase; border-bottom: 2px solid #eee; padding-bottom: 10px; margin-bottom: 15px; color: #2c3e50; }}
                
                .sonde-card {{ background: #fdfdfd; border: 1px solid #eee; border-radius: 4px; margin-bottom: 25px; padding: 12px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }}
                .sonde-title {{ font-weight: bold; font-size: 1.1em; margin-bottom: 12px; padding-bottom: 5px; border-bottom: 2px solid #34495e; display: flex; justify-content: space-between; }}
                
                .execution-row {{ display: flex; gap: 15px; margin-bottom: 15px; padding-top: 10px; border-top: 1px dashed #ccc; }}
                .execution-row:first-of-type {{ border-top: none; padding-top: 0; }}

                .col-schema {{ flex: 0 0 150px; background: #f1f2f6; border-radius: 4px; display: flex; align-items: center; justify-content: center; text-align: center; font-weight: bold; font-size: 0.85em; color: #2c3e50; padding: 10px; border: 1px solid #d1d8e0; align-self: flex-start; }}
                .col-result {{ flex: 1; overflow-x: auto; min-width: 0; }}
                
                .sql-box {{ font-family: monospace; font-size: 0.75em; background: #272822; color: #f8f8f2; padding: 8px; margin: 5px 0; border-radius: 3px; }}
                .mop-btn {{ text-decoration: none; background: #34495e; color: white; padding: 3px 10px; border-radius: 3px; font-size: 0.75em; font-weight: normal; }}
                
                table {{ border-collapse: collapse; width: 100%; font-size: 0.85em; background: white; }}
                th {{ background-color: #f8f9fa; padding: 8px; border: 1px solid #dee2e6; text-align: left; }}
                td {{ padding: 6px; border: 1px solid #dee2e6; }}
                .error {{ color: #e74c3c; font-weight: bold; background: #fee; padding: 10px; border-radius: 4px; font-size: 0.8em; }}
                
                @media (max-width: 1200px) {{ .report-grid {{ grid-template-columns: 1fr; }} }}
            </style>
        </head>
        <body>
            <div class="page-a4">
                <header>
                    <h1>📊 Rapport d'Exploitation: {pack_name}</h1>
                    <div class="date">Généré le {now}</div>
                </header>
                <div class="report-grid">
        """

        for f_nom in sorted(structured_data.keys()):
            html += f'<div class="function-block"><div class="function-title">📂 {f_nom}</div>'
            
            for s_nom, executions in structured_data[f_nom].items():
                # On prend les infos communes du premier résultat de la liste
                first_exec = executions[0]
                alerte_raw = str(first_exec.get('alerte', 'Mineur')).strip().capitalize()
                couleur = color_map.get(alerte_raw, "#2c3e50")
                mop_url = first_exec.get('lien_mop')
                html_mop = f'<a href="{mop_url}" target="_blank" class="mop-btn">📖 MOP</a>' if mop_url and str(mop_url) != 'nan' else ""

                html += f"""
                <div class="sonde-card">
                    <div class="sonde-title" style="color: {couleur}; border-bottom-color: {couleur};">
                        <span>● {s_nom}</span>
                        {html_mop}
                    </div>
                    
                    <details style="margin-bottom: 15px;">
                        <summary style="font-size: 0.75em; cursor: pointer; color: #3498db; font-weight: bold;">📜 Voir la requête SQL commune</summary>
                        <div class="sql-box">{first_exec.get('requete_sql', 'N/A')}</div>
                    </details>
                """

                # On itère sur les différentes exécutions (Schémas) de cette même sonde
                for r in executions:
                    is_sqlite = str(r.get('type_db', '')).upper() == "SQLITE"
                    
                    html += '<div class="execution-row">'
                    
                    if not is_sqlite:
                        html += f"""
                        <div class="col-schema">
                            <div><small style="color:#7f8c8d;">SCHEMA</small><br>{r['schema']}</div>
                        </div>
                        """
                    
                    html += '<div class="col-result">'
                    if r.get('error'):
                        html += f"<div class='error'>❌ {r['error']}</div>"
                    elif r.get('data') is not None and not r['data'].empty:
                        html += r['data'].to_html(index=False, border=0)
                    else:
                        html += "<p style='font-size: 0.8em; color: #27ae60; margin: 0;'>✅ Aucune anomalie.</p>"
                    
                    html += "</div></div>" # Fin execution-row

                html += "</div>" # Fin sonde-card
            html += "</div>" # Fin function-block

        html += "</div></div></body></html>"

        try:
            abs_path = os.path.abspath(self.report_path)
            with open(abs_path, "w", encoding="utf-8") as f:
                f.write(html)
            webbrowser.open("file:///" + abs_path.replace("\\", "/"))
        except Exception as e:
            print(f"Erreur : {e}")

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
    def __init__(self, master, pack_id, pack_name, db_type):
        super().__init__(master)
        self.pack_id = pack_id
        self.pack_name = pack_name
        self.db_type = db_type
        self.title(f"Organisation du Pack : {self.pack_name}")
        
        # --- PLEIN ÉCRAN ---
        try:
            self.state('zoomed')
        except:
            sw = self.winfo_screenwidth()
            sh = self.winfo_screenheight()
            self.geometry(f"{sw}x{sh}+0+0")

        self.configure(bg="#444") # Fond gris foncé pour le contraste
        self.grab_set()

        self.sections = {} 
        self.drag_widget = None
        self.floating_label = None

        # --- ZONE SCROLLABLE ---
        self.main_container = tk.Frame(self, bg="#444")
        self.main_container.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(self.main_container, bg="#444", highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.main_container, orient="vertical", command=self.canvas.yview)
        
        # La "Page" blanche centrale
        self.page_white = tk.Frame(self.canvas, bg="white", padx=40, pady=40)
        
        self.page_white.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas_window = self.canvas.create_window((self.winfo_screenwidth()/2, 0), 
                                                       window=self.page_white, anchor="n")
        
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # --- STRUCTURE EN DEUX COLONNES DANS LA PAGE ---
        self.page_white.grid_columnconfigure(0, weight=1)
        self.page_white.grid_columnconfigure(1, weight=1)

        self.col_left = tk.Frame(self.page_white, bg="white")
        self.col_left.grid(row=0, column=0, sticky="nsew", padx=15)
        
        self.col_right = tk.Frame(self.page_white, bg="white")
        self.col_right.grid(row=0, column=1, sticky="nsew", padx=15)

        # --- BOUTON FIXE EN BAS ---
        footer = tk.Frame(self, bg="#222", pady=15)
        footer.pack(fill="x", side="bottom")
        
        tk.Button(footer, text=f"💾 ENREGISTRER L'ORDRE POUR : {self.pack_name.upper()}", 
                  bg="#28a745", fg="white", font=("Arial", 12, "bold"), 
                  padx=40, pady=10, command=self._save_order).pack()

        self._build_sections()
        
        # Responsive : Ajuste la largeur de la page blanche
        self.bind("<Configure>", self._on_resize)

    def _on_resize(self, event):
        cw = self.canvas.winfo_width()
        new_width = min(1200, cw * 0.95)
        self.canvas.itemconfig(self.canvas_window, width=new_width)

    def _build_sections(self):
        """Récupère UNIQUEMENT les sondes cochées du pack et les groupe par fonction"""
        conn = get_db_connection()
        
        # 1. On récupère les catégories (fonctions)
        fonctions = conn.execute("SELECT * FROM sonde_fonctions ORDER BY nom").fetchall()
        
        display_count = 0
        for func in fonctions:
            # 2. On récupère les sondes qui sont liées à ce pack ET à cette fonction
            sondes = conn.execute("""
                SELECT s.id, s.nom_sonde 
                FROM reporting_items ri
                JOIN sondes s ON ri.sonde_id = s.id
                WHERE ri.pack_id = ? AND s.fonction_id = ?
                ORDER BY ri.ordre
            """, (self.pack_id, func['id'])).fetchall()
            
            if sondes:
                # Alternance Gauche (pair) / Droite (impair)
                parent_col = self.col_left if display_count % 2 == 0 else self.col_right
                
                frame = tk.LabelFrame(parent_col, text=f"  {func['nom'].upper()}  ", 
                                      bg="white", font=("Arial", 10, "bold"), padx=10, pady=10)
                frame.pack(fill="x", pady=15, padx=5, anchor="n")
                
                frame.fonction_id = func['id']
                self.sections[func['id']] = frame
                
                for s in sondes:
                    self._create_item(frame, s['id'], s['nom_sonde'])
                
                display_count += 1
        conn.close()

    def _create_item(self, parent, s_id, nom):
        f = tk.Frame(parent, bg="#f8f9fa", bd=1, relief="solid", cursor="fleur")
        f.pack(fill="x", pady=3)
        f.s_id = s_id
        
        lbl = tk.Label(f, text=f" ☰   {nom}", bg="#f8f9fa", anchor="w", pady=10, font=("Segoe UI", 10))
        lbl.pack(fill="x")
        
        lbl.bind("<Button-1>", self._on_start)
        lbl.bind("<B1-Motion>", self._on_drag)
        lbl.bind("<ButtonRelease-1>", self._on_drop)

    def _on_start(self, event):
        self.drag_widget = event.widget.master
        self.drag_widget.config(bg="#e9ecef")
        
        self.floating_label = tk.Toplevel(self)
        self.floating_label.overrideredirect(True)
        self.floating_label.attributes("-alpha", 0.8)
        
        f = tk.Frame(self.floating_label, bg="#d1ecf1", bd=2, relief="ridge")
        f.pack()
        tk.Label(f, text=event.widget.cget("text"), bg="#d1ecf1", padx=15, pady=8, font=("Segoe UI", 10, "bold")).pack()

    def _on_drag(self, event):
        if self.floating_label:
            self.floating_label.geometry(f"+{event.x_root+15}+{event.y_root+15}")

    def _on_drop(self, event):
        if self.floating_label:
            self.floating_label.destroy()
            self.floating_label = None

        x, y = event.x_root, event.y_root
        target_section = None
        
        # On cherche dans quelle section (LabelFrame) on a lâché la souris
        for fid, frame in self.sections.items():
            x1, y1 = frame.winfo_rootx(), frame.winfo_rooty()
            x2, y2 = x1 + frame.winfo_width(), y1 + frame.winfo_height()
            if x1 <= x <= x2 and y1 <= y <= y2:
                target_section = frame
                break
        
        if target_section:
            s_id = self.drag_widget.s_id
            s_nom = event.widget.cget("text").replace(" ☰   ", "")
            self.drag_widget.destroy()
            
            # Recréation de la sonde dans la nouvelle section (ou la même à nouvelle position)
            new_w = self._create_item(target_section, s_id, s_nom)
            
            y_in_frame = y - target_section.winfo_rooty()
            children = [c for c in target_section.winfo_children() if hasattr(c, 's_id') and c != new_w]
            
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
            self.drag_widget.config(bg="#f8f9fa")

    def _save_order(self):
        conn = get_db_connection()
        try:
            # On initialise bien la variable ici
            global_idx = 0 
            
            for fid, frame in self.sections.items():
                sondes_widgets = [c for c in frame.winfo_children() if hasattr(c, 's_id')]
                for w in sondes_widgets:
                    # Remplacement de global_order par global_idx
                    conn.execute("""
                        UPDATE reporting_items 
                        SET ordre = ? 
                        WHERE pack_id = ? AND sonde_id = ?
                    """, (global_idx, self.pack_id, w.s_id))
                    
                    global_idx += 1 # On incrémente la bonne variable
            
            conn.commit()
            messagebox.showinfo("Succès", "Ordre du pack enregistré !")
            self.destroy()
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur de sauvegarde : {e}")
        finally:
            conn.close()

class ReportingManagerWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.title("Gestion des Packs de Reporting")
        
        # --- CENTRAGE DE LA FENÊTRE ---
        w, h = 1000, 750
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        self.grab_set()
        
        self.db_type_var = tk.StringVar(value="Oracle")
        self.pack_name_var = tk.StringVar()
        self.current_pack_id = None # Pour savoir si on crée ou on modifie
        self.check_vars = {} 

        self._build_ui()
        self._load_packs()

    def _build_ui(self):
        # --- SAISIE NOM ET TYPE ---
        top = ttk.LabelFrame(self, text=" 1. Détails du Reporting ", padding=10)
        top.pack(fill="x", padx=10, pady=5)
        
        ttk.Label(top, text="Nom :").pack(side="left")
        ttk.Entry(top, textvariable=self.pack_name_var, width=30).pack(side="left", padx=5)
        
        ttk.Label(top, text="Type BDD :").pack(side="left", padx=5)
        cb = ttk.Combobox(top, textvariable=self.db_type_var, values=("Oracle", "SQLite"), state="readonly", width=10)
        cb.pack(side="left", padx=5)
        cb.bind("<<ComboboxSelected>>", lambda e: self._load_sondes_list())

        # --- LISTE DES SONDES À COCHER ---
        mid = ttk.LabelFrame(self, text=" 2. Sélectionner les sondes à inclure ", padding=10)
        mid.pack(fill="both", expand=True, padx=10, pady=5)

        self.canvas = tk.Canvas(mid, highlightthickness=0)
        scroll = ttk.Scrollbar(mid, orient="vertical", command=self.canvas.yview)
        self.sf = ttk.Frame(self.canvas)
        
        self.sf.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.sf, anchor="nw")
        self.canvas.configure(yscrollcommand=scroll.set)
        self.canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        # --- LISTE DES PACKS EXISTANTS ---
        bot = ttk.LabelFrame(self, text=" 3. Reportings enregistrés ", padding=10)
        bot.pack(fill="x", padx=10, pady=5)

        self.tree = ttk.Treeview(bot, columns=("id", "nom", "type"), show="headings", height=5)
        self.tree.heading("id", text="ID"); self.tree.heading("nom", text="Libellé"); self.tree.heading("type", text="BDD")
        self.tree.column("id", width=50); self.tree.pack(side="left", fill="x", expand=True)
        
        self.tree.bind("<<TreeviewSelect>>", self._on_pack_select)

        # --- BOUTONS ---
        btns = ttk.Frame(self, padding=10)
        btns.pack(fill="x")
        ttk.Button(btns, text="💾 SAUVEGARDER (CRÉER/MAJ)", command=self._save_pack).pack(side="left", padx=5)
        ttk.Button(btns, text="✨ NOUVEAU (RESET)", command=self._reset_form).pack(side="left", padx=5)
        ttk.Button(btns, text="🗑️ SUPPRIMER", command=self._delete_pack).pack(side="left", padx=5)
        ttk.Button(btns, text="⚙️ ORGANISER)", command=self._open_organiser).pack(side="right", padx=5)

        self._load_sondes_list()

    def _on_pack_select(self, event):
        """Remplit le formulaire et coche les sondes lors d'un clic dans la liste"""
        sel = self.tree.selection()
        if not sel: return
        
        vals = self.tree.item(sel[0])['values']
        self.current_pack_id = vals[0]
        self.pack_name_var.set(vals[1])
        self.db_type_var.set(vals[2])
        
        # Recharger d'abord la liste des sondes du bon type
        self._load_sondes_list()
        
        # Cocher les sondes qui appartiennent à ce pack
        conn = get_db_connection()
        items = conn.execute("SELECT sonde_id FROM reporting_items WHERE pack_id=?", (self.current_pack_id,)).fetchall()
        active_ids = [r[0] for r in items]
        conn.close()
        
        for sid, var in self.check_vars.items():
            var.set(sid in active_ids)

    def _reset_form(self):
        """Vide le formulaire pour créer un nouveau pack"""
        self.current_pack_id = None
        self.pack_name_var.set("")
        for var in self.check_vars.values():
            var.set(False)
        self.tree.selection_remove(self.tree.selection())

    def _load_sondes_list(self):
        for w in self.sf.winfo_children(): w.destroy()
        self.check_vars = {}
        conn = get_db_connection()
        sondes = conn.execute("SELECT id, nom_sonde FROM sondes WHERE type_db=? ORDER BY nom_sonde", (self.db_type_var.get(),)).fetchall()
        for s in sondes:
            var = tk.BooleanVar()
            self.check_vars[s[0]] = var
            ttk.Checkbutton(self.sf, text=s[1], variable=var).pack(anchor="w", pady=1)
        conn.close()

    def _save_pack(self):
        nom = self.pack_name_var.get().strip()
        if not nom: return
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        if self.current_pack_id:
            # Mise à jour du nom et type
            cur.execute("UPDATE reporting_packs SET nom=?, db_type=? WHERE id=?", (nom, self.db_type_var.get(), self.current_pack_id))
            p_id = self.current_pack_id
        else:
            # Création
            cur.execute("INSERT INTO reporting_packs (nom, db_type) VALUES (?, ?)", (nom, self.db_type_var.get()))
            p_id = cur.lastrowid
        
        # Mise à jour des sondes (on vide et on remet)
        cur.execute("DELETE FROM reporting_items WHERE pack_id=?", (p_id,))
        for sid, var in self.check_vars.items():
            if var.get():
                cur.execute("INSERT INTO reporting_items (pack_id, sonde_id, ordre) VALUES (?, ?, 0)", (p_id, sid))
        
        conn.commit()
        conn.close()
        messagebox.showinfo("Succès", "Configuration enregistrée.")
        self._load_packs()

    def _load_packs(self):
        for i in self.tree.get_children(): self.tree.delete(i)
        conn = get_db_connection()
        res = conn.execute("SELECT * FROM reporting_packs").fetchall()
        for r in res: self.tree.insert("", "end", values=(r[0], r[1], r[2]))
        conn.close()

    def _delete_pack(self):
        sel = self.tree.selection()
        if not sel: return
        p_id = self.tree.item(sel[0])['values'][0]
        if messagebox.askyesno("Confirmation", "Supprimer ce pack ?"):
            conn = get_db_connection()
            conn.execute("DELETE FROM reporting_packs WHERE id=?", (p_id,))
            conn.execute("DELETE FROM reporting_items WHERE pack_id=?", (p_id,))
            conn.commit(); conn.close(); self._load_packs(); self._reset_form()

    def _open_organiser(self):
        sel = self.tree.selection()
        if not sel: 
            messagebox.showwarning("Attention", "Sélectionnez un pack dans la liste pour l'organiser.")
            return
        v = self.tree.item(sel[0])['values']
        OrganiserWindow(self, v[0], v[1], v[2])
class MassDeleteSondeWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Suppression en masse des sondes")
        self.geometry("600x700")
        self.grab_set()
        
        self.check_vars = {} # {sonde_id: BooleanVar}
        
        self._build_ui()
        self._load_sondes()

    def _build_ui(self):
        container = ttk.Frame(self, padding=15)
        container.pack(fill="both", expand=True)

        ttk.Label(container, text="Cochez les sondes à supprimer définitivement :", 
                  font=("Arial", 10, "bold")).pack(anchor="w", pady=(0, 10))

        # Zone de liste avec scrollbar
        list_frame = ttk.Frame(container)
        list_frame.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(list_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.canvas.yview)
        self.scrollable_frame = ttk.Frame(self.canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Boutons d'action
        btn_frame = ttk.Frame(container, padding=10)
        btn_frame.pack(fill="x")

        ttk.Button(btn_frame, text="TOUT COCHER", command=self._select_all).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="TOUT DÉCOCHER", command=self._deselect_all).pack(side="left", padx=5)
        
        # Bouton supprimer en rouge
        btn_del = tk.Button(btn_frame, text="🗑️ SUPPRIMER LA SÉLECTION", bg="#ffcccc", fg="red", 
                            font=("Arial", 10, "bold"), command=self._confirm_delete)
        btn_del.pack(side="right", padx=5)

    def _load_sondes(self):
        conn = get_db_connection()
        sondes = conn.execute("SELECT id, nom_sonde, type_db FROM sondes ORDER BY type_db, nom_sonde").fetchall()
        conn.close()

        for s in sondes:
            var = tk.BooleanVar()
            self.check_vars[s['id']] = var
            # Affichage formaté : [Type BDD] Nom de la sonde
            txt = f"[{s['type_db']}] {s['nom_sonde']}"
            chk = ttk.Checkbutton(self.scrollable_frame, text=txt, variable=var)
            chk.pack(anchor="w", pady=2)

    def _select_all(self):
        for var in self.check_vars.values(): var.set(True)

    def _deselect_all(self):
        for var in self.check_vars.values(): var.set(False)

    def _confirm_delete(self):
        to_delete = [sid for sid, var in self.check_vars.items() if var.get()]
        
        if not to_delete:
            return messagebox.showwarning("Attention", "Aucune sonde sélectionnée.")

        if messagebox.askyesno("Confirmation Critique", 
                               f"Êtes-vous sûr de vouloir supprimer ces {len(to_delete)} sondes ?\n"
                               "Cette action supprimera également les cibles et l'ordre dans les reportings."):
            conn = get_db_connection()
            try:
                # Suppression propre dans toutes les tables liées
                placeholders = ','.join(['?'] * len(to_delete))
                conn.execute(f"DELETE FROM sonde_cibles WHERE sonde_id IN ({placeholders})", to_delete)
                conn.execute(f"DELETE FROM reporting_items WHERE sonde_id IN ({placeholders})", to_delete)
                conn.execute(f"DELETE FROM sondes WHERE id IN ({placeholders})", to_delete)
                conn.commit()
                messagebox.showinfo("Succès", f"{len(to_delete)} sondes supprimées.")
                self.master._load_liste_sondes() # Rafraîchir la combo de recherche
                self.destroy()
            except Exception as e:
                conn.rollback()
                messagebox.showerror("Erreur", str(e))
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
        ttk.Button(f_func, text="-", width=3, command=self._delete_fonction_action).pack(side="left", padx=2)



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
        ttk.Button(f_btns, text="GERER REPORTING", command=self._open_reporting_manager).pack(side="left", fill="x", expand=True)
        ttk.Button(f_btns, text="🗑️ SUPPR. MASSE", command=self._open_mass_delete).pack(side="left", fill="x", expand=True, padx=2)
        tk.Button(f_btns, text="SUPPRIMER", bg="#ffcccc", fg="red", command=self._delete_sonde_active).pack(side="left", fill="x", expand=True, padx=2)

    def _open_mass_delete(self):
        MassDeleteSondeWindow(self)
    
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

        if not all([nom, f_nom, req, alerte]): 
            return messagebox.showwarning("Erreur", "Veuillez remplir le nom, la fonction, la requête et le niveau d'alerte.")
        
        is_safe, forbidden_word = _is_readonly_query(req)
        if not is_safe:
            messagebox.showerror(
                "Sécurité", 
                f"Action interdite détectée : '{forbidden_word}'\n\n"
                "Une sonde ne doit servir que pour de l'extraction d'informations (SELECT)."
            )
            return
        conn = get_db_connection()
        try:
            f_res = conn.execute("SELECT id FROM sonde_fonctions WHERE nom=?", (f_nom,)).fetchone()
            if not f_res: return messagebox.showerror("Erreur", "Fonction inconnue.")
            f_id = f_res['id']

            if self.current_sonde_id:
                # UPDATE : Correction de la syntaxe
                conn.execute("""UPDATE sondes SET 
                                nom_sonde=?, fonction_id=?, requete=?, type_db=?, 
                                type_sonde=?, type_alerte=?, lien_MOP=?
                                WHERE id=?""", 
                             (nom, f_id, req, db_type, t_sonde, alerte, mop, self.current_sonde_id))
                sid = self.current_sonde_id
                conn.execute("DELETE FROM sonde_cibles WHERE sonde_id=?", (sid,))
            else:
                # INSERT : Correction ici (on enlève le =? dans la liste des colonnes)
                cur = conn.cursor()
                cur.execute("""INSERT INTO sondes 
                                (nom_sonde, fonction_id, requete, type_db, type_sonde, type_alerte, lien_MOP) 
                                VALUES (?,?,?,?,?,?,?)""", 
                             (nom, f_id, req, db_type, t_sonde, alerte, mop))
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
            messagebox.showerror("Erreur SQL", f"Détail : {str(e)}")
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

    def _delete_fonction_action(self):
        f_nom = self.fonction_var.get()
        if not f_nom:
            return messagebox.showwarning("Attention", "Veuillez sélectionner une fonction à supprimer dans la liste.")

        # Confirmation
        if not messagebox.askyesno("Confirmation", f"Voulez-vous vraiment supprimer la fonction '{f_nom}' ?"):
            return

        conn = get_db_connection()
        try:
            # 1. Vérifier si des sondes utilisent cette fonction
            res = conn.execute("""
                SELECT COUNT(*) FROM sondes s
                JOIN sonde_fonctions f ON s.fonction_id = f.id
                WHERE f.nom = ?
            """, (f_nom,)).fetchone()

            if res[0] > 0:
                conn.close()
                return messagebox.showerror("Erreur", 
                    f"Impossible de supprimer '{f_nom}' : {res[0]} sonde(s) utilisent actuellement cette fonction.\n"
                    "Supprimez ou réaffectez les sondes d'abord.")

            # 2. Suppression
            conn.execute("DELETE FROM sonde_fonctions WHERE nom = ?", (f_nom,))
            conn.commit()
            
            messagebox.showinfo("Succès", f"Fonction '{f_nom}' supprimée.")
            
            # 3. Rafraîchissement
            self.fonction_var.set("")
            self._load_fonctions()
            
        except Exception as e:
            messagebox.showerror("Erreur", f"Une erreur est survenue : {e}")
        finally:
            conn.close()
            
    def _open_reporting_manager(self):
       ReportingManagerWindow(self)

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
        self.conn = sqlite3.connect(DB_PATH)
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
        tk.Button(btn_m_grid, text="Completer via MXDOS", command=self._open_import_schemas_popup, bg="#3498db", fg="white").pack(side=tk.LEFT, padx=5)
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
            
            
            
    def _open_import_schemas_popup(self):
        # 1. Récupérer les serveurs dispos en base SQLite
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, libelle, host, service FROM DB_conn")
        servers = cursor.fetchall()

        if not servers:
            messagebox.showwarning("Attention", "Aucun serveur configuré en base.")
            return

        # 2. Création de la popup
        popup = tk.Toplevel(self)
        popup.title("Importer les schémas depuis MXDOS")
        popup.geometry("400x300")
        popup.grab_set()

        tk.Label(popup, text="Sélectionnez le serveur source :", font=('Arial', 10, 'bold')).pack(pady=10)

        lb = tk.Listbox(popup, width=50)
        lb.pack(padx=10, pady=5)
        for s in servers:
            lb.insert(tk.END, f"{s[1]} ({s[2]} - {s[3]})")

        def proceed():
            selection = lb.curselection()
            if not selection: return
            
            server_data = servers[selection[0]] # (id, nom, host, service)
            popup.destroy()
            self._process_schema_import(server_data)

        tk.Button(popup, text="Suivant", command=proceed, bg="#2ecc71", fg="white").pack(pady=10)
        
    def _process_schema_import(self, server_info):
        srv_id, srv_nom, srv_host, srv_service = server_info

        # 1. Demander Login / Password
        login = simpledialog.askstring("Connexion Oracle", f"Login pour {srv_nom} :")
        if not login: return
        pwd = simpledialog.askstring("Connexion Oracle", f"Mot de passe pour {login} :", show='*')
        if not pwd: return

        try:
            # 2. Connexion Oracle
            dsn = oracledb.makedsn(srv_host, 1521, service_name=srv_service) # Port 1521 par défaut
            conn_ora = oracledb.connect(user=login, password=pwd, dsn=dsn)
            cursor_ora = conn_ora.cursor()

            sql = """
                SELECT TRIM(XDOS_IDSITE) as CLIENT  ,TRIM(XDOS_ID) as CODE, TRIM(XDOS_CDCORA) as SCHEMA, TRIM(XDOS_CDIORA) as SERVICE ,TRIM(XDOS_TYDOSS) AS TYPE_SCHEMA
                FROM MXDOS
                WHERE XDOS_TYDOSS <> 'MULT'
                AND XDOS_ID <> 'EXMETI'
                AND XDOS_FLCLOSE=0
                and xdos_idsite <> 'WI'
            """
            cursor_ora.execute(sql)
            rows = cursor_ora.fetchall()
            
            # 3. Insertion en SQLite
            cursor_sqlite = self.conn.cursor()
            added_count = 0
            
            for r in rows:
                client, code, schema_user, service, type_schema = r
                
                # Vérifier si le schéma existe déjà pour ce serveur
                cursor_sqlite.execute(
                    "SELECT id FROM schemas WHERE id_serveur = ? AND schema = ?", 
                    (srv_id, schema_user)
                )
                
                if not cursor_sqlite.fetchone():
                    # Adapte les colonnes ci-dessous à ta table 'schemas'
                    # Ici j'assume : id_serveur, nom_client, code, schema, service, type
                    cursor_sqlite.execute(
                        "INSERT INTO schemas (id_serveur, nom, code, schema, service, type) VALUES (?, ?, ?, ?, ?, ?)",
                        (srv_id, client, code, schema_user, service, type_schema)
                    )
                    added_count += 1

            self.conn.commit()
            conn_ora.close()
            
            messagebox.showinfo("Succès", f"Import terminé !\n{added_count} nouveaux schémas ajoutés.")
            self._load_schemas() # Recharger la liste dans l'interface

        except Exception as e:
            messagebox.showerror("Erreur Import", f"Impossible de récupérer les schémas :\n{str(e)}")
                
class ReportSelectorWindow(tk.Toplevel):
    def __init__(self, master, db_type):
        super().__init__(master)
        self.title("Sélection des Reportings")
        self.geometry("400x500")
        self.db_type = db_type
        self.result = [] # Liste des IDs sélectionnés
        self.grab_set()

        ttk.Label(self, text=f"Choisir les rapports ({db_type}) :", font=("Arial", 10, "bold")).pack(pady=10)

        # Zone scrollable pour les cases à cocher
        container = ttk.Frame(self)
        container.pack(fill="both", expand=True, padx=10)
        
        self.canvas = tk.Canvas(container, highlightthickness=0)
        scroll = ttk.Scrollbar(container, orient="vertical", command=self.canvas.yview)
        self.inner_frame = ttk.Frame(self.canvas)
        
        self.inner_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.inner_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scroll.set)
        
        self.canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        self.check_map = {} # {id_pack: BooleanVar}
        self._load_packs()

        ttk.Button(self, text="🚀 LANCER L'EXTRACTION", command=self._on_validate).pack(pady=15)

    def _load_packs(self):
        conn = get_db_connection()
        # On ne propose que les packs correspondant au type de BDD sélectionné au login
        packs = conn.execute("SELECT id, nom FROM reporting_packs WHERE db_type=?", (self.db_type,)).fetchall()
        for p in packs:
            var = tk.BooleanVar()
            self.check_map[p['id']] = (var, p['nom'])
            ttk.Checkbutton(self.inner_frame, text=p['nom'], variable=var).pack(anchor="w", pady=2)
        conn.close()

    def _on_validate(self):
        self.result = [pid for pid, (var, name) in self.check_map.items() if var.get()]
        if not self.result:
            messagebox.showwarning("Attention", "Veuillez cocher au moins un reporting.")
            return
        self.destroy()            
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
        init_db_compare()
        
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
        ttk.Button(top, text="Paramétrage Compare", command=self._open_compare_window).pack(side="right", padx=2)

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
        
        # Listes pour stocker les IDs correspondant aux index des Listbox
        self.ids_m = [] 
        self.ids_g = [] 
        self.ids_sqlite = []

        if t == "SQLite":
            self.l_sqlite.delete(0, "end")
            res = conn.execute("SELECT id, libelle FROM DB_conn WHERE type_db='SQLite'").fetchall()
            for r in res: 
                self.l_sqlite.insert("end", r['libelle'])
                self.ids_sqlite.append(r['id'])
        else:
            # 1. Schémas : Code pur comme avant
            self.l_m.delete(0, "end")
            res = conn.execute("""
                SELECT m.id, m.code 
                FROM schemas m 
                JOIN DB_conn o ON m.DB_conn_id = o.id 
                WHERE o.type_db=?
            """, (t,)).fetchall()
            for r in res: 
                self.l_m.insert("end", r['code'])
                self.ids_m.append(r['id'])
            
            # 2. Grappes : Libellé + Service pour différencier les instances
            self.l_g.delete(0, "end")
            res_g = conn.execute("SELECT id, libelle, service from DB_conn WHERE type_db=?", (t,)).fetchall()
            for r in res_g: 
                # Affichage combiné pour lever l'ambiguïté
                display_text = f"{r['libelle']} ({r['service']})"
                self.l_g.insert("end", display_text)
                self.ids_g.append(r['id'])
                
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
                target_id = self.ids_sqlite[i] # On récupère l'ID réel
                res = conn_sqlite.execute("SELECT host, libelle FROM DB_conn WHERE id=?", (target_id,)).fetchone()
                if res:
                    targets.append({'host': res['host'], 'schema': res['libelle'], 'port': '', 'service': ''})
        else:
            # --- 1. Gestion des SCHÉMAS via ID ---
            selected_m = self.l_m.curselection()
            for i in selected_m:
                schema_id = self.ids_m[i]
                res = conn_sqlite.execute("""
                    SELECT s.schema, o.host, o.port, o.service 
                    FROM schemas s 
                    JOIN DB_conn o ON s.DB_conn_id = o.id 
                    WHERE s.id = ?""", (schema_id,)).fetchone()
                if res: 
                    targets.append(dict(res))

            # --- 2. Gestion des GRAPPES via ID ---
            selected_g = self.l_g.curselection()
            for i in selected_g:
                db_conn_id = self.ids_g[i]
                # On récupère TOUS les schémas rattachés à cet ID de connexion précis
                res_g = conn_sqlite.execute("""
                    SELECT s.schema, o.host, o.port, o.service 
                    FROM schemas s
                    JOIN DB_conn o ON s.DB_conn_id = o.id
                    WHERE o.id = ?""", (db_conn_id,)).fetchall()
                
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

        sql_raw = sql.strip()
        sql_upper = sql_raw.upper()
        
        # 1. Gestion intelligente du point-virgule
        if not sql_upper.startswith(("BEGIN", "DECLARE")):
            if sql_raw.endswith(';'):
                sql_raw = sql_raw[:-1].strip()
        else:
            if not sql_raw.endswith(';'):
                sql_raw += ';'

        # 2. Confirmation
        is_write_op = sql_upper.startswith(("UPDATE", "INSERT", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "BEGIN", "DECLARE"))
        if is_write_op and not messagebox.askyesno("Confirmation", "Action sensible détectée. Continuer ?", icon='warning'):
            return

        output_dir = os.path.join(os.getcwd(), "result")
        if not os.path.exists(output_dir): os.makedirs(output_dir)

        self._log(f"🚀 Lancement sur {len(targets)} cible(s)...")
        
        for t in targets:
            conn = None
            try:
                schema_name = t.get('schema', 'Base')
                self._log(f"Traitement de {schema_name}...")

                if db_type == "Oracle":
                    dsn = oracledb.makedsn(t['host'], t['port'], service_name=t['service'])
                    conn = oracledb.connect(user=schema_name, password=pwd, dsn=dsn)
                    cursor = conn.cursor()
                    
                    # Activer la capture des logs
                    cursor.callproc("dbms_output.enable", (None,))
                    
                    # EXECUTION
                    cursor.execute(sql_raw)
                    
                    # --- A. GESTION DES RÉSULTATS (CSV) ---
                    # On vérifie si c'est une requête de type SELECT
                    if cursor.description:
                        colnames = [d[0] for d in cursor.description]
                        rows = cursor.fetchall()
                        
                        # On force la création du CSV même si 0 lignes (pour avoir l'entête)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        csv_path = os.path.join(output_dir, f"{schema_name}_{ts}.csv")
                        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                            writer = csv.writer(f, delimiter=';')
                            writer.writerow(colnames)
                            if rows:
                                writer.writerows(rows)
                        self._log(f"✅ CSV généré : {len(rows)} lignes.", "OK")
                    else:
                        # Si pas de SELECT, on valide
                        conn.commit()

                    # --- B. GESTION DES LOGS (DBMS_OUTPUT) ---
                    # On le fait APRÈS le fetchall pour ne pas perturber le flux de données
                    dbms_logs = []
                    status_var = cursor.var(oracledb.NUMBER)
                    line_var = cursor.var(oracledb.STRING)
                    while True:
                        cursor.callproc("dbms_output.get_line", (line_var, status_var))
                        if status_var.getvalue() != 0: break
                        dbms_logs.append(str(line_var.getvalue()))

                    if dbms_logs:
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        log_path = os.path.join(output_dir, f"LOG_{schema_name}_{ts}.log")
                        with open(log_path, 'w', encoding='utf-8') as f:
                            f.write("\n".join(dbms_logs))
                        self._log(f"📄 LOG généré.", "OK")

                    # Si rien n'a été produit (ni CSV ni LOG) pour un UPDATE par ex.
                    if not cursor.description and not dbms_logs:
                        self._log(f"✅ Exécution réussie (COMMIT).", "OK")

                    cursor.close()
                    conn.close()

            except Exception as e:
                self._log(f"❌ ERREUR sur {t.get('schema', 'Base')}: {str(e)}", "ERROR")
                if conn:
                    try: conn.close()
                    except: pass

        self._log("🏁 Fin.")

    def _on_launch_probes(self):
        """Lance les sondes : l'utilisateur est le schéma de la sonde"""
        db_type = self.type_v.get()
        pwd = None
        
        if db_type != "SQLite":
            res = ask_credentials(self)
            if not res: return
            # On récupère juste le mot de passe (res[1] si ta fenêtre renvoie un tuple)
            pwd = res[1] if isinstance(res, (list, tuple)) else res
            if not pwd: return

        selector = ReportSelectorWindow(self, db_type)
        self.wait_window(selector)
        if not selector.result: return

        try:
            engine = ProbeEngine(get_db_connection, logger=self) 
            for pack_id in selector.result:
                # ... gestion nom pack et report_path ...
                
                # On passe uniquement pwd (db_filter et pack_id servent au mapping)
                engine.run_all(db_filter=db_type, pack_id=pack_id, pwd=pwd)
            
            self._log("✔️ Tous les rapports ont été générés.", "SUCCESS")
        except Exception as e:
            self._log(f"CRITIQUE : {str(e)}", "ERROR")

    def _log(self, message, level="INFO"):
        self.log_text.insert("end", f"[{level}] {message}\n")
        self.log_text.see("end")
        self.update_idletasks()

    def _open_bdd(self): self.withdraw(); ParamWindow(self)
    def _open_sondes(self): self.withdraw(); SondeWindow(self)
    
    def _open_compare_window(self):
        self.withdraw()  # On cache la fenêtre principale
        CompareWindow(self) # On lance la nouvelle classe
        
    def _load_files(self, event=None): # Ajout de event=None
        if os.path.exists("requete"):
            files = [f for f in os.listdir("requete") if f.endswith(".sql")]
            self.cb_files["values"] = sorted(files)
            # On ne change la sélection que si rien n'est sélectionné
            if files and not self.file_v.get(): 
                self.cb_files.current(0)

if __name__ == "__main__":
    app = MultiRequetesApp(); app.mainloop()