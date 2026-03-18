import os
import sqlite3
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog

# --- CONFIGURATION ---
DB_PATH = "multirequetes.db"

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

    cur.execute("CREATE TABLE IF NOT EXISTS grappes (id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT UNIQUE)")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS oracle_conn (
        id INTEGER PRIMARY KEY AUTOINCREMENT, host TEXT, service TEXT, sid TEXT, 
        grappe_id INTEGER, type_db TEXT DEFAULT 'Oracle', port TEXT DEFAULT '1521',
        FOREIGN KEY(grappe_id) REFERENCES grappes(id))""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS schemas (
        id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, nom TEXT, schema TEXT, 
        grappe_id INTEGER, oracle_conn_id INTEGER,
        FOREIGN KEY(grappe_id) REFERENCES grappes(id),
        FOREIGN KEY(oracle_conn_id) REFERENCES oracle_conn(id))""")
    
    cur.execute("""CREATE TABLE IF NOT EXISTS sondes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nom_sonde TEXT,
        type_sonde TEXT,
        fonction_id INTEGER,
        type_alerte TEXT,
        type_db TEXT,
        serveur_id INTEGER,
        requete TEXT,
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

        
# --- FENÊTRE : selection grappe SCHEMA pour les sondes---#
class CiblePickerWindow(tk.Toplevel):
    def __init__(self, master, current_targets, db_type):
        super().__init__(master)
        self.title(f"Sélection des cibles - {db_type}")
        
        # On élargit un peu la fenêtre pour les deux colonnes
        w, h = 700, 600 
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.grab_set() 
        
        self.db_type = db_type
        self.result = current_targets 
        self.vars_g = {}
        self.vars_m = {}

        self._build_ui()

    def _build_ui(self):
        main = ttk.Frame(self, padding=10)
        main.pack(fill="both", expand=True)

        # Zone Scrollable
        canvas = tk.Canvas(main)
        scroll = ttk.Scrollbar(main, orient="vertical", command=canvas.yview)
        # Le frame qui contiendra nos deux colonnes
        frame = ttk.Frame(canvas)

        frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=frame, anchor="nw", width=650) # On fixe une largeur pour le contenu
        canvas.configure(yscrollcommand=scroll.set)

        canvas.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        # Configuration des colonnes (poids égal)
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        conn = get_db_connection()

        # --- COLONNE 0 : GRAPPES ---
        col_g = ttk.Frame(frame, padding=5)
        col_g.grid(row=0, column=0, sticky="nsew")
        
        ttk.Label(col_g, text="--- GRAPPES ---", font=("Arial", 10, "bold")).pack(anchor="w", pady=5)
        
        grappes = conn.execute("""SELECT DISTINCT g.id, g.nom FROM grappes g 
                                  JOIN oracle_conn o ON o.grappe_id = g.id 
                                  WHERE o.type_db = ?""", (self.db_type,)).fetchall()
        for g in grappes:
            v = tk.BooleanVar(value=g['id'] in self.result.get('GRAPPE', []))
            self.vars_g[g['id']] = v
            ttk.Checkbutton(col_g, text=g['nom'], variable=v).pack(anchor="w", padx=10, pady=2)

        # --- COLONNE 1 : schemas (Schémas) ---
        col_m = ttk.Frame(frame, padding=5)
        col_m.grid(row=0, column=1, sticky="nsew")
        
        ttk.Label(col_m, text="--- SCHEMAS ---", font=("Arial", 10, "bold")).pack(anchor="w", pady=5)
        
        schemas = conn.execute("""SELECT m.id, m.code,m.schema FROM schemas m 
                                   JOIN oracle_conn o ON m.oracle_conn_id = o.id 
                                   WHERE o.type_db = ?""", (self.db_type,)).fetchall()
        for m in schemas:
            v = tk.BooleanVar(value=m['id'] in self.result.get('SCHEMA', []))
            self.vars_m[m['id']] = v
            ttk.Checkbutton(col_m, text=m['schema'], variable=v).pack(anchor="w", padx=10, pady=2)

        conn.close()

        # Bouton Terminer en bas (en dehors du scroll)
        ttk.Button(self, text="TERMINER LA SÉLECTION", command=self._on_save).pack(fill="x", pady=10, padx=10)

    def _on_save(self):
        self.result = {
            'GRAPPE': [gid for gid, v in self.vars_g.items() if v.get()],
            'SCHEMA': [mid for mid, var in self.vars_m.items() if var.get()]
        }
        self.destroy()
        
        
# --- FENÊTRE : ORGANISER --- #
class OrganiserWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Organisation des Sondes - Vue Dynamique")
        
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
        conn = get_db_connection()
        fonctions = conn.execute("SELECT * FROM sonde_fonctions ORDER BY nom").fetchall()
        
        for i, func in enumerate(fonctions):
            parent = self.col_left if i % 2 == 0 else self.col_right
            frame = tk.LabelFrame(parent, text=f"  {func['nom'].upper()}  ", bg="white", 
                                  font=("Arial", 10, "bold"), fg="#333", padx=10, pady=10)
            frame.pack(fill="x", pady=15, padx=5)
            frame.fonction_id = func['id']
            self.sections[func['id']] = frame
            
            sondes = conn.execute("SELECT * FROM sondes WHERE fonction_id=? ORDER BY ordre ASC", (func['id'],)).fetchall()
            for s in sondes:
                self._create_sonde_widget(frame, s['id'], s['nom_sonde'])
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

# --- FENÊTRE : PARAMÉTRAGE SONDE (VERSION RESTAURÉE + ORGANISER) ---
class SondeWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.title("Paramétrage des Sondes")
        
        # Centrage de la fenêtre
        w, h = 800, 650
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.current_sonde_id = None
        
        # Stockage temporaire des cibles choisies
        self.selected_targets = {'GRAPPE': [], 'SCHEMA': []}

        # Variables
        self.nom_sonde_var = tk.StringVar()
        self.type_sonde_var = tk.StringVar(value="Fonctionnelle")
        self.fonction_var = tk.StringVar()
        self.alerte_var = tk.StringVar(value="Majeur")
        self.db_type_var = tk.StringVar(value="Oracle")
        self.search_sonde_var = tk.StringVar()

        self._build_ui()
        self._load_fonctions()
        self._load_liste_sondes()
        self.grab_set()

    def _on_close(self):
        self.master.deiconify()
        self.destroy()

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

        ttk.Label(grid, text="Nom Sonde * :").grid(row=0, column=0, sticky="w")
        ttk.Entry(grid, textvariable=self.nom_sonde_var).grid(row=0, column=1, sticky="ew", pady=5)

        ttk.Label(grid, text="Type :").grid(row=1, column=0, sticky="w")
        ttk.Combobox(grid, textvariable=self.type_sonde_var, values=("Fonctionnelle", "Technique"), state="readonly").grid(row=1, column=1, sticky="ew", pady=2)

        ttk.Label(grid, text="Fonction * :").grid(row=2, column=0, sticky="w")
        f_func = ttk.Frame(grid)
        f_func.grid(row=2, column=1, sticky="ew")
        self.cb_fonctions = ttk.Combobox(f_func, textvariable=self.fonction_var, state="readonly")
        self.cb_fonctions.pack(side="left", fill="x", expand=True)
        ttk.Button(f_func, text="+", width=3, command=self._add_fonction_popup).pack(side="left", padx=2)

        ttk.Label(grid, text="Type BDD :").grid(row=3, column=0, sticky="w")
        cb_db = ttk.Combobox(grid, textvariable=self.db_type_var, values=("Oracle", "MySQL", "PostgreSQL"), state="readonly")
        cb_db.grid(row=3, column=1, sticky="ew", pady=2)
        # On réinitialise les cibles si on change de type de BDD car elles ne sont plus valides
        cb_db.bind("<<ComboboxSelected>>", lambda e: self._reset_targets())

        ttk.Label(grid, text="Cibles :").grid(row=4, column=0, sticky="w")
        self.btn_cible = ttk.Button(grid, text="🎯 Définir les cibles (0 sélectionnées)", 
                                   command=self._open_cible_picker)
        self.btn_cible.grid(row=4, column=1, sticky="ew", pady=5)

        grid.columnconfigure(1, weight=1)

        ttk.Label(container, text="Requête SQL * :").pack(anchor="w", pady=(10, 0))
        self.txt_req = tk.Text(container, height=10)
        self.txt_req.pack(fill="x", pady=5)

        f_btns = ttk.Frame(container)
        f_btns.pack(fill="x", pady=10)
        ttk.Button(f_btns, text="ENREGISTRER", command=self._save_sonde).pack(side="left", fill="x", expand=True, padx=2)
        ttk.Button(f_btns, text="ORGANISER", command=lambda: OrganiserWindow(self)).pack(side="left", fill="x", expand=True, padx=2)
        tk.Button(f_btns, text="SUPPRIMER", bg="#ffcccc", fg="red", command=self._delete_sonde_active).pack(side="left", fill="x", expand=True, padx=2)

    def _open_cible_picker(self):
        # Ouvre la fenêtre de sélection (on lui passe les cibles actuelles et le type de BDD)
        picker = CiblePickerWindow(self, self.selected_targets, self.db_type_var.get())
        self.wait_window(picker) 
        
        # Récupération du résultat après fermeture
        self.selected_targets = picker.result
        self._update_target_button_text()

    def _update_target_button_text(self):
        count = len(self.selected_targets['GRAPPE']) + len(self.selected_targets['SCHEMA'])
        self.btn_cible.config(text=f"🎯 Définir les cibles ({count} sélectionnées)")

    def _reset_targets(self):
        self.selected_targets = {'GRAPPE': [], 'SCHEMA': []}
        self._update_target_button_text()

    def _reset_form(self):
        self.current_sonde_id = None
        self.nom_sonde_var.set("")
        self.fonction_var.set("")
        self.search_sonde_var.set("")
        self.cb_search.set("")
        self.txt_req.delete("1.0", "end")
        self._reset_targets()

    def _load_liste_sondes(self):
        conn = get_db_connection()
        res = conn.execute("SELECT id, nom_sonde FROM sondes ORDER BY id DESC").fetchall()
        self.cb_search["values"] = [f"{r['id']} | {r['nom_sonde']}" for r in res]
        conn.close()

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
            
            # Chargement de la fonction
            f = conn.execute("SELECT nom FROM sonde_fonctions WHERE id=?", (s['fonction_id'],)).fetchone()
            if f: self.fonction_var.set(f['nom'])
            
            # Chargement de la requête
            self.txt_req.delete("1.0", "end")
            self.txt_req.insert("1.0", s['requete'])

            # CHARGEMENT DES CIBLES ENREGISTRÉES
            self.selected_targets = {'GRAPPE': [], 'SCHEMA': []}
            cibles = conn.execute("SELECT type_cible, cible_id FROM sonde_cibles WHERE sonde_id=?", (sid,)).fetchall()
            for c in cibles:
                self.selected_targets[c['type_cible']].append(c['cible_id'])
            
            self._update_target_button_text()
        conn.close()

    def _save_sonde(self):
        nom = self.nom_sonde_var.get().strip()
        f_nom = self.fonction_var.get()
        req = self.txt_req.get("1.0", "end-1c").strip()
        db_type = self.db_type_var.get()
        
        if not all([nom, f_nom, req]): 
            return messagebox.showwarning("Erreur", "Veuillez remplir le nom, la fonction et la requête.")
        
        if not (self.selected_targets['GRAPPE'] or self.selected_targets['SCHEMA']):
            return messagebox.showwarning("Erreur", "Veuillez sélectionner au moins une cible (SCHEMA ou Grappe).")

        conn = get_db_connection()
        try:
            # Récupérer l'ID de la fonction
            f_res = conn.execute("SELECT id FROM sonde_fonctions WHERE nom=?", (f_nom,)).fetchone()
            if not f_res: return messagebox.showerror("Erreur", "Fonction inconnue.")
            f_id = f_res['id']

            if self.current_sonde_id:
                # MISE À JOUR SONDE
                conn.execute("""UPDATE sondes SET nom_sonde=?, fonction_id=?, requete=?, type_db=? 
                             WHERE id=?""", (nom, f_id, req, db_type, self.current_sonde_id))
                sid = self.current_sonde_id
                # On nettoie les anciennes cibles avant de réinsérer
                conn.execute("DELETE FROM sonde_cibles WHERE sonde_id=?", (sid,))
            else:
                # NOUVELLE SONDE
                cur = conn.cursor()
                cur.execute("""INSERT INTO sondes (nom_sonde, fonction_id, requete, type_db) 
                            VALUES (?,?,?,?)""", (nom, f_id, req, db_type))
                sid = cur.lastrowid

            # INSERTION DES CIBLES MULTIPLES
            for gid in self.selected_targets['GRAPPE']:
                conn.execute("INSERT INTO sonde_cibles (sonde_id, type_cible, cible_id) VALUES (?, 'GRAPPE', ?)", (sid, gid))
            
            for mid in self.selected_targets['SCHEMA']:
                conn.execute("INSERT INTO sonde_cibles (sonde_id, type_cible, cible_id) VALUES (?, 'SCHEMA', ?)", (sid, mid))

            conn.commit()
            messagebox.showinfo("Succès", "Sonde et cibles enregistrées avec succès.")
            self._reset_form()
            self._load_liste_sondes()
        except Exception as e:
            messagebox.showerror("Erreur de sauvegarde", str(e))
        finally:
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

    def _load_fonctions(self):
        conn = get_db_connection()
        res = conn.execute("SELECT nom FROM sonde_fonctions ORDER BY nom").fetchall()
        self.cb_fonctions["values"] = [r["nom"] for r in res]
        conn.close()

    def _delete_sonde_active(self):
        if self.current_sonde_id and messagebox.askyesno("Confirmation", "Supprimer définitivement cette sonde et ses cibles ?"):
            conn = get_db_connection()
            # Supprimer les cibles d'abord (intégrité)
            conn.execute("DELETE FROM sonde_cibles WHERE sonde_id=?", (self.current_sonde_id,))
            # Supprimer la sonde
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
        self.geometry("800x600")
        w, h = 800, 600
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        self.geometry(f"{w}x{h}+{x}+{y}")
        
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        
        self.db_type_var = tk.StringVar(value="Oracle")
        self.host_var = tk.StringVar(); self.port_var = tk.StringVar(value="1521"); self.service_var = tk.StringVar(); self.oracle_grappe_var = tk.StringVar()
        self.mag_code_var = tk.StringVar(); self.mag_schema_var = tk.StringVar(); self.mag_oracle_var = tk.StringVar()
        self._current_oracle_id = None; self._current_SCHEMA_id = None

        self._build_ui(); self._load_data_filtered(); self.grab_set()

    def _on_close(self):
        self.master.deiconify(); self.master._load_data(); self.destroy()

    def _build_ui(self):
        container = ttk.Frame(self, padding=10); container.pack(fill="both", expand=True)
        # SGBD
        f_type = ttk.LabelFrame(container, text="1. SGBD", padding=5); f_type.pack(fill="x", pady=5)
        cb = ttk.Combobox(f_type, textvariable=self.db_type_var, values=("Oracle", "MySQL", "PostgreSQL"), state="readonly")
        cb.pack(fill="x"); cb.bind("<<ComboboxSelected>>", lambda e: self._load_data_filtered())
        # Serveurs
        f_net = ttk.LabelFrame(container, text="2. Serveurs", padding=5); f_net.pack(fill="x")
        self.tree_o = ttk.Treeview(f_net, columns=("h", "p", "s", "g"), show="headings", height=5)
        for c, t in [("h", "Hôte"), ("p", "Port"), ("s", "Service"), ("g", "Grappe")]: self.tree_o.heading(c, text=t)
        self.tree_o.pack(side="left", fill="both", expand=True); self.tree_o.bind("<<TreeviewSelect>>", self._on_ora_sel)
        form_n = ttk.Frame(f_net, padding=5); form_n.pack(side="right")
        ttk.Entry(form_n, textvariable=self.host_var).pack(); ttk.Entry(form_n, textvariable=self.port_var).pack(); ttk.Entry(form_n, textvariable=self.service_var).pack()
        self.cb_g = ttk.Combobox(form_n, textvariable=self.oracle_grappe_var); self.cb_g.pack()
        ttk.Button(form_n, text="Sauver", command=self._save_o).pack(fill="x")
        # schemas
        f_mag = ttk.LabelFrame(container, text="3. schemas", padding=5); f_mag.pack(fill="both", expand=True)
        self.tree_m = ttk.Treeview(f_mag, columns=("c", "s", "g"), show="headings")
        for c, t in [("c", "Code"), ("s", "Schéma"), ("g", "Grappe")]: self.tree_m.heading(c, text=t)
        self.tree_m.pack(side="left", fill="both", expand=True); self.tree_m.bind("<<TreeviewSelect>>", self._on_mag_sel)
        form_m = ttk.Frame(f_mag, padding=5); form_m.pack(side="right")
        ttk.Entry(form_m, textvariable=self.mag_code_var).pack(); ttk.Entry(form_m, textvariable=self.mag_schema_var).pack()
        self.cb_o = ttk.Combobox(form_m, textvariable=self.mag_oracle_var, state="readonly"); self.cb_o.pack()
        ttk.Button(form_m, text="Sauver", command=self._save_m).pack(fill="x")

    def _load_data_filtered(self):
        conn = get_db_connection(); t = self.db_type_var.get()
        self.tree_o.delete(*self.tree_o.get_children())
        res = conn.execute("SELECT o.*, g.nom as gnom FROM oracle_conn o LEFT JOIN grappes g ON o.grappe_id = g.id WHERE o.type_db=?", (t,)).fetchall()
        self.cb_o["values"] = [f"{r['id']} | {r['host']}" for r in res]
        for r in res: self.tree_o.insert("", "end", iid=r["id"], values=(r["host"], r["port"], r["service"], r["gnom"]))
        self.tree_m.delete(*self.tree_m.get_children())
        res_m = conn.execute("SELECT m.*, g.nom as gnom FROM schemas m JOIN oracle_conn o ON m.oracle_conn_id = o.id LEFT JOIN grappes g ON m.grappe_id = g.id WHERE o.type_db=?", (t,)).fetchall()
        for r in res_m: self.tree_m.insert("", "end", iid=r["id"], values=(r["code"], r["schema"], r["gnom"]))
        gres = conn.execute("SELECT nom FROM grappes").fetchall(); self.cb_g["values"] = [r["nom"] for r in gres]; conn.close()

    def _save_o(self):
        conn = get_db_connection(); cur = conn.cursor()
        g = self.oracle_grappe_var.get().strip() or "SANS GRAPPE"
        cur.execute("INSERT OR IGNORE INTO grappes (nom) VALUES (?)", (g,))
        gid = cur.execute("SELECT id FROM grappes WHERE nom=?", (g,)).fetchone()["id"]
        if self._current_oracle_id: cur.execute("UPDATE oracle_conn SET host=?, port=?, service=?, grappe_id=? WHERE id=?", (self.host_var.get(), self.port_var.get(), self.service_var.get(), gid, self._current_oracle_id))
        else: cur.execute("INSERT INTO oracle_conn (type_db, host, port, service, grappe_id) VALUES (?,?,?,?,?)", (self.db_type_var.get(), self.host_var.get(), self.port_var.get(), self.service_var.get(), gid))
        conn.commit(); conn.close(); self._load_data_filtered()

    def _save_m(self):
        if not self.cb_o.get(): return
        oid = self.cb_o.get().split(" | ")[0]
        conn = get_db_connection(); cur = conn.cursor(); gid = cur.execute("SELECT grappe_id FROM oracle_conn WHERE id=?", (oid,)).fetchone()["grappe_id"]
        if self._current_SCHEMA_id: cur.execute("UPDATE schemas SET code=?, schema=?, oracle_conn_id=?, grappe_id=? WHERE id=?", (self.mag_code_var.get(), self.mag_schema_var.get(), oid, gid, self._current_SCHEMA_id))
        else: cur.execute("INSERT INTO schemas (code, schema, oracle_conn_id, grappe_id) VALUES (?,?,?,?)", (self.mag_code_var.get(), self.mag_schema_var.get(), oid, gid))
        conn.commit(); conn.close(); self._load_data_filtered()

    def _on_ora_sel(self, e):
        s = self.tree_o.selection()
        if s: self._current_oracle_id = s[0]; v = self.tree_o.item(s[0], "values"); self.host_var.set(v[0]); self.port_var.set(v[1]); self.service_var.set(v[2]); self.oracle_grappe_var.set(v[3])

    def _on_mag_sel(self, e):
        s = self.tree_m.selection()
        if s: self._current_SCHEMA_id = s[0]; v = self.tree_m.item(s[0], "values"); self.mag_code_var.set(v[0]); self.mag_schema_var.set(v[1])

# --- APPLICATION PRINCIPALE ---
class MultiRequetesApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.geometry("800x600")
        self.title("Multirequete")
        w, h = 800, 600
        ws = self.winfo_screenwidth()
        hs = self.winfo_screenheight()
        x = (ws // 2) - (w // 2)
        y = (hs // 2) - (h // 2)
        
        # 3. ON APPLIQUE LA GÉOMÉTRIE
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.grab_set()
        
        setup_environment()
        init_db()
        
        # Configuration de la grille pour l'adaptativité
        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1) # Le corps (body) est extensible

        self.type_v = tk.StringVar(value="Oracle")
        self.file_v = tk.StringVar() 
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        # TOP MENU (Fixe en haut)
        top = ttk.Frame(self, padding=10)
        top.grid(row=0, column=0, sticky="ew")
        
        ttk.Label(top, text="SGBD :").pack(side="left")
        cb = ttk.Combobox(top, textvariable=self.type_v, values=("Oracle", "MySQL", "PostgreSQL"), state="readonly")
        cb.pack(side="left", padx=5)
        cb.bind("<<ComboboxSelected>>", lambda e: self._load_data())
        #----liste requete  
        ttk.Label(top, text="Fichier SQL :").pack(side="left", padx=(15, 0))
        self.cb_files = ttk.Combobox(top, textvariable=self.file_v, state="readonly", width=35)
        self.cb_files.pack(side="left", padx=5)
        
        ttk.Button(top, text="Paramétrage BDD", command=self._open_bdd).pack(side="right", padx=2)
        ttk.Button(top, text="Paramétrage Sondes", command=self._open_sondes).pack(side="right", padx=2)

        # BODY (Extensible)
        body = ttk.Frame(self, padding=10)
        body.grid(row=1, column=0, sticky="nsew")
        body.grid_columnconfigure(0, weight=1) # Col schemas
        body.grid_columnconfigure(1, weight=1) # Col Grappes
        body.grid_rowconfigure(0, weight=1)

        # schemas
        m_col = ttk.LabelFrame(body, text=" schemas / Serveurs ", padding=5)
        m_col.grid(row=0, column=0, sticky="nsew", padx=5)
        self.l_m = tk.Listbox(m_col, font=("Segoe UI", 10))
        self.l_m.pack(fill="both", expand=True)
        
        # Grappes
        g_col = ttk.LabelFrame(body, text=" Grappes ", padding=5)
        g_col.grid(row=0, column=1, sticky="nsew", padx=5)
        self.l_g = tk.Listbox(g_col, font=("Segoe UI", 10))
        self.l_g.pack(fill="both", expand=True)

        # BOTTOM (Fixe en bas)
        bottom = ttk.Frame(self, padding=10)
        bottom.grid(row=2, column=0, sticky="ew")
        ttk.Button(bottom, text="Lancer les requêtes").pack(side="right")

    def _open_bdd(self): self.withdraw(); ParamWindow(self)
    def _open_sondes(self): self.withdraw(); SondeWindow(self)

    
    def _load_files(self):
        """Scanne le dossier requete et remplit la liste en haut"""
        if os.path.exists("requete"):
            files = [f for f in os.listdir("requete") if f.endswith(".sql")]
            self.cb_files["values"] = sorted(files)
            if files and not self.file_v.get():
                self.cb_files.current(0)

    def _load_data(self):
        conn = get_db_connection(); t = self.type_v.get()
        self.l_m.delete(0, "end")
        res = conn.execute("SELECT m.*, o.host FROM schemas m JOIN oracle_conn o ON m.oracle_conn_id = o.id WHERE o.type_db=?", (t,)).fetchall()
        for r in res: self.l_m.insert("end", f"{r['code']} ({r['host']})")
        self.l_g.delete(0, "end")
        res_g = conn.execute("SELECT DISTINCT g.nom FROM grappes g JOIN oracle_conn o ON o.grappe_id = g.id WHERE o.type_db=?", (t,)).fetchall()
        for r in res_g: self.l_g.insert("end", r['nom'])
        conn.close()
        self._load_files()

if __name__ == "__main__":
    app = MultiRequetesApp(); app.mainloop()