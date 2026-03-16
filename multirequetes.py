import os
import sqlite3
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
import glob
import ttkthemes
import oracledb

DB_PATH = "multirequetes.db"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS oracle_conn (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host TEXT,
            service TEXT,
            sid TEXT,
            grappe_id INTEGER,
            FOREIGN KEY(grappe_id) REFERENCES grappes(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS grappes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT UNIQUE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS magasins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            nom TEXT,
            grappe_id INTEGER,
            oracle_conn_id INTEGER,
            FOREIGN KEY(grappe_id) REFERENCES grappes(id),
            FOREIGN KEY(oracle_conn_id) REFERENCES oracle_conn(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS requetes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nom TEXT UNIQUE,
            chemin TEXT
        )
        """
    )

    # Ensure columns exist for backward compatibility
    cur.execute("PRAGMA table_info(magasins)")
    cols = [row[1] for row in cur.fetchall()]
    if "oracle_conn_id" not in cols:
        cur.execute("ALTER TABLE magasins ADD COLUMN oracle_conn_id INTEGER")

    cur.execute("PRAGMA table_info(oracle_conn)")
    cols = [row[1] for row in cur.fetchall()]
    if "grappe_id" not in cols:
        cur.execute("ALTER TABLE oracle_conn ADD COLUMN grappe_id INTEGER")

    conn.commit()
    conn.close()


class ParamWindow(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.title("Paramétrage")
        self.geometry("1000x660")
        self.minsize(900, 600)
        # Appliquer le même thème
        self.style = ttkthemes.ThemedStyle(self)
        self.style.set_theme("adapta")
        self._current_magasin_id = None
        self._current_oracle_id = None
        self._build_ui()
        self._load_data()
        # Rendre la fenêtre modale : désactiver la fenêtre principale
        self.grab_set()
        self.wait_window(self)
        self.master = master
        self.mag_list = tk.Listbox(master)
        self.mag_list.pack()

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        # --- Connexions Oracle ---
        oracle_frame = ttk.LabelFrame(root, text="Connexions Oracle")
        oracle_frame.pack(fill="x", padx=5, pady=5)

        # Liste des connexions
        self.oracle_tree = ttk.Treeview(
            oracle_frame, columns=("host", "service", "sid", "grappe"), show="headings", height=6
        )
        self.oracle_tree.heading("host", text="Hôte")
        self.oracle_tree.heading("service", text="Service")
        self.oracle_tree.heading("sid", text="SID")
        self.oracle_tree.heading("grappe", text="Grappe")
        self.oracle_tree.pack(fill="both", expand=True, side="left", padx=(6, 0), pady=6)
        self.oracle_tree.bind("<<TreeviewSelect>>", self._on_oracle_select)

        oracle_scroll = ttk.Scrollbar(oracle_frame, orient="vertical", command=self.oracle_tree.yview)
        oracle_scroll.pack(side="left", fill="y", pady=6)
        self.oracle_tree.configure(yscrollcommand=oracle_scroll.set)

        oracle_form = ttk.Frame(oracle_frame)
        oracle_form.pack(side="left", fill="y", padx=10, pady=6)

        ttk.Label(oracle_form, text="Hôte :").grid(row=0, column=0, sticky="e", padx=4, pady=4)
        self.host_var = tk.StringVar()
        ttk.Entry(oracle_form, textvariable=self.host_var, width=30).grid(row=0, column=1, padx=4, pady=4)

        ttk.Label(oracle_form, text="Service :").grid(row=1, column=0, sticky="e", padx=4, pady=4)
        self.service_var = tk.StringVar()
        ttk.Entry(oracle_form, textvariable=self.service_var, width=30).grid(row=1, column=1, padx=4, pady=4)

        ttk.Label(oracle_form, text="SID :").grid(row=2, column=0, sticky="e", padx=4, pady=4)
        self.sid_var = tk.StringVar()
        ttk.Entry(oracle_form, textvariable=self.sid_var, width=30).grid(row=2, column=1, padx=4, pady=4)

        ttk.Label(oracle_form, text="Grappe :").grid(row=3, column=0, sticky="e", padx=4, pady=4)
        self.oracle_grappe_var = tk.StringVar()
        self.oracle_grappe_combo = ttk.Combobox(oracle_form, textvariable=self.oracle_grappe_var, width=28)
        self.oracle_grappe_combo.grid(row=3, column=1, padx=4, pady=4)

        ttk.Button(oracle_form, text="Sauvegarder", command=self._save_oracle).grid(row=4, column=0, columnspan=2, pady=10)
        ttk.Button(oracle_form, text="Supprimer", command=self._delete_oracle).grid(row=5, column=0, columnspan=2, pady=4)

        # --- Magasin / Grappe ---
        magasin_frame = ttk.LabelFrame(root, text="Magasin / Grappe")
        magasin_frame.pack(fill="both", expand=True, padx=5, pady=5)

        left = ttk.Frame(magasin_frame)
        left.pack(side="left", fill="both", expand=True, padx=(0, 5), pady=10)

        self.magasins_tree = ttk.Treeview(
            left, columns=("code", "nom", "grappe", "oracle"), show="headings", height=16
        )
        self.magasins_tree.heading("code", text="Code")
        self.magasins_tree.heading("nom", text="Nom")
        self.magasins_tree.heading("grappe", text="Grappe")
        self.magasins_tree.heading("oracle", text="Connexion Oracle")
        self.magasins_tree.pack(fill="both", expand=True, side="left")
        self.magasins_tree.bind("<<TreeviewSelect>>", self._on_magasin_select)

        magaz_scroll = ttk.Scrollbar(left, orient="vertical", command=self.magasins_tree.yview)
        magaz_scroll.pack(side="left", fill="y")
        self.magasins_tree.configure(yscrollcommand=magaz_scroll.set)

        right = ttk.Frame(magasin_frame)
        right.pack(side="left", fill="y", padx=(5, 0), pady=10)

        ttk.Label(right, text="Code magasin :").pack(anchor="w", pady=(4, 0))
        self.mag_code_var = tk.StringVar()
        ttk.Entry(right, textvariable=self.mag_code_var, width=30).pack(pady=4)

        ttk.Label(right, text="Nom magasin :").pack(anchor="w", pady=(4, 0))
        self.mag_nom_var = tk.StringVar()
        ttk.Entry(right, textvariable=self.mag_nom_var, width=30).pack(pady=4)

        ttk.Label(right, text="Grappe :").pack(anchor="w", pady=(4, 0))
        self.mag_grappe_var = tk.StringVar()
        self.mag_grappe_entry = ttk.Entry(right, textvariable=self.mag_grappe_var, width=30, state="readonly")
        self.mag_grappe_entry.pack(pady=4)

        ttk.Label(right, text="Connexion Oracle :").pack(anchor="w", pady=(10, 0))
        self.mag_oracle_var = tk.StringVar()
        self.mag_oracle_combo = ttk.Combobox(right, textvariable=self.mag_oracle_var, width=28)
        self.mag_oracle_combo.pack(pady=4)
        self.mag_oracle_combo.bind('<<ComboboxSelected>>', self._on_mag_oracle_select)

        ttk.Button(right, text="Ajouter / Mettre à jour", command=self._save_magasin).pack(fill="x", pady=4)
        ttk.Button(right, text="Supprimer", command=self._delete_magasin).pack(fill="x")

        ttk.Separator(root, orient="horizontal").pack(fill="x", pady=6)
        ttk.Button(root, text="Quitter", command=self.destroy).pack(side="right", padx=10, pady=6)

        self._status_label = ttk.Label(root, text="", foreground="green")
        self._status_label.pack(fill="x", padx=10, pady=(4, 0))

    def _load_data(self):
        conn = get_db_connection()
        cur = conn.cursor()

        # Oracle
        cur.execute(
            "SELECT o.id, o.host, o.service, o.sid, g.nom AS grappe "
            "FROM oracle_conn o LEFT JOIN grappes g ON o.grappe_id = g.id "
            "ORDER BY o.id DESC"
        )
        oracles = cur.fetchall()
        self.oracle_tree.delete(*self.oracle_tree.get_children())
        for o in oracles:
            self.oracle_tree.insert(
                "",
                "end",
                iid=o["id"],
                values=(o["host"], o["service"], o["sid"], o["grappe"] or ""),
            )

        # Configurer la liste de connexions pour le choix magasin
        self.mag_oracle_combo["values"] = [
            f"{o['host']}|{o['service']}|{o['sid']}|{o['grappe'] or ''}" for o in oracles
        ]

        # Grappe values
        cur.execute("SELECT nom FROM grappes ORDER BY nom")
        grappes = [r["nom"] for r in cur.fetchall()]
        self.oracle_grappe_combo["values"] = grappes
        # Magasins
        for item in self.magasins_tree.get_children():
            self.magasins_tree.delete(item)
        cur.execute(
            "SELECT m.id, m.code, m.nom, o.id AS oracle_id, o.host, o.service, o.sid, g.nom AS grappe "
            "FROM magasins m "
            "LEFT JOIN oracle_conn o ON m.oracle_conn_id = o.id "
            "LEFT JOIN grappes g ON o.grappe_id = g.id "
            "ORDER BY m.code"
        )
        for m in cur.fetchall():
            oracle_text = ""
            if m["oracle_id"]:
                oracle_text = f"{m['host']}|{m['service']}|{m['sid']}|{m['grappe'] or ''}"
            self.magasins_tree.insert(
                "",
                "end",
                iid=m["id"],
                values=(m["code"], m["nom"], m["grappe"] or "", oracle_text),
            )

        conn.close()

    def _save_oracle(self):
        conn = get_db_connection()
        cur = conn.cursor()

        grappe = self.oracle_grappe_var.get().strip()
        grappe_id = None
        if grappe:
            cur.execute("SELECT id FROM grappes WHERE nom = ?", (grappe,))
            row = cur.fetchone()
            if row:
                grappe_id = row["id"]
            else:
                cur.execute("INSERT INTO grappes (nom) VALUES (?)", (grappe,))
                grappe_id = cur.lastrowid

        if hasattr(self, "_current_oracle_id") and self._current_oracle_id:
            cur.execute(
                "UPDATE oracle_conn SET host = ?, service = ?, sid = ?, grappe_id = ? WHERE id = ?",
                (
                    self.host_var.get(),
                    self.service_var.get(),
                    self.sid_var.get(),
                    grappe_id,
                    self._current_oracle_id,
                ),
            )
            saved_id = self._current_oracle_id
        else:
            cur.execute(
                "INSERT INTO oracle_conn (host, service, sid, grappe_id) VALUES (?, ?, ?, ?)",
                (self.host_var.get(), self.service_var.get(), self.sid_var.get(), grappe_id),
            )
            saved_id = cur.lastrowid

        conn.commit()
        conn.close()

        # Recharger les données et sélectionner l'enregistrement modifié/créé
        self._load_data()
        if saved_id:
            try:
                self.oracle_tree.selection_set(saved_id)
                self.oracle_tree.see(saved_id)
                self._current_oracle_id = saved_id
            except Exception:
                self._current_oracle_id = None

        self._status_label.configure(text="Paramètres Oracle enregistrés.")

    def _delete_oracle(self):
        selection = self.oracle_tree.selection()
        if not selection:
            return
        oid = selection[0]
        resp = messagebox.askyesno(
            "Supprimer", "Supprimer cette connexion Oracle et dissocier tous les magasins ?"
        )
        if not resp:
            return

        conn = get_db_connection()
        cur = conn.cursor()
        # dissocier des magasins
        cur.execute("UPDATE magasins SET oracle_conn_id = NULL WHERE oracle_conn_id = ?", (oid,))
        # supprimer la connexion
        cur.execute("DELETE FROM oracle_conn WHERE id = ?", (oid,))
        conn.commit()
        conn.close()
        self._load_data()
        self._status_label.configure(text="Connexion Oracle supprimée.")

    def _on_oracle_select(self, event):
        selection = self.oracle_tree.selection()
        if not selection:
            self._current_oracle_id = None
            return
        oid = selection[0]
        self._current_oracle_id = oid
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT o.host, o.service, o.sid, g.nom AS grappe "
            "FROM oracle_conn o LEFT JOIN grappes g ON o.grappe_id = g.id "
            "WHERE o.id = ?",
            (oid,),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            self.host_var.set(row["host"])
            self.service_var.set(row["service"])
            self.sid_var.set(row["sid"])
            self.oracle_grappe_var.set(row["grappe"] or "")

    def _on_magasin_select(self, event):
        selection = self.magasins_tree.selection()
        if not selection:
            self._current_magasin_id = None
            return
        self._current_magasin_id = selection[0]
        values = self.magasins_tree.item(self._current_magasin_id, "values")
        self.mag_code_var.set(values[0])
        self.mag_nom_var.set(values[1])

        # Récupérer la connexion Oracle et la grappe associée
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT o.host, o.service, o.sid, g.nom AS grappe "
            "FROM magasins m "
            "LEFT JOIN oracle_conn o ON m.oracle_conn_id = o.id "
            "LEFT JOIN grappes g ON o.grappe_id = g.id "
            "WHERE m.id = ?",
            (self._current_magasin_id,),
        )
        row = cur.fetchone()
        conn.close()

        if row and row["host"]:
            self.mag_oracle_var.set(f"{row['host']}|{row['service']}|{row['sid']}|{row['grappe'] or ''}")
            self.mag_grappe_var.set(row["grappe"] or "")
        else:
            self.mag_oracle_var.set("")
            self.mag_grappe_var.set("")

    def _on_mag_oracle_select(self, event):
        oracle_selection = self.mag_oracle_var.get().strip()
        if not oracle_selection:
            self.mag_grappe_var.set("")
            return

        parts = oracle_selection.split("|", 3)
        host, service, sid = parts[0], parts[1], parts[2] if len(parts) > 2 else ""
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT g.nom AS grappe "
            "FROM oracle_conn o "
            "LEFT JOIN grappes g ON o.grappe_id = g.id "
            "WHERE o.host = ? AND o.service = ? AND o.sid = ?",
            (host, service, sid),
        )
        row = cur.fetchone()
        conn.close()
        self.mag_grappe_var.set(row["grappe"] if row and row["grappe"] else "")

    def _save_magasin(self):
        code = self.mag_code_var.get().strip()
        nom = self.mag_nom_var.get().strip()
        if not code or not nom:
            messagebox.showwarning("Paramétrage", "Veuillez renseigner code et nom.")
            return

        conn = get_db_connection()
        cur = conn.cursor()

        oracle_selection = self.mag_oracle_var.get().strip()
        oracle_id = None
        grappe_id = None
        if oracle_selection:
            parts = oracle_selection.split("|", 3)
            host, service, sid = parts[0], parts[1], parts[2] if len(parts) > 2 else ""
            cur.execute(
                "SELECT id, grappe_id FROM oracle_conn WHERE host = ? AND service = ? AND sid = ?",
                (host, service, sid),
            )
            o = cur.fetchone()
            if o:
                oracle_id = o["id"]
                grappe_id = o["grappe_id"]

        if self._current_magasin_id:
            cur.execute(
                "UPDATE magasins SET code = ?, nom = ?, grappe_id = ?, oracle_conn_id = ? WHERE id = ?",
                (code, nom, grappe_id, oracle_id, self._current_magasin_id),
            )
        else:
            try:
                cur.execute(
                    "INSERT INTO magasins (code, nom, grappe_id, oracle_conn_id) VALUES (?, ?, ?, ?)",
                    (code, nom, grappe_id, oracle_id),
                )
            except sqlite3.IntegrityError:
                messagebox.showwarning("Paramétrage", "Ce magasin existe déjà.")
                conn.close()
                return

        conn.commit()
        conn.close()
        self._current_magasin_id = None
        self.mag_code_var.set("")
        self.mag_nom_var.set("")
        self._load_data()
        self._status_label.configure(text="Magasin enregistré.")

    def _delete_magasin(self):
        if not self._current_magasin_id:
            return
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM magasins WHERE id = ?", (self._current_magasin_id,))
        conn.commit()
        conn.close()
        self._current_magasin_id = None
        self.mag_code_var.set("")
        self.mag_nom_var.set("")
        self.mag_grappe_var.set("")
        self._load_data()
        self._status_label.configure(text="Magasin supprimé.")


class MultiRequetesApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Multi-Requêtes")
        self.geometry("920x640")
        # Appliquer un thème moderne
        self.style = ttkthemes.ThemedStyle(self)
        self.style.set_theme("adapta")  # Thème adapta pour un look moderne
        init_db()
        self._build_ui()
        self._load_data()

    def _build_ui(self):
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Requête SQL à exécuter :").grid(row=0, column=0, sticky="w")
        self.req_combo = ttk.Combobox(top, values=[], width=60)
        self.req_combo.grid(row=0, column=1, sticky="ew", padx=5)

        self.btn_param = ttk.Button(top, text="Paramétrer", command=self._open_param)
        self.btn_param.grid(row=0, column=2, padx=4)

        self.btn_execute = ttk.Button(top, text="Exécuter", command=self._execute)
        self.btn_execute.grid(row=0, column=3, padx=4)

        top.columnconfigure(1, weight=1)

        body = ttk.Frame(self, padding=10)
        body.pack(fill="both", expand=True)

        self.mag_list = tk.Listbox(body, selectmode="extended", height=15)
        self.mag_list.pack(side="left", fill="both", expand=True)
        self.mag_scroll = ttk.Scrollbar(body, orient="vertical", command=self.mag_list.yview)
        self.mag_scroll.pack(side="left", fill="y")
        self.mag_list.configure(yscrollcommand=self.mag_scroll.set)

        self.grappe_list = tk.Listbox(body, selectmode="extended", height=15)
        self.grappe_list.pack(side="left", fill="both", expand=True, padx=(10, 0))
        self.grappe_scroll = ttk.Scrollbar(body, orient="vertical", command=self.grappe_list.yview)
        self.grappe_scroll.pack(side="left", fill="y")
        self.grappe_list.configure(yscrollcommand=self.grappe_scroll.set)

        self.status_text = tk.Text(self, height=6, wrap="word")
        self.status_text.pack(fill="x", padx=10, pady=10)
        self.status_text.insert("end", "Prêt. Cliquez sur Paramétrer pour initialiser les données.\n")
        self.status_text.configure(state="disabled")

    def _log(self, msg: str):
        self.status_text.configure(state="normal")
        self.status_text.insert("end", msg + "\n")
        self.status_text.see("end")
        self.status_text.configure(state="disabled")

    def _load_data(self):
        conn = get_db_connection()
        cur = conn.cursor()

        # Scan du répertoire requete pour les fichiers .sql
        requete_dir = "requete"
        if os.path.exists(requete_dir):
            sql_files = glob.glob(os.path.join(requete_dir, "*.sql"))
            self.req_combo["values"] = [os.path.basename(f) for f in sql_files]
            self._requete_paths = {os.path.basename(f): f for f in sql_files}
        else:
            self.req_combo["values"] = []
            self._requete_paths = {}

        self.mag_list.delete(0, "end")
        cur.execute(
            "SELECT m.code, m.nom, g.nom AS grappe FROM magasins m LEFT JOIN grappes g ON m.grappe_id = g.id ORDER BY m.code"
        )
        for m in cur.fetchall():
            self.mag_list.insert("end", f"{m['code']} - {m['nom']} (Grappe: {m['grappe'] or ''})")

        self.grappe_list.delete(0, "end")
        cur.execute("SELECT nom FROM grappes ORDER BY nom")
        for g in cur.fetchall():
            self.grappe_list.insert("end", g["nom"])

        conn.close()

    def _open_param(self):
        ParamWindow(self)
      
    def _execute_on_oracle(self, sql, oracle_selection):
        host, service, sid = oracle_selection.split("|", 3)
        dsn_tns = oracledb.DSN_TNS('your_tns_entry')
        conn = oracledb.connect(dsn_tns, 'username', 'password')
        cursor = conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            print(row)
        cursor.close()
        conn.close()  
  
    def _execute(self):
        selection = self.req_combo.get()
        if not selection:
            messagebox.showwarning("Exécution", "Veuillez sélectionner une requête.")
            return
        selectionOra = self.mag_list.curselection()
        if selectionOra:
            index = selection[0]
            messagebox.showwarning("index",index)
            # Récupération de la valeur de l'élément sélectionné
            oracle_selection = self.mag_list.get(index).strip()
        else:
            # Aucun élément sélectionné, gérer le cas ici
            messagebox.showwarning("alerte","Aucun élément sélectionné")
    

        if not oracle_selection:
            messagebox.showwarning("Exécution", "Veuillez sélectionner une base Oracle.")
        return
        path = self._requete_paths.get(selection)
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    sql = f.read()
                self._log(f"Exécution de la requête '{selection}':\n{sql}")
            except Exception as e:
                self._log(f"Erreur lors de la lecture du fichier '{selection}': {e}")
        else:
            self._log(f"Requête '{selection}' non trouvée.")



if __name__ == "__main__":
    app = MultiRequetesApp()
    app.mainloop()
