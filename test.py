import tkinter as tk
from tkinter import ttk
import sqlite3

# -----------------------------
# Connexion SQLite et création tables
# -----------------------------
def connexion_bdd():
    conn = sqlite3.connect("projet.db")
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS connexions (
        id_connexion INTEGER PRIMARY KEY AUTOINCREMENT,
        type_connexion TEXT,
        adresse_connexion TEXT,
        service TEXT,
        sid TEXT
    )""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS requetes (
        id_requete INTEGER PRIMARY KEY AUTOINCREMENT,
        id_connexion INTEGER,
        categorie_fonctionnelle TEXT,
        intitule TEXT,
        requete_sql TEXT,
        FOREIGN KEY(id_connexion) REFERENCES connexions(id_connexion)
    )""")
    conn.commit()
    return conn

# -----------------------------
# Ajouter une connexion
# -----------------------------
def ajouter_connexion():
    conn = connexion_bdd()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO connexions (type_connexion, adresse_connexion, service, sid)
        VALUES (?, ?, ?, ?)
    """, (champ_type.get(), champ_adresse.get(), champ_service.get(), champ_sid.get()))
    conn.commit()
    conn.close()
    label_resultat.config(text="Connexion ajoutée !")
    rafraichir_connexions()

# -----------------------------
# Ajouter une requête SQL
# -----------------------------
def ajouter_requete():
    # Récupérer id_connexion depuis l'adresse sélectionnée
    adresse = menu_connexion.get()
    conn = connexion_bdd()
    cursor = conn.cursor()
    cursor.execute("SELECT id_connexion FROM connexions WHERE adresse_connexion=?", (adresse,))
    row = cursor.fetchone()
    if row:
        id_conn = row[0]
        cursor.execute("""
            INSERT INTO requetes (id_connexion, categorie_fonctionnelle, intitule, requete_sql)
            VALUES (?, ?, ?, ?)
        """, (id_conn, champ_categorie.get(), champ_intitule.get(), champ_sql.get()))
        conn.commit()
        label_resultat.config(text="Requête ajoutée !")
        rafraichir_requetes()
    else:
        label_resultat.config(text="Erreur : connexion introuvable")
    conn.close()

# -----------------------------
# Rafraîchir liste des connexions (adresse)
# -----------------------------
def rafraichir_connexions():
    conn = connexion_bdd()
    cursor = conn.cursor()
    cursor.execute("SELECT adresse_connexion FROM connexions")
    adresses = [row[0] for row in cursor.fetchall()]
    menu_connexion['values'] = adresses
    conn.close()

# dictionnaire global pour lier adresse -> id_connexion
conn_dict = {}

def rafraichir_connexions():
    global conn_dict
    conn = connexion_bdd()
    cursor = conn.cursor()
    cursor.execute("SELECT id_connexion, adresse_connexion FROM connexions")
    rows = cursor.fetchall()
    adresses = [row[1] for row in rows]        # texte affiché
    conn_dict = {row[1]: row[0] for row in rows}  # mapping texte -> id
    menu_connexion['values'] = adresses
    conn.close()
# -----------------------------
# Rafraîchir liste des requêtes (intitulé)
# -----------------------------
def rafraichir_requetes():
    conn = connexion_bdd()
    cursor = conn.cursor()
    cursor.execute("SELECT intitule FROM requetes")
    intitulés = [row[0] for row in cursor.fetchall()]
    menu_requete['values'] = intitulés
    conn.close()

   

# -----------------------------
# Interface
# -----------------------------
fenetre = tk.Tk()
fenetre.title("Gestion Connexions et Requêtes SQL")
fenetre.geometry("500x650")

# --- Connexions ---
tk.Label(fenetre, text="Type Connexion").pack()
champ_type = tk.Entry(fenetre)
champ_type.pack()

tk.Label(fenetre, text="Adresse (nom serveur)").pack()
champ_adresse = tk.Entry(fenetre)
champ_adresse.pack()

tk.Label(fenetre, text="Service").pack()
champ_service = tk.Entry(fenetre)
champ_service.pack()

tk.Label(fenetre, text="SID").pack()
champ_sid = tk.Entry(fenetre)
champ_sid.pack()

tk.Button(fenetre, text="Ajouter Connexion", command=ajouter_connexion).pack(pady=5)

# --- Requêtes SQL ---
tk.Label(fenetre, text="Sélectionner Connexion (Nom serveur)").pack()
menu_connexion = ttk.Combobox(fenetre)
menu_connexion.pack()

tk.Label(fenetre, text="Catégorie fonctionnelle").pack()
champ_categorie = tk.Entry(fenetre)
champ_categorie.pack()

tk.Label(fenetre, text="Intitulé").pack()
champ_intitule = tk.Entry(fenetre)
champ_intitule.pack()

tk.Label(fenetre, text="Requête SQL").pack()
champ_sql = tk.Entry(fenetre)
champ_sql.pack()

tk.Button(fenetre, text="Ajouter Requête", command=ajouter_requete).pack(pady=5)

# Liste déroulante pour sélectionner une requête
tk.Label(fenetre, text="Sélectionner Requête (Intitulé)").pack()
menu_requete = ttk.Combobox(fenetre)
menu_requete.pack()

label_resultat = tk.Label(fenetre, text="")
label_resultat.pack(pady=10)

# Initialiser menus
rafraichir_connexions()
rafraichir_requetes()
fenetre.mainloop()