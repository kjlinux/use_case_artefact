# Modelisation et Normalisation

## Contexte

Le fichier source est un CSV plat de 2253 lignes et 29 colonnes. Chaque ligne represente un article de vente. Les attributs client, produit et canal sont repetes a chaque ligne, ce qui genere une redondance importante et des risques d'anomalies (mise a jour, insertion, suppression).

L'objectif est de normaliser ces donnees en 3FN puis en DKNF.

---

## Partie A : Normalisation en 3e Forme Normale (3FN)

### Demarche

**1FN** : deja satisfaite. Chaque cellule est atomique, chaque ligne est identifiable par `item_id`.

**2FN** : on elimine les dependances partielles. Le grain du fichier est `item_id`. Mais `sale_date`, `customer_id`, `channel` dependent de `sale_id` (pas de `item_id` directement). Les attributs produit dependent de `product_id`. On extrait ces groupes dans leurs propres tables.

**3FN** : on elimine les dependances transitives. `channel_campaigns` depend de `channel` (pas de la cle primaire). On regroupe canal et campagne dans une table dediee.

### Tables 3FN

**customers**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| customer_id | INTEGER | PK |
| first_name | VARCHAR(100) | nullable |
| last_name | VARCHAR(100) | nullable |
| email | VARCHAR(255) | nullable |
| gender | VARCHAR(20) | |
| age_range | VARCHAR(20) | |
| signup_date | DATE | |
| country | VARCHAR(100) | |

**products**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| product_id | INTEGER | PK |
| product_name | VARCHAR(255) | NOT NULL |
| category | VARCHAR(100) | |
| brand | VARCHAR(100) | |
| color | VARCHAR(50) | |
| size | VARCHAR(10) | |
| catalog_price | NUMERIC(10,2) | |
| cost_price | NUMERIC(10,2) | |

**channels**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| channel_id | SERIAL | PK |
| channel_name | VARCHAR(50) | UNIQUE |
| campaign_name | VARCHAR(100) | |

**sales**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| sale_id | INTEGER | PK |
| sale_date | DATE | NOT NULL |
| customer_id | INTEGER | FK -> customers |
| channel_id | INTEGER | FK -> channels |
| total_amount | NUMERIC(10,2) | nullable |

**sale_items**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| item_id | INTEGER | PK |
| sale_id | INTEGER | FK -> sales |
| product_id | INTEGER | FK -> products |
| quantity | INTEGER | NOT NULL |
| original_price | NUMERIC(10,2) | |
| unit_price | NUMERIC(10,2) | |
| discount_applied | NUMERIC(10,2) | |
| discount_percent | VARCHAR(10) | |
| discounted | INTEGER | |
| item_total | NUMERIC(10,2) | |

### Justification

- On passe de 1 table de 29 colonnes a 5 tables.
- Les attributs client ne sont stockes qu'une fois par client (au lieu de ~4x).
- Les attributs produit ne sont stockes qu'une fois par produit (au lieu de ~4.5x).
- Le mapping channel/campaign est centralise.

### Limites de la 3FN

- Les valeurs de domaine (pays, categories, couleurs...) restent des chaines repetees dans chaque table qui les utilise.
- Les champs derives (`unit_price`, `item_total`, `discounted`, `discount_percent`, `total_amount`) sont toujours stockes. Si `discount_applied` change sans que `unit_price` soit mis a jour, on a une incoherence.

---

## Partie B : Normalisation en DKNF (Domain-Key Normal Form)

### Principe

En DKNF, chaque contrainte sur les donnees est une consequence logique des contraintes de domaine (valeurs autorisees) et des contraintes de cle (unicite, references). Il ne reste aucune source d'anomalie possible.

Concretement, ca implique :
- Extraire toutes les valeurs de domaine dans des tables de reference
- Supprimer tous les champs calculables (ce sont des contraintes de calcul, pas des contraintes de domaine ou de cle)
- Utiliser des cles etrangeres partout pour enforcer l'integrite

### Tables DKNF

**countries** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| country_id | SERIAL | PK |
| country_name | VARCHAR(100) | NOT NULL, UNIQUE |

**categories** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| category_id | SERIAL | PK |
| category_name | VARCHAR(100) | NOT NULL, UNIQUE |

**brands** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| brand_id | SERIAL | PK |
| brand_name | VARCHAR(100) | NOT NULL, UNIQUE |

**colors** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| color_id | SERIAL | PK |
| color_name | VARCHAR(50) | NOT NULL, UNIQUE |

**sizes** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| size_id | SERIAL | PK |
| size_label | VARCHAR(10) | NOT NULL, UNIQUE |

**age_ranges** (table de reference)

| Colonne | Type | Contrainte |
|---------|------|-----------|
| age_range_id | SERIAL | PK |
| age_range_label | VARCHAR(20) | NOT NULL, UNIQUE |

**channels**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| channel_id | SERIAL | PK |
| channel_name | VARCHAR(50) | NOT NULL, UNIQUE |
| campaign_name | VARCHAR(100) | |

**customers**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| customer_id | INTEGER | PK |
| first_name | VARCHAR(100) | nullable |
| last_name | VARCHAR(100) | nullable |
| email | VARCHAR(255) | nullable |
| gender | gender_enum | |
| age_range_id | INTEGER | FK -> age_ranges, NOT NULL |
| signup_date | DATE | |
| country_id | INTEGER | FK -> countries, NOT NULL |

**products**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| product_id | INTEGER | PK |
| product_name | VARCHAR(255) | NOT NULL |
| category_id | INTEGER | FK -> categories, NOT NULL |
| brand_id | INTEGER | FK -> brands, NOT NULL |
| color_id | INTEGER | FK -> colors, NOT NULL |
| size_id | INTEGER | FK -> sizes, NOT NULL |
| catalog_price | NUMERIC(10,2) | NOT NULL |
| cost_price | NUMERIC(10,2) | NOT NULL |

**sales**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| sale_id | INTEGER | PK |
| sale_date | DATE | NOT NULL |
| customer_id | INTEGER | FK -> customers, NOT NULL |
| channel_id | INTEGER | FK -> channels, NOT NULL |

**sale_items**

| Colonne | Type | Contrainte |
|---------|------|-----------|
| item_id | INTEGER | PK |
| sale_id | INTEGER | FK -> sales, NOT NULL |
| product_id | INTEGER | FK -> products, NOT NULL |
| quantity | INTEGER | NOT NULL, CHECK > 0 |
| original_price | NUMERIC(10,2) | NOT NULL |
| discount_applied | NUMERIC(10,2) | NOT NULL, DEFAULT 0 |

### Champs supprimes par rapport a la 3FN

| Champ supprime | Raison | Formule de derivation |
|----------------|--------|----------------------|
| total_amount (sales) | Calculable | SUM(quantity * (original_price - discount_applied)) GROUP BY sale_id |
| unit_price (sale_items) | Calculable | original_price - discount_applied |
| discount_percent (sale_items) | Calculable | discount_applied / original_price |
| discounted (sale_items) | Calculable | CASE WHEN discount_applied > 0 THEN 1 ELSE 0 END |
| item_total (sale_items) | Calculable | quantity * (original_price - discount_applied) |

Ces champs sont reconstruits dans une vue SQL (v_star_schema) qui joint toutes les tables DKNF et recalcule les valeurs derivees.

### Justification des choix

- **gender comme ENUM PostgreSQL** : dans le dataset actuel, seul "Female" apparait. Mais le type ENUM avec ('Female', 'Male', 'Other') permet d'anticiper sans ajouter une table de reference. C'est une contrainte de domaine au niveau colonne.

- **campaign_name dans channels** : le mapping 1:1 entre canal et campagne est une dependance fonctionnelle. En DKNF, on la conserve dans la meme table car la campagne depend de la cle (channel_id).

- **Tables de reference separees** : meme si "Tiva" est la seule marque, on cree quand meme la table brands. En production, d'autres marques pourraient s'ajouter. Surtout, ca empeche l'insertion de valeurs incorrectes.

---

## Interet de la DKNF dans notre cas

La question posee est : pourquoi aller jusqu'a la DKNF plutot que s'arreter en 3FN ?

**1. Elimination des incoherences de calcul**

En 3FN, on stocke `unit_price` et `discount_applied`. Si une correction est faite sur `discount_applied` sans mettre a jour `unit_price`, les donnees deviennent fausses silencieusement. En DKNF, `unit_price` n'existe pas dans la table : il est calcule a la volee. Impossible d'avoir une incoherence.

**2. Gouvernance des donnees par le schema**

En 3FN, un produit peut avoir `category = "Hatz"` (typo de "Hats"). En DKNF, la table `categories` ne contient que les valeurs autorisees. La contrainte de cle etrangere empeche l'insertion de valeurs invalides. Le schema lui-meme enforce la qualite des donnees.

**3. Maintenance simplifiee**

Si l'entreprise decide de renommer "E-commerce" en "Web Store", en 3FN il faut mettre a jour toutes les lignes de sales qui contiennent cette chaine. En DKNF, on modifie une seule ligne dans la table channels.

**4. Pas de donnees mortes**

Le champ `total_amount` est vide dans 225 lignes du CSV original. En DKNF, on ne le stocke pas du tout : il est toujours calculable et toujours correct. Pas besoin de gerer les cas ou il est null.

**5. Pertinence pour un contexte de data engineering**

En tant que data engineer, on construit des pipelines qui alimentent des tables. Si on stocke des champs derives, chaque pipeline doit garantir leur coherence. En DKNF, le pipeline n'insere que les donnees brutes et la vue se charge du reste. Ca reduit la complexite du code d'ingestion et les risques de bugs.

---

## Vue en etoile (Star Schema)

A partir des tables DKNF, une vue `v_star_schema` reconstruit la vision denormalisee pour les besoins analytiques. C'est le meilleur des deux mondes :
- Stockage normalise (pas de redondance, pas d'anomalies)
- Vue denormalisee pour les requetes analytiques (pas de jointures manuelles)

La vue inclut tous les champs derives recalcules et joint les 11 tables en une seule vision plate.
