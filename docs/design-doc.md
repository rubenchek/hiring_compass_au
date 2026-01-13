# Design Doc — Système de pilotage et d’automatisation contrôlée des candidatures en Australie (v0.1)

---

## 1. Pourquoi j’ai besoin de ce système

Postuler en Australie implique :

- un **grand volume d’annonces**
- connaitre le marché australien
- des **variations importantes de profils attendus**
- un **coût cognitif élevé** pour adapter CV et lettre à chaque offre

Le processus manuel est :

- chronophage
- peu scalable
- difficile à analyser a posteriori

Je veux créer un système qui **automatise la chaîne complète de candidature**, tout en restant **contrôlable, explicable et améliorable**, afin de :

- analyser et localiser la demande
- postuler plus efficacement
- tester des stratégies de candidature
- apprendre par le feedback réel (réponses, refus, entretiens)

---

## 2. Ce que le système doit me permettre de faire

- Collecter automatiquement des annonces pertinentes
- Analyser et visualiser la demande (géographie, rôles)
- Classer et prioriser les annonces
- Assister la génération de CV et lettres
- Tracer chaque candidature (contenu, contexte, timing)
- Observer ce qui fonctionne réellement et ajuster la stratégie

👉 L’objectif n’est pas la perfection du texte, mais **l’efficacité globale du processus**.

---

## 3. Hypothèses de départ (à tester)

- **H1** — La personnalisation automatique ciblée est suffisante pour obtenir des réponses comparables au manuel
- **H2** — Certaines catégories d’annonces réagissent mieux à l’auto-candidature que d’autres
- **H3** — La vitesse et la cohérence comptent plus que l’optimisation stylistique fine

Ces hypothèses sont considérées comme **fausses tant qu’elles ne sont pas validées par des données**.

---

## 4. Règles non négociables

- Toute candidature générée doit être **traçable** (inputs → outputs)
- Le système doit permettre un **contrôle humain à tout moment**
- Une amélioration du système = un **meilleur taux de réponse ou un gain de temps mesurable**
- Les règles explicites précèdent les modèles complexes
- Chaque automatisation doit pouvoir être **désactivée**

---

## 5. Hors périmètre (pour l’instant)

- Pas d’auto-envoi non contrôlé sur des plateformes à risque de ban
- Pas d’optimisation stylistique infinie des lettres
- Pas de fine-tuning lourd de LLM au départ
- Pas de multi-pays
- Pas d’interface utilisateur complexe

👉 Le système est **fonctionnel avant d’être élégant**.

---

## 6. Découpage conceptuel du système (haut niveau)

Le système est composé de blocs indépendants :

1. **Ingestion**
    
    → annonces, métadonnées, descriptions
    
2. **Analyse & scoring**
    
    → pertinence, priorité, type de rôle
    
3. **Génération**
    
    → CV adapté, lettre contextualisée
    
4. **Candidature**
    
    → auto ou semi-auto
    
5. **Tracking & feedback**
    
    → réponses, délais, résultats
    

Chaque bloc peut évoluer indépendamment.

---

## 7. À quoi sert ce document

Ce document sert à :

- définir clairement la **nature du système**
- éviter de construire un simple “assistant de texte”
- maintenir un cap orienté **processus + données**
- trancher rapidement lorsqu’une idée crée de la dérive

Il est modifié **uniquement si la nature du système change**.

---

### ✅ Fin du Design Doc v0.1