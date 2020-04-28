# Objectif: 
Pour chaque output files:
- Récupérer la liste des subarrays du grand reconstructed array à stocker dans le output file
    - Les subarrays sont définis comme étant l'intersection des input files, des buffers et des output files
    - Les subarrays sont représentés par des value range codés en utilisant les "slice" de python
    - Les coordonnées dans arrays_dict sont les coordonnées relatives au reconstructed array
    
- Toute cette information est stockée dans un dictionnaire qu'on appelle le "arrays_dict"
- De même pour chaque subarray à stocker dans le output file on veut savoir où le stocker: ses coordonnées dans le référentiel de l'output file
- On veut donc le meme dictionnaire que arrays_dict mais en remove l'offset du output file à tous les slices: regions_dict

# Méthode: 

## A) buffer treatment
- Pour chaque buffer on récupère les subarrays contenus dedans que l'on appelle "volumes": volume = subarray
    - Chaque volume a un index qui le discrimine 
        - Les indices de 1 à 7 sont les indices des volumes dans le modèle mathématique. Ils permettront d'utiliser la keep strategy qui traite les volumes différemments selon leur index.
        - Les indices supérieurs à 7 sont les autres volumes, bordés par les volumes [1:7]

### Résultat: 
    - un dictionnaire qui map chaque buffer aux volumes qu'il contient
    - à l'intérieur les coordonnées sont directement dans la base de reconstructed image

## B) créer le "arrays_dict" à partir du "buff_to_vols" dictionnaire
### B.1) put each volume in its associated output file
- speedup a bit by using heuristic: volume can intercept with output file only if output file intersect buffer in the first place so first compute intersection between buffers et output files

### B.2) au sein de chaque output files, merge les volumes qui sont dans différents buffers mais qui vont être merge
Ainsi on indique à dask qu'il faudra sauvegarder tout ce volume merge en même temps, et donc implicitement qu'il va falloir attendre la lecture des deux buffers avant de pouvoir écrire.

### B.3) clean arrays_dict: simplement remplacer tous les volumes qui sont stockés dans des objets Volume et les remplacer par des listes de slices.

## C) Créer regions_dict from arrays_dict 