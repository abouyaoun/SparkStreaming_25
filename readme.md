



axe bloquant :

Dans le producer on envoie la data vers le consumer qui sont mal parser
donc on recois de la data érronée.
solution : parser des le producer  /Recommandation : envoie un vrai objet JSON


Pour obtenir une copie du projet sur votre machine :

Ouvrez un terminal.

Cloner le dépôt GitHub :

git clone https://github.com/abouyaoun/SparkStreaming_25.git

Accédez au répertoire du projet :

cd SparkStreaming_25

Le projet utilise Git LFS pour stocker les fichiers JAR. Pour récupérer ces fichiers correctement :

Installez Git LFS si ce n’est pas déjà fait :

macOS : brew install git-lfs

Debian/Ubuntu : sudo apt update && sudo apt install git-lfs

Initialisez Git LFS dans ce repo :

git lfs install

Récupérez les fichiers LFS après le clone :

git lfs pull

Vous avez maintenant tous les fichiers, y compris les JAR, en local. Vous pouvez ensuite :

Lister les branches disponibles :

git branch -a

Basculer sur une branche existante (ex. yassine_docker) :
