
FROM python:3.11-slim

# Définir le dossier de travail
WORKDIR /app


# Installer Java (OpenJDK 21) pour PySpark
RUN apt-get update && apt-get install -y openjdk-21-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
# Debug Java (affiche JAVA_HOME et version Java au build)
RUN echo "JAVA_HOME=$JAVA_HOME" && java -version

# Copier le requirements.txt à la racine du projet
COPY ../requirements.txt ./

# Installer les dépendances Python
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source et les dossiers utiles
COPY ../extraction ./extraction
COPY ../transformation ./transformation
COPY ../load ./load
COPY ../database ./database
COPY ../main.py ./main.py

# Créer le dossier data (monté en volume en prod)
RUN mkdir -p ./data

# Commande par défaut : pipeline complet
CMD ["python", "main.py"]