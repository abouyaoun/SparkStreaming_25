
cd ../producer
sbt clean compile assembly

cd ..
docker compose build
docker compose up

(services - selectionner element n'ayant pas start et appuyer sur start)


--modif consumer

docker volume prune --all
cd consumer
sbt clean compile assembly
cd ../producer
sbt clean compile assembly
docker compose up --build

--streamlit


cd streamlit
source venv/bin/activate
python -m streamlit run app.py

http://localhost:8501
