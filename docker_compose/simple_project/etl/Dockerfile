FROM python:3.10

# Выберите папку, в которой будут размещаться файлы проекта внутри контейнера
WORKDIR /opt/etl

# Скопируйте в контейнер файлы, которые редко меняются
COPY requirements.txt requirements.txt

RUN  pip install --no-cache-dir --upgrade pip \
     && pip install --no-cache-dir -r requirements.txt

# Скопируйте всё оставшееся. Для ускорения сборки образа эту команду стоит разместить ближе к концу файла.
COPY . .

# команда сбора файлов
#RUN python connection.py

# Укажите порт, на котором приложение будет доступно внутри Docker-сети
#EXPOSE 9000


