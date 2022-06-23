FROM python:3.10.5-bullseye
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
CMD python src/cflpy/docker_success.py
# CMD python src/cflpy/main.py
