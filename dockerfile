FROM postgres:latest
WORKDIR /app
COPY . .
RUN apt install python3
RUN pip install -r requirements.txt
CMD python src/cflpy/docker_success.py
# CMD python src/cflpy/main.py
