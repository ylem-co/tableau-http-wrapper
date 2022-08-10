FROM --platform=x86_64 python:3-slim-buster

ENV FLASK_APP "/opt/tableau/flaskr"

WORKDIR /opt/tableau

COPY . .

RUN pip3 install -r requirements.txt

EXPOSE 5000

CMD [ "flask", "run", "--host=0.0.0.0" ]