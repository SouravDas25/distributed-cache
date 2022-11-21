
FROM python:3
ENV PYTHONUNBUFFERED 1
ADD ./src/ /home/app
WORKDIR /home/app
RUN pip install -r requirements.txt
#RUN ls -l
#CMD [ "pwd" ]
#CMD [ "main", "manage.py runserver 0.0.0.0:8000"
ENTRYPOINT python python/main.py