
FROM python:3
ENV PYTHONUNBUFFERED 1
ADD ./ /home/app/masternode
WORKDIR /home/app/masternode
RUN pip install -r requirements.txt
#RUN ls -l
#CMD [ "pwd" ]
#CMD [ "main", "manage.py runserver 0.0.0.0:8000"
WORKDIR /home/app
EXPOSE 8080
ENTRYPOINT python masternode/main/server.py