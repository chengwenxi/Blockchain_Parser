FROM python:3.6.3
COPY ./ /opt/parser/
WORKDIR /opt/parser/
RUN pip install -r requirements.txt
EXPOSE 3434
